#undef NDEBUG

#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cassert>
#include <errno.h>
#include "../page_cache.h"


const char *filename = "test_page_cache.out";


/* Fill filename with 'length' bytes in the form " 000 001 002..." */
void writeFile(int length) {
  char buf[1025];
  const int buf_len = 1024;
  int pos = 0, k = 0;
  int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
  assert(fd != -1);

  while (pos < length) {
    for (int i=0; i < buf_len; i += 4) {
      sprintf(buf+i, " %03d", k);
      if (++k == 1000) k = 0;
    }
    int write_len = std::min(length-pos, buf_len);
    int bytes_written = write(fd, buf, write_len);
    assert(bytes_written == write_len);
    pos += write_len;
  }
  close(fd);
}


void testReadOnly() {
  printf("testReadOnly "); fflush(stdout);

  remove(filename);

  PageCache cache(PageCache::VISIBLE_AFTER_CLOSE, 64, 10000);
  int fd, result;
  char buf[1024] = {0};

  // invalid file descriptors; nothing should be cached
  assert(!cache.isCached(-1, 0));
  assert(!cache.isCached(0, 0));
  assert(!cache.isCached(1, 0));
  assert(!cache.isCached(135, 0));
  
  // test fail to open
  fd = cache.open(filename, O_RDONLY);
  assert(fd == -1);
  assert(errno == ENOENT);

  // create the file, now it should work
  writeFile(100);
  fd = cache.open(filename, O_RDONLY);
  assert(fd != -1);

  // check file position
  assert(0 == cache.lseek(fd, 0, SEEK_CUR));

  // write should fail if opened for write
  result = cache.write(fd, buf, 10);
  assert(result == -1);
  assert(errno == EBADF);

  // read a little
  result = cache.read(fd, buf, 10);
  assert(!memcmp(buf, " 000 001 0", 10));

  // check file position
  assert(10 == cache.lseek(fd, 0, SEEK_CUR));

  // that whole block should now be cached
  assert(cache.isCached(fd, 0));
  assert(cache.isCached(fd, 63));
  assert(!cache.isPageDirty(0, 0));

  // with no readahead past the end of the block
  assert(!cache.isCached(fd, 64));

  // read the rest of the file
  result = cache.read(fd, buf+10, 1014);
  assert(result == 90);
  assert(!memcmp(buf, " 000 001 002 003 004 005 006 007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024", 100));

  // check file position
  assert(100 == cache.lseek(fd, 0, SEEK_CUR));

  assert(cache.fsck());
  assert(0 == cache.close(fd));

  printf("done.\n");
}


void testReadConsistency() {
  printf("testReadConsistency "); fflush(stdout);
  remove(filename);

  char buf[100];
  int write_fd, read_fd;
  PageCache *cache = new PageCache(PageCache::VISIBLE_AFTER_WRITE, 64, 10000);
  
  // VISIBLE_AFTER_WRITE
  // Pages should never become cached.
  write_fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
  assert(write_fd != -1);
  assert(4 == pwrite(write_fd, "AAAA", 4, 0));

  read_fd = cache->open(filename, O_RDONLY);
  assert(read_fd != -1);
  assert(4 == cache->pread(read_fd, buf, 4, 0));
  assert(!memcmp(buf, "AAAA", 4));
  assert(!cache->isCached(read_fd, 0));
  
  // change the data
  assert(4 == pwrite(write_fd, "BBBB", 4, 0));

  // the cache should re-read the data
  assert(4 == cache->pread(read_fd, buf, 4, 0));
  assert(!memcmp(buf, "BBBB", 4));
  assert(!cache->isCached(read_fd, 0));
  assert(!cache->close(read_fd));
  delete cache;
  
  // change the data
  assert(4 == pwrite(write_fd, "CCCC", 4, 0));

  // VISIBLE_AFTER_CLOSE
  // Once the page is cached, an update to the file will not be noticed.
  cache = new PageCache(PageCache::VISIBLE_AFTER_CLOSE, 64, 10000);
  
  read_fd = cache->open(filename, O_RDONLY);
  assert(read_fd != -1);
  assert(4 == cache->pread(read_fd, buf, 4, 0));
  assert(!memcmp(buf, "CCCC", 4));
  assert(cache->isCached(read_fd, 0));
  
  // change the data
  assert(4 == pwrite(write_fd, "DDDD", 4, 0));

  // change is not detected
  assert(4 == cache->pread(read_fd, buf, 4, 0));
  assert(!memcmp(buf, "CCCC", 4));
  assert(cache->isCached(read_fd, 0));

  // if another file descriptor is opened, the change will be detected
  // in both file descriptors
  int read_fd2 = cache->open(filename, O_RDONLY);
  assert(read_fd2 != -1 && read_fd2 != read_fd);
  // opening another file descriptor on this file will invalidate cached pages
  assert(!cache->isCached(read_fd, 0));
  assert(!cache->isCached(read_fd2, 0));
  assert(4 == cache->pread(read_fd2, buf, 4, 0));
  assert(!memcmp(buf, "DDDD", 4));
  memset(buf, 0, 4);
  assert(4 == cache->pread(read_fd, buf, 4, 0));
  assert(!memcmp(buf, "DDDD", 4));
  
  // change the data
  assert(4 == pwrite(write_fd, "EEEE", 4, 0));

  // closing the file doesn't invalidate the read cache
  cache->close(read_fd);
  assert(cache->isCached(read_fd2, 0));
  memset(buf, 0, 4);
  assert(4 == cache->pread(read_fd2, buf, 4, 0));
  assert(!memcmp(buf, "DDDD", 4));

  cache->close(read_fd2);

  assert(-1 == cache->pread(read_fd, buf, 4, 0));
  assert(errno == EBADF);
  errno = 0;
  assert(-1 == cache->pread(read_fd2, buf, 4, 0));
  assert(errno == EBADF);

  delete cache;

  // VISIBLE_AFTER_EXIT
  // Don't invalidate the page even after the file is closed.
  cache = new PageCache(PageCache::VISIBLE_AFTER_EXIT, 64, 10000);
  read_fd = cache->open(filename, O_RDONLY);
  memset(buf, 0, 4);
  assert(!cache->isCached(read_fd, 0));
  assert(4 == cache->pread(read_fd, buf, 4, 0));
  assert(!memcmp(buf, "EEEE", 4));
  assert(cache->isCached(read_fd, 0));

  // change the data
  assert(4 == pwrite(write_fd, "FFFF", 4, 0));
  
  cache->close(read_fd);
  // isCached returns false on closed file descriptors
  assert(!cache->isCached(read_fd, 0));

  // reopen the file
  read_fd = cache->open(filename, O_RDONLY);
  assert(cache->isCached(read_fd, 0));
  memset(buf, 0, 4);
  assert(4 == cache->pread(read_fd, buf, 4, 0));
  assert(!memcmp(buf, "EEEE", 4));

  // flushWriteCache shouldn't change anything
  assert(0 == cache->flushWriteCache());
  assert(4 == cache->pread(read_fd, buf, 4, 0));
  assert(!memcmp(buf, "EEEE", 4));
  assert(cache->fsck());

  // flushAll should
  assert(0 == cache->flushAll());
  assert(cache->fsck());
  assert(!cache->isCached(read_fd, 0));
  assert(4 == cache->pread(read_fd, buf, 4, 0));
  assert(!memcmp(buf, "FFFF", 4));

  cache->close(read_fd);

  delete cache;
  
  assert(!close(write_fd));
  
  printf("done.\n");
}


int main() {
  remove(filename);

  testReadOnly();
  testReadConsistency();

  return 0;
}


