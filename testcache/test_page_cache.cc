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

using std::string;
using std::vector;

const char *filename = "test_page_cache.out";
const char *filename2 = "test_page_cache2.out";


/* Fill filename with 'length' bytes in the form " 000 001 002..." */
void writeFile(int length, const char *fn = filename) {
  char buf[1025];
  const int buf_len = 1024;
  int pos = 0, k = 0;
  int fd = open(fn, O_WRONLY | O_CREAT | O_TRUNC, 0644);
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


string readFile(const char *fn = filename) {
  string content;
  char block[10];
  int fd = open(fn, O_RDONLY);
  if (fd == -1) return content;

  ssize_t bytes_read;
  while ((bytes_read = read(fd, block, sizeof block)) > 0) {
    content.append(block, block + bytes_read);
  }
  close(fd);
  return content;
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


void testLengthUpdates() {
  printf("testLengthUpdates "); fflush(stdout);

  remove(filename);
  writeFile(16);
  writeFile(16, filename2);
  
  int fd, fd2;
  char buf[1024] = {0};
  string contents;
  PageCache *cache = new PageCache(PageCache::VISIBLE_AFTER_WRITE, 64, 10000);

  fd = cache->open(filename, O_RDWR);
  fd2 = open(filename2, O_RDWR);

  // the file is currently 16 bytes long
  assert(pread(fd2, buf, 1024, 0) == 16);
  assert(cache->pread(fd, buf, 1024, 0) == 16);

  // check the file position--it should still be 0, not 1024
  assert(0 == lseek(fd2, 0, SEEK_CUR));
  assert(0 == cache->lseek(fd, 0, SEEK_CUR));

  // now try a normal read, which will update the position
  assert(cache->read(fd, buf, 1024) == 16);
  assert(read(fd2, buf, 1024) == 16);
  assert(16 == cache->lseek(fd, 0, SEEK_END));
  assert(16 == lseek(fd2, 0, SEEK_END));

  // append a few bytes via another file descriptor
  writeFile(32);
  writeFile(32, filename2);

  // with VISIBLE_AFTER_WRITE, that append should be visible
  lseek(fd2, 0, SEEK_SET);
  cache->lseek(fd, 0, SEEK_SET);
  assert(read(fd2, buf, 1024) == 32);
  assert(cache->read(fd, buf, 1024) == 32);

  // make sure my writes change the length too
  write(fd2, "appendthis", 10);
  cache->write(fd, "appendthis", 10);

  assert(pread(fd2, buf, 1024, 0) == 42);
  assert(cache->pread(fd, buf, 1024, 0) == 42);

  contents = readFile();
  assert(contents == " 000 001 002 003 004 005 006 007appendthis");
  contents = "";
  contents = readFile(filename2);
  assert(contents == " 000 001 002 003 004 005 006 007appendthis");
  
  delete cache;

  // do similar tests with VISIBLE_AFTER_CLOSE
  remove(filename);
  remove(filename2);
  writeFile(8);
  writeFile(8, filename2);
  
  cache = new PageCache(PageCache::VISIBLE_AFTER_CLOSE, 64, 10000);

  fd = cache->open(filename, O_RDWR);
  assert(!cache->isCached(fd, 0));
  fd2 = open(filename2, O_RDWR);

  // the file is currently 8 bytes long
  assert(cache->read(fd, buf, 1024) == 8);
  assert(cache->isCached(fd, 0));
  assert(read(fd2, buf, 1024) == 8);

  // check the file position--it should be 8, not 1024
  assert(8 == cache->lseek(fd, 0, SEEK_CUR));
  assert(8 == cache->lseek(fd, 0, SEEK_END));

  // append a few bytes via another file descriptor
  writeFile(16);
  writeFile(16, filename2);

  // with VISIBLE_AFTER_CLOSE, that append should not be visible
  assert(cache->pread(fd, buf, 1024, 0) == 8);
  assert(pread(fd2, buf, 1024, 0) == 16);
  
  // check file position again
  assert(8 == cache->lseek(fd, 0, SEEK_CUR));
  assert(8 == lseek(fd2, 0, SEEK_CUR));

  // overwrite and append, make sure my writes change the length
  cache->write(fd, "appendthis", 10);
  write(fd2, "appendthis", 10);
  assert(18 == cache->lseek(fd, 0, SEEK_CUR));
  assert(18 == cache->lseek(fd, 0, SEEK_END));
  assert(18 == lseek(fd2, 0, SEEK_CUR));
  assert(18 == lseek(fd2, 0, SEEK_END));

  memset(buf, 0, 1024);
  assert(0 == cache->lseek(fd, 0, SEEK_SET));
  assert(cache->read(fd, buf, 1024) == 18);
  assert(!memcmp(buf, " 000 001appendthis", 18));

  // the contents on disk haven't changed yet
  contents = readFile();
  assert(contents == " 000 001 002 003");
  contents = readFile(filename2);
  assert(contents == " 000 001appendthis");

  // add a cached page
  assert(!cache->isCached(fd, 64));
  cache->fsck();
  assert(61 == cache->write(fd, " here is more content for the test that will go into the file", 61));
  cache->fsck();
  assert(cache->isCached(fd, 64));
  assert(61 == write(fd2, " here is more content for the test that will go into the file", 61));
  assert(79 == cache->lseek(fd, 0, SEEK_END));
  assert(79 == cache->lseek(fd, 0, SEEK_CUR));
  assert(79 == lseek(fd2, 0, SEEK_END));
  assert(79 == lseek(fd2, 0, SEEK_CUR));
  
  memset(buf, 0, 1024);
  assert(79 == cache->pread(fd, buf, 1024, 0));
  assert(!memcmp(buf, " 000 001appendthis here is more content for the test that will go into the file", 79));
  
  memset(buf, 0, 1024);
  assert(79 == pread(fd2, buf, 1024, 0));
  assert(!memcmp(buf, " 000 001appendthis here is more content for the test that will go into the file", 79));

  // the contents on disk haven't changed yet
  contents = readFile();
  assert(contents == " 000 001 002 003");
  contents = readFile(filename2);
  assert(contents == " 000 001appendthis here is more content for the test that will go into the file");

  cache->close(fd);
  close(fd2);

  // now the file has been updated
  contents = readFile();
  assert(contents == " 000 001appendthis here is more content for the test that will go into the file");
  
  delete cache;

  remove(filename);
  remove(filename2);
  
  printf("done.\n");
}


// TODO when deferred ftruncates are enabled, modify these tests to match
void testTruncate() {
  // try shortening abcdefgh to abcd, then lengthening to abcd....
  // make sure "efgh" don't persist

  printf("testTruncate "); fflush(stdout);
  int fd, fd2;
  char buf[1024] = {0};
  string contents;
  PageCache *cache = new PageCache(PageCache::VISIBLE_AFTER_WRITE, 16, 10000);

  assert(-1 == cache->ftruncate(47, 100) && errno == EBADF);

  writeFile(24);
  writeFile(24, filename2);
  fd = cache->open(filename, O_RDONLY);
  fd2 = open(filename, O_RDONLY);
  assert(fd != -1 && fd2 != -1);
  assert(-1 == cache->ftruncate(fd, 100) && errno == EINVAL);
  assert(-1 == ftruncate(fd2, 100) && (errno == EBADF || errno == EINVAL));
  assert(0 == cache->close(fd));
  assert(0 == close(fd2));

  fd = cache->open(filename, O_RDWR);
  fd2 = open(filename2, O_RDWR);

  assert(readFile() == " 000 001 002 003 004 005");
  assert(readFile(filename2) == " 000 001 002 003 004 005");
  assert(0 == cache->ftruncate(fd, 20));
  assert(0 == ftruncate(fd2, 20));
  assert(readFile() == " 000 001 002 003 004");
  assert(readFile(filename2) == " 000 001 002 003 004");

  assert(0 == cache->ftruncate(fd, 30));
  memset(buf, 255, sizeof buf);
  memset(buf+100, 0, 30);
  strcpy(buf+100, " 000 001 002 003 004");
  assert(30 == cache->pread(fd, buf, 100, 0));
  assert(!memcmp(buf, buf+100, 100));
  
  cache->close(fd);
  delete cache;

  cache = new PageCache(PageCache::VISIBLE_AFTER_CLOSE, 16, 10000);
  writeFile(16);
  fd = cache->open(filename, O_RDWR);
  assert(16 == cache->lseek(fd, 0, SEEK_END));
  
  // write a few bytes past EOF, check for zero fill
  assert(40 == cache->lseek(fd, 40, SEEK_SET));
  assert(8 == cache->write(fd, "XXXXXXXX", 8));
  assert(48 == cache->lseek(fd, 0, SEEK_END));

  // on disk file is unchanged
  fd2 = open(filename, O_RDWR);
  assert(16 == pread(fd2, buf, 100, 0));

  // now flush changes
  cache->close(fd);
  memset(buf, 255, sizeof buf);
  assert(48 == pread(fd2, buf, 100, 0));
  strcpy(buf+100, " 000 001 002 003");
  memset(buf+116, 0, 24);
  memcpy(buf+140, "XXXXXXXX", 8);
  assert(!memcmp(buf, buf+100, 100));
  
  fd = cache->open(filename, O_RDWR);
  
  cache->ftruncate(fd, 18);
  assert(18 == cache->pread(fd, buf, 100, 0));
  assert(!memcmp(buf, " 000 001 002 003 0", 18));
  // with VISIBLE_AFTER_CLOSE, the change should not have taken effect yet
  assert(20 == pread(fd2, buf, 100, 0));
  assert(!memcmp(buf, " 000 001 002 003 0", 18));
  cache->close(fd);
  // now the change should have been done
  assert(18 == pread(fd2, buf, 100, 0));

  // try extending the file
  writeFile(12);
  fd = cache->open(filename, O_RDWR);
  // 12 -> 50
  cache->ftruncate(fd, 50);
  assert(50 == cache->lseek(fd, 0, SEEK_END));
  assert(" 000 001 002" == readFile());
  assert(12 == pread(fd2, buf, 100, 0));
  assert(!cache->isCached(fd, 0));
  assert(!cache->isCached(fd, 16));
  assert(!cache->isCached(fd, 32));
  // set buf+100 to how the file should be
  memset(buf, 255, sizeof(buf));
  strcpy(buf+100, " 000 001 002");
  memset(buf+112, 0, 50-12);
  assert(50 == cache->pread(fd, buf, 100, 0));
  assert(!memcmp(buf, buf+100, 100));
  cache->close(fd);

  // closing the file makes the ftruncate happen
  memset(buf, 255, 100);
  assert(50 == pread(fd2, buf, 100, 0));
  assert(!memcmp(buf, buf+100, 100));
  
  
  close(fd2);
  delete cache;

  printf("done.\n");
}
  

int main() {
  remove(filename);

  testReadOnly();
  testReadConsistency();
  testLengthUpdates();
  testTruncate();

  return 0;
}


