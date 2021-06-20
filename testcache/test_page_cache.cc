#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cassert>
#include <errno.h>
#include "../page_cache.h"

#undef NDEBUG

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
  
  assert(0 == cache.close(fd));

  printf("done.\n");
}

int main() {
  remove(filename);

  testReadOnly();

#if 0
  PageCache cache(PageCache::VISIBLE_AFTER_CLOSE, 64, 500);

  //  = "000 001 002 003 004 005 006 007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024 025 026 027 028 029 030 031 032 ";
  char buf[200] = {0};
  int count, fd;
  // long offset;

  fd = cache.open(filename, O_RDWR);
  if (fd == -1) {
    printf("Error opening %s: %s\n", filename, strerror(errno));
    return 1;
  } 
  printf("opened file for reading via cache, fd = %d\n", fd);

  assert(!cache.isCached(fd, 0));
  count = cache.read(fd, buf, 10);
  assert(count == 10);
  assert(!memcmp(buf, data, 10));
  assert(cache.isCached(fd, 10));
  assert(!cache.isCached(fd, 64));
  cache.fsck();

  count = cache.read(fd, buf+10, 190);
  assert(count == 90);
  assert(cache.isCached(fd, 64));
  
  cache.close(fd);
  remove(filename);
  
  printf("done\n");
  return 0;
  #endif
  
}


