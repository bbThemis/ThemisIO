#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <errno.h>
#include "../page_cache.h"

const char *filename = "test_page_cache.out";


int main() {
  char data[] = "000 001 002 003 004 005 006 007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024\n";
  FILE *outf = fopen(filename, "w");
  fwrite(data, 1, strlen(data), outf);
  fclose(outf);

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
}


