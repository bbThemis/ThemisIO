#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

const char *FILENAME = "/myfs/simple.out";


int main(int argc, char **argv) {
	int fd = open(FILENAME, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    printf("HI!\n");
	if (fd == -1) {
		printf("Error creating %s: %s\n", FILENAME, strerror(errno));
		return 1;
	}

    void * mmap_test = mmap(0, 4097, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0);
    printf("mmap ptr: %p\n", mmap_test);

	int array_size = 256, tmp_size;
	if (argc > 1 && 1 == sscanf(argv[1], "%d", &tmp_size) && tmp_size > 0)
		array_size = tmp_size;
	size_t array_bytes = array_size * sizeof(int);

	int *buf = (int*) malloc(array_bytes);
	for (int i=0; i < array_size; i++)
		buf[i] = i;

	size_t bytes_written = write(fd, buf, array_bytes);
	if (bytes_written != array_bytes)
		printf("write failed, %ld of %ld bytes\n", (long)bytes_written, (long)array_bytes);

	free(buf);
	close(fd);


	return 0;
}

