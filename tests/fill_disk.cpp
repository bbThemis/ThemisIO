#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>
#include <time.h>

const char *DEFAULT_FILENAME = "/myfs/fill_disk.out";
const int BLOCK_SIZE = 1<<30;
const int REPORT_INTERVAL = 1<<30;
const long MAX_SIZE = (1L<<30) * 1000;

double t0 = 0;


// return time in seconds since t0
double getTime() {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec + 1e-9 * t.tv_nsec - t0;
}


int main() {
	t0 = getTime();
	const char *filename = DEFAULT_FILENAME;

	int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (fd == -1) {
		printf("Failed to open \"%s\" for writing: %s\n", filename, strerror(errno));
		return 1;
	}

	char *buf = (char*) malloc(BLOCK_SIZE);
	memset(buf, 0XFF, BLOCK_SIZE);

	long file_offset = 0;

	// output the current size every REPORT_INTERVAL bytes
	long next_report_size = REPORT_INTERVAL;
	
	while (1) {
		int bytes_written = write(fd, buf, BLOCK_SIZE);
		if (bytes_written == -1) {
			printf("\nError writing %d bytes at offset %ld: %s\n",
						 BLOCK_SIZE, file_offset, strerror(errno));
			break;
		}

		file_offset += bytes_written;

		if (bytes_written != BLOCK_SIZE) {
			printf("\nMaximum size reached at %ld bytes.\n", file_offset);
			break;
		}

		if (file_offset >= next_report_size) {
			printf("\r%.1fs file size %ld GB...", getTime(), file_offset / (1<<30));
			fflush(stdout);
			next_report_size += REPORT_INTERVAL;
		}

		if (file_offset >= MAX_SIZE) {
			printf("\nStopping at %ld bytes.\n", file_offset);
			break;
		}
	}

	close(fd);
	remove(filename);
	free(buf);

	return 0;
}
