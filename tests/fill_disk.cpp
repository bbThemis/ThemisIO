#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <string>

using std::string;

const char *DEFAULT_FILENAME = "/myfs/fill_disk.out";
const int DEFAULT_BLOCK_SIZE = 1<<30;
const long DEFAULT_MAX_SIZE = (1L<<30) * 1;
const int REPORT_INTERVAL = 1<<30;

double t0 = 0;


int parseSize(const char *str, uint64_t *result);


/* Handle options that can be set on the command line.
*/
struct Options {

	Options() : filename(DEFAULT_FILENAME), block_size(DEFAULT_BLOCK_SIZE), max_size(DEFAULT_MAX_SIZE) {}

	// Returns true iff the command line arguments are successfully parsed.
	bool parseArgs(int argc, char **argv) {
		unsigned long tmp64;

		for (int argno = 1; argno < argc; argno++) {
			const char *arg = argv[argno];

			if (!strcmp(arg, "-h") || !strcmp(arg, "--help")) {
				printHelp();
			}

			else if (startsWith(arg, "-f=")) {
				filename = arg+3;
			}

			else if (startsWith(arg, "-b=")) {
				if (parseSize(arg+3, &tmp64)) {
					printf("Invalid block size: %s\n", arg+3);
					return false;
				}
				block_size = tmp64;
			}

			else if (startsWith(arg, "-m=")) {
				if (parseSize(arg+3, &tmp64)) {
					printf("Invalid maximum size: %s\n", arg+3);
					return false;
				}
				max_size = tmp64;
			}

			else {
				return false;
			}
		}

		return true;
	}

	static bool startsWith(const char *s, const char *prefix) {
		size_t len = strlen(prefix);
		return 0 == strncmp(s, prefix, len);
	}

	void printHelp() {
		printf("\n"
					 "  fill_disk [opt]\n"
					 "  Writes to a file until write() fails or until a maximum file size is reached.\n"
					 "  opt:\n"
					 "    -f=<filename>\n"
					 "    -b=<block size>  (how many bytes in each call to write())\n"
					 "    -m=<maximum file size>  (k, m, g stuffixes recognized)\n"
					 "\n");
		exit(1);
	}


	string filename;
	int block_size;
	long max_size; 
};


static const uint64_t SIZE_FACTORS[6] = {
  (uint64_t)1 << 10,
  (uint64_t)1 << 20,
  (uint64_t)1 << 30,
  (uint64_t)1 << 40,
  (uint64_t)1 << 50,
  (uint64_t)1 << 60
};

static const char SIZE_SUFFIXES[6] = {'k', 'm', 'g', 't', 'p', 'x'};

static const int SIZES_COUNT = 6;


/* Parse an unsigned number with a case-insensitive magnitude suffix:
     k : multiply by 2^10
     m : multiply by 2^20
     g : multiply by 2^30
     t : multiply by 2^40
     p : multiply by 2^50
     x : multiply by 2^60

   For example, "32m" would parse as 33554432.
   Floating point numbers are allowed in the input, but the result is always
   a 64-bit integer:  ".5g" yields uint64_t(.5 * 2^30)
   Return nonzero on error.
*/
int parseSize(const char *str, uint64_t *result) {
  uint64_t multiplier;
  const char *last;
  char suffix;
  int consumed, i;
  double mantissa;
  
  /* missing argument check */
  if (!str || !str[0] || (!isdigit((int)str[0]) && str[0] != '.')) return 1;

  if (1 != sscanf(str, "%lf%n", &mantissa, &consumed))
    return 1;
  last = str + consumed;

  /* these casts are to avoid issues with signed chars */
  suffix = (char)tolower((int)*last);
  
  if (suffix == 0) {

    multiplier = 1;

  } else {
    
    for (i=0; i < SIZES_COUNT; i++) {
      if (suffix == SIZE_SUFFIXES[i]) {
        break;
      }
    }
    
    /* unrecognized suffix */
    if (i == SIZES_COUNT)
      return 1;

    multiplier = SIZE_FACTORS[i];
  }

  *result = (uint64_t)(multiplier * mantissa);
  return 0;
}


// return time in seconds since t0
double getTime() {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec + 1e-9 * t.tv_nsec - t0;
}


int main(int argc, char **argv) {

	Options opt;
	if (!opt.parseArgs(argc, argv)) return 1;

	t0 = getTime();

	const char *filename = opt.filename.c_str();
	int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (fd == -1) {
		printf("Failed to open \"%s\" for writing: %s\n", filename, strerror(errno));
		return 1;
	}

	printf("Writing up to %ld bytes (%.1f GB) to %s in blocks of %d bytes\n", 
				 opt.max_size, (double)opt.max_size / (1<<30), filename, opt.block_size);

	char *buf = (char*) malloc(opt.block_size);
	if (!buf) {
		printf("Failed to allocate buffer for %d bytes\n", opt.block_size);
		return 1;
	}
	memset(buf, 0XFF, opt.block_size);

	long file_offset = 0;

	// output the current size every REPORT_INTERVAL bytes
	long next_report_size = REPORT_INTERVAL;
	
	while (1) {
		int bytes_written = write(fd, buf, opt.block_size);
		if (bytes_written == -1) {
			printf("\nError writing %d bytes at offset %ld: %s\n",
						 opt.block_size, file_offset, strerror(errno));
			break;
		}

		file_offset += bytes_written;

		if (bytes_written != opt.block_size) {
			printf("\nMaximum size reached at %ld bytes.\n", file_offset);
			break;
		}

		if (file_offset >= next_report_size) {
			printf("\r%.1fs file size %ld GB...", getTime(), file_offset / (1<<30));
			fflush(stdout);
			next_report_size += REPORT_INTERVAL;
		}

		if (file_offset >= opt.max_size) {
			printf("\nStopping at %ld bytes.\n", file_offset);
			break;
		}
	}

	close(fd);
	// remove(filename);
	free(buf);

	return 0;
}
