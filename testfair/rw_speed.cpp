/*
	Test program to generate IO.

	Each rank creates its own file. The filename includes the Slurm job id
  and user id to avoid conflicts. The file is filled with a simple pattern
	of data, then the data is read back, verifying the contents. This is
	repeated until the specified run time elapses. At the end of the run,
	the average throughput is reported in MB/sec, along with an easy-to-parse
	array summarizing the throughput during each second of the run. The total
	throughput of all ranks is combined, but a standard deviation across ranks
  is also output.

	Example:
	  ./rw_speed -filesize=100m -iosize=4k -time=5 -prefix=/myfs/rw_speed -tag=job0

    sample output:
		  rw_speed.job0 user=42 mbps=52.3 mbps_rank_stddev=3.6 mbps_split=(12.1,55.1,50.9,52.1,45.0)
*/


#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <climits>
#include <cassert>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <string>
#include <vector>
#include <iomanip>
#include <sstream>
#include <mpi.h>

using std::string;
using std::vector;

int np, rank;
double t0;

const double DEFAULT_RUN_TIME_SEC = 10;
const char *DEFAULT_FILENAME_PREFIX = "/myfs/rw_speed";
const int DEFAULT_IO_SIZE = 4096;
const long DEFAULT_FILE_SIZE = 1024*1024*100;

int parseSize(const char *str, uint64_t *result);
double getTime() {return MPI_Wtime() - t0;}


struct Options {
	string tag;

	string filename_prefix;

	// how long should this run
	double run_time_sec;

	// size of each io operation
	int io_size;

	// maximum test file size
	long file_size;

	// delete test file iff cleanup==true
	bool cleanup;

	Options() :
		filename_prefix(DEFAULT_FILENAME_PREFIX),
		run_time_sec(DEFAULT_RUN_TIME_SEC),
		io_size(DEFAULT_IO_SIZE),
		file_size(DEFAULT_FILE_SIZE),
		cleanup(true)
 {}

	void printHelp() {
		if (rank == 0) {
			printf("\n"
						 "  rw_speed <opt>\n"
						 "  opt:\n"
						 "   -time=S : run for S seconds (default: %.0f)\n"
						 "   -prefix=<path> : test output filename prefix (default: %s)\n"
						 "   -iosize=<bytes> : size of each IO operations (default: %d)\n"
						 "      this will be rounded up to a multiple of 8, if necessary\n"
						 "   -filesize=<bytes> : maximum output file size (default: %ld)\n"
						 "   -tag=<name> : a name included in output to aid analysis\n"
						 "   -nodelete : don't delete output file\n"
						 "  <bytes>: k, m, g suffixes recognized (e.g. \"4k\", \"1.5G\")\n"
						 "\n",
						 DEFAULT_RUN_TIME_SEC, DEFAULT_FILENAME_PREFIX, DEFAULT_IO_SIZE, DEFAULT_FILE_SIZE);
						 
		}
		MPI_Finalize();
		exit(1);
		return;
	}

	static bool startsWith(const char *s, const char *prefix) {
		size_t len = strlen(prefix);
		return 0 == strncmp(s, prefix, len);
	}

	bool parseArgs(int argc, char **argv) {
		uint64_t tmp64;

		for (int argno=1; argno < argc; argno++) {
			const char *arg = argv[argno];

			if (startsWith(arg, "-time=")) {
				if (1 != sscanf(arg+6, "%lf", &run_time_sec) ||
						run_time_sec <= 0) {
					if (rank == 0) {
						printf("Invalid run time: %s\n", arg);
					}
					return false;
				}
			}

			else if (startsWith(arg, "-prefix=")) {
				filename_prefix = arg+8;
			}

			else if (startsWith(arg, "-iosize=")) {
				if (parseSize(arg+8, &tmp64)) {
					if (rank==0)
						printf("Failed to parse io size: %s\n", arg);
					return false;
				}
				if (tmp64 > INT_MAX-3) {
					if (rank==0)
						printf("io size too large: %s\n", arg);
					return false;
				}
				io_size = (int)tmp64;

				// round up to a multiple of 8
				io_size = (io_size + 7) & ~7;
			}

			else if (startsWith(arg, "-filesize=")) {
				if (parseSize(arg+10, &tmp64)) {
					if (rank==0)
						printf("Failed to parse file size: %s\n", arg);
					return false;
				}
				file_size = tmp64;
			}

			else if (startsWith(arg, "-tag=")) {
				tag = arg+5;
			}

			else if (!strcmp(arg, "-nodelete")) {
				cleanup = false;
			}
		}
		return true;
	}

	string getFilename() {
		std::ostringstream buf;
		buf << filename_prefix << ".rank" << rank << ".uid" << getuid();
		return buf.str();
	}

};


/* track IO throughput over time, tracking throughput during each second
	 of the run. */
class IOOverTime {
public:
	IOOverTime(int io_size_)
		: io_size(io_size_)
	{}

	// add one io event at the current time
	void inc() {
		double timestamp = getTime();
		size_t slice_idx = (size_t) timestamp;
		if (slice_idx >= slice_bytes.size()) {
			slice_bytes.resize(slice_idx);
		}
		slice_bytes[slice_idx] += io_size;
	}

	string toString() {
		std::ostringstream buf;
		buf << "(";
		for (size_t i=0; i < slice_bytes.size(); i++) {
			if (i > 0) buf << ",";
			double mbps = (double)slice_bytes[i] / (1<<20);
			buf << std::setprecision(1) << mbps;
		}
		buf << ")";
		return buf.str();
	}

private:
	int io_size;

	// slice_bytes[i] sums the number of bytes output from time i to i+1
	vector<long> slice_bytes;
	
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


void fillBuffer(vector<long> &data, long file_offset) {
	for (size_t i=0; i < data.size(); i++) {
		data[i] = file_offset;
		file_offset += 8;
	}
}
	

int main(int argc, char **argv) {
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &np);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	t0 = MPI_Wtime();

	Options opt;
	if (!opt.parseArgs(argc, argv)) {
		MPI_Finalize();
		return 1;
	}

	long io_bytes = 0;
	IOOverTime io_over_time(opt.io_size);
	
	string filename_str = opt.getFilename();
	const char *filename = filename_str.c_str();

	int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (fd == -1) {
		printf("[%d] Error creating %s: %s\n", rank, filename, strerror(errno));
		fflush(stdout);
		assert(0 == "failed to create output file");
	}

	vector<long> data(opt.io_size / sizeof(long));
	vector<long> expected_data(opt.io_size / sizeof(long));
	long file_offset;
	bool done = false;

	MPI_Barrier(MPI_COMM_WORLD);
	double start_time = MPI_Wtime();

	while (!done) {
		
		// write to the file

		printf("[%d] writing...\n", rank);
		for (file_offset = 0; 
				 file_offset < opt.file_size;
				 file_offset += opt.io_size) {

			// out of time
			if (getTime() > opt.run_time_sec) break;

			fillBuffer(data, file_offset);
			int bytes_written = write(fd, data.data(), opt.io_size);
			if (bytes_written != opt.io_size) {
				printf("[%d] write fail, %d of %d bytes\n", rank, bytes_written, opt.io_size);
				done = true;
				break;
			}
			io_bytes += opt.io_size;
			io_over_time.inc();
		}

		// read from the file
		printf("[%d] reading...\n", rank);

		// XXX ThemisIO doesn't yet support seek
		close(fd);
		fd = open(filename, O_RDONLY);
		assert(fd >= 0);
		
		for (file_offset = 0; 
				 file_offset < opt.file_size;
				 file_offset += opt.io_size) {

			// out of time
			if (getTime() > opt.run_time_sec) break;

			fillBuffer(expected_data, file_offset);
			int bytes_read = read(fd, data.data(), opt.io_size);
			if (bytes_read != opt.io_size) {
				printf("[%d] read fail, %d of %d bytes\n", rank, bytes_read, opt.io_size);
				done = true;
				break;
			}
			if (memcmp(expected_data.data(), data.data(), opt.io_size)) {
				printf("[%d] data read back incorrectly at offset %ld\n", rank, file_offset);
				done = true;
				break;
			}

			io_bytes += opt.io_size;
			io_over_time.inc();
		}

		// XXX ThemisIO doesn't yet support seek
		close(fd);
		fd = open(filename, O_WRONLY);
		assert(fd >= 0);
	}

	MPI_Barrier(MPI_COMM_WORLD);
	double elapsed_sec = MPI_Wtime() - start_time;

	long total_io_bytes = 0;
	MPI_Reduce(&io_bytes, &total_io_bytes, 1, MPI_LONG, MPI_SUM, 0,
						 MPI_COMM_WORLD);

	double total_mbps = total_io_bytes / ((1<<20) * elapsed_sec);

	for (int r=0; r < np; r++) {
		MPI_Barrier(MPI_COMM_WORLD);
		if (r == rank) {
			printf("[%d] mbps slices %s\n", rank, io_over_time.toString().c_str());
			fflush(stdout);
			usleep(100000);
		}
	}

	if (rank==0) {
		string tag;
		if (opt.tag.size() > 0) tag = "." + opt.tag;
		printf("rw_speed%s user=%ld mbps=%.3f",
					 tag.c_str(), (long)getuid(), total_mbps);
	}

	close(fd);

	if (opt.cleanup)
		remove(filename);

	return 0;
}
