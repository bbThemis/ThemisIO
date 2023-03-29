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

    -time=S : run for S seconds
		-prefix=<path> : test output filename prefix
		-iosize=<bytes> : size of each IO operations
		-filesize=<bytes> : maximum output file size
		-tag=<name> : a name included in output to aid analysis
		-nodelete : don't delete output file

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
#include <cmath>
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

int np, rank, node_count;
double t0;

const double DEFAULT_RUN_TIME_SEC = 10;
const char *DEFAULT_FILENAME_PREFIX = "/myfs/rw_speed";
const int DEFAULT_IO_SIZE = 4096;
const long DEFAULT_FILE_SIZE = 1024*1024*100;

int parseSize(const char *str, uint64_t *result);
double getTime() {return MPI_Wtime() - t0;}
int getNodeCount();
// use SLURM_NNODES and THEMIS_FAKE_NNODES the same way qp_client uses them to get the node count for a job
int getSimulatedNodeCount();
std::string timestamp(int use_utc_time);


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
		exit(0);
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

      else {
        printHelp();
      }
		}
		return true;
	}

	string getFilename() {
		char nodename[MPI_MAX_PROCESSOR_NAME+1];
		int namelen;
		MPI_Get_processor_name(nodename, &namelen);
		nodename[namelen] = 0;

		// truncate after first part of the name
		char *dot = strchr(nodename, '.');
		if (dot) {
			*dot = 0;
		}
		
		std::ostringstream buf;
		// buf << filename_prefix << ".rank" << rank << ".node" << nodename << ".pid" << getpid() << ".uid" << getuid();
		// buf << filename_prefix << "." << nodename << "." << getpid() << "_S00";
		buf << filename_prefix << "." << nodename << "." << getpid();
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
			slice_bytes.resize(slice_idx+1);
		}
		slice_bytes[slice_idx] += io_size;
	}

	string toString() {
		std::ostringstream buf;
		buf << "(";
		for (size_t i=0; i < slice_bytes.size(); i++) {
			if (i > 0) buf << ",";
			double mbps = (double)slice_bytes[i] / (1<<20);
			buf << std::fixed << std::setprecision(1) << mbps;
		}
		buf << ")";
		return buf.str();
	}

	/* Gather all data onto rank 0. */
	void gather() {
		// make sure everyone is the same length
		int max_len = slice_bytes.size();
		MPI_Allreduce(MPI_IN_PLACE, &max_len, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
		assert(max_len >= slice_bytes.size());
		slice_bytes.resize(max_len, 0);
		MPI_Reduce(rank==0 ? MPI_IN_PLACE : slice_bytes.data(), slice_bytes.data(),
							 max_len, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
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


int getNodeCount() {
	int rank, is_rank0, nodes;
	MPI_Comm shmcomm;

	MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
											MPI_INFO_NULL, &shmcomm);
	MPI_Comm_rank(shmcomm, &rank);
	is_rank0 = (rank == 0) ? 1 : 0;
	MPI_Allreduce(&is_rank0, &nodes, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
	MPI_Comm_free(&shmcomm);
	return nodes;
}


// use SLURM_NNODES and THEMIS_FAKE_NNODES the same way qp_client uses them to get the node count for a job
int getSimulatedNodeCount() {
	const char *nodes_str = getenv("THEMIS_FAKE_NNODES");
	if (!nodes_str)
		nodes_str = getenv("SLURM_NNODES");
	if (!nodes_str)
		return 0;
	return atoi(nodes_str);
}


/* Returns the current time in the form YYYY-mm-dd:HH:MM:SS.
   If is_utc_time is nonzero, use the current UTC/GMT time. Otherwise
   use local time. */
std::string timestamp(int use_utc_time) {
  struct tm tm_struct;
  time_t now;
	char buf[20];

  time(&now);
  
  if (use_utc_time) {
    gmtime_r(&now, &tm_struct);
  } else {
    localtime_r(&now, &tm_struct);
  }

  strftime(buf, 20, "%Y-%m-%d:%H:%M:%S", &tm_struct);

  return buf;
}


void fillBuffer(vector<long> &data, long file_offset) {
	for (size_t i=0; i < data.size(); i++) {
		data[i] = file_offset;
		file_offset += 8;
	}
}


void gatherStats(long io_bytes, long &total_io_bytes, double &rank_io_bytes_stddev) {
  // sum all ranks io_bytes into total_io_bytes
	// MPI_Reduce(&io_bytes, &total_io_bytes, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

  // gather all values of io_bytes to compute standard deviation
  vector<long> rank_io_bytes(np);
  MPI_Gather(&io_bytes, 1, MPI_LONG, rank_io_bytes.data(), 1, MPI_LONG, 0, MPI_COMM_WORLD);

  if (rank == 0) {
    total_io_bytes = 0;
    double sum2 = 0;  // sum of squares
    for (int i=0; i < np; i++) {
      total_io_bytes += rank_io_bytes[i];
      sum2 += (double)rank_io_bytes[i] * rank_io_bytes[i];
    }
    double variance = (sum2 - ((double)total_io_bytes * total_io_bytes)/np) / (np-1);
    rank_io_bytes_stddev = sqrt(variance);
  }
}
	

int main(int argc, char **argv) {
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &np);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	node_count = getNodeCount();
	int simulated_node_count = getSimulatedNodeCount();
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
	printf("[%d] filename=\"%s\"\n", rank, filename);
	char nFileName[100];
	sprintf(nFileName, "%s1", filename);
	int fd = open(nFileName, O_WRONLY | O_CREAT | O_TRUNC, 0644);

	// make sure everyone was able to open their files
	int min_fd;
	MPI_Allreduce(&fd, &min_fd, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
	if (min_fd < 0) {
	  if (fd == -1) {
	    printf("[%d] Error creating %s: %s\n", rank, filename, strerror(errno));
	  }
	  MPI_Finalize();
	  return 1;
	}

	vector<long> data(opt.io_size / sizeof(long));
	vector<long> expected_data(opt.io_size / sizeof(long));
	long file_offset;
	bool done = false;

	if (rank==0) {
		char hostname[256];
		gethostname(hostname, sizeof hostname);
		printf("rw_speed time=%s np=%d nn=%d sim_nn=%d rank0host=%s -prefix=%s -iosize=%d -filesize=%ld -time=%.1f -tag=%s%s\n",
					 timestamp(0).c_str(), np, node_count, simulated_node_count, hostname, opt.filename_prefix.c_str(), opt.io_size, opt.file_size,
					 opt.run_time_sec, opt.tag.c_str(), opt.cleanup ? "" : " -nodelete");
	}

	MPI_Barrier(MPI_COMM_WORLD);
	double start_time = MPI_Wtime();
	int iteration = 0;

	while (!done) {
		iteration++;

		// write to the file

		// printf("[%d] %.3f writing...\n", rank, getTime());
		for (file_offset = 0; 
				 file_offset < opt.file_size;
				 file_offset += opt.io_size) {

			// out of time
			if (getTime() > opt.run_time_sec) {
       			done=true;
       			break;
      		}
			// if(iteration == 2) break;
			fillBuffer(data, file_offset);
			int bytes_written = write(fd, data.data(), opt.io_size);
			// printf("write fd %d\n", fd);
			if (bytes_written != opt.io_size) {
				printf("[%d] %.6f write fail, %d of %d bytes\n", rank, getTime(), bytes_written, opt.io_size);
				done = true;
				break;
			}
			io_bytes += opt.io_size;
			io_over_time.inc();
		}

		// read from the file
		// printf("[%d] %.3f reading...\n", rank, getTime());

		// XXX ThemisIO doesn't yet support seek
		close(fd);
		char nFileName[100];
		sprintf(nFileName, "%s%d", filename, iteration);
		fd = open(nFileName, O_RDONLY);
		if (fd < 0) {
			printf("[%d] %.6f read iteration %d of %s, open returned %d, error %s\n",
						 rank, getTime(), iteration, filename, fd, strerror(errno));
			break;
		}
		// else {
		// 	printf("open file 1 %s\n", nFileName);
		// }
			
		assert(fd >= 0);
		
		for (file_offset = 0; 
				 file_offset < opt.file_size;
				 file_offset += opt.io_size) {

			// out of time
			if (getTime() > opt.run_time_sec) {
				done=true;
				break;
      		}
			// if(file_offset == 0) continue;

			fillBuffer(expected_data, file_offset);
			int bytes_read = pread(fd, data.data(), opt.io_size, file_offset);
			// printf("read fd %d file_offset %ld\n", fd, file_offset);
			if (bytes_read != opt.io_size) {
				printf("[%d] %.6f read fail, %d of %d bytes\n", rank, getTime(), bytes_read, opt.io_size);
				done = true;
				break;
			}
			int wrong_byte_idx = -1;
			if (wrong_byte_idx = memcmp(expected_data.data(), data.data(), opt.io_size)) {
				printf("[%d] %.6f data read back incorrectly at offset %ld at idx %d\n", rank, getTime(), file_offset, wrong_byte_idx);
				done = true;
				
				break;
			}

			io_bytes += opt.io_size;
			io_over_time.inc();
			// printf("read finish file_offset:%ld\n", file_offset);
		}

		// XXX ThemisIO doesn't yet support seek
		close(fd);
		sprintf(nFileName, "%s%d", filename, iteration + 1);
		fd = open(nFileName, O_WRONLY | O_CREAT | O_TRUNC, 0644);
		if (fd < 0) {
			printf("[%d] %.6f write iteration %d of %s, open returned %d error %s\n",
						 rank, getTime(), iteration, nFileName, fd, strerror(errno));
			break;
		} 
		// else {
		// 	printf("open file 2 %s\n", nFileName);
		// }

		assert(fd >= 0);
	}

	MPI_Barrier(MPI_COMM_WORLD);
	double elapsed_sec = MPI_Wtime() - start_time;

  // printf("[%d] %ld bytes\n", rank, io_bytes);
	long total_io_bytes;
  	double rank_io_bytes_stddev;
  	gatherStats(io_bytes, total_io_bytes, rank_io_bytes_stddev);

	double total_mbps = total_io_bytes / ((1<<20) * elapsed_sec);
  	double mbps_rank_stddev = rank_io_bytes_stddev / ((1<<20) * elapsed_sec);

	io_over_time.gather();

	// summarize output
	if (rank==0) {
		string tag;
		if (opt.tag.size() > 0) tag = "." + opt.tag;

    long user_id = getuid();
    const char *fake_user_id = getenv("THEMIS_FAKE_USERID");
    if (fake_user_id)
      sscanf(fake_user_id, "%ld", &user_id);

		char job_id[100] = {0};
		const char *slurm_jobid = getenv("SLURM_JOBID");
		if (slurm_jobid && strlen(slurm_jobid) < 50)
			snprintf(job_id, sizeof job_id, " jobid=%s", slurm_jobid);

    const char *fake_job_id = getenv("THEMIS_FAKE_JOBID");
    if (fake_job_id)
      snprintf(job_id, sizeof job_id, " jobid=%s", fake_job_id);

		printf("rw_speed%s time=%s nn=%d np=%d user=%ld%s mbps=%.3f mbps_rank_stddev=%.3f mbps_1sec_time_slices=%s\n",
					 tag.c_str(), timestamp(0).c_str(), node_count, np, user_id, job_id, total_mbps, mbps_rank_stddev,
					 io_over_time.toString().c_str());
	}

	close(fd);

	if (opt.cleanup)
		remove(filename);

  	MPI_Finalize();

	return 0;
}
