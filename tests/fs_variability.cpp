/*
  Test file system performance variability.
  
  This is a long-term test (it should run for days or weeks) of how much
  variability there is in simple file system operations. It should do
  small tests which should not produce a noticeable burden on the file system.

  Hypothesis A: Lustre data servers (OSTs) are underutilized and there
    will be little variability in their performance, since there will
    rarely be competing jobs stressing the data servers.
  Hypothesis B: Lustre metadata servers (MDS) are frequently
    overloaded and will demonstrate high variability in their
    performance.

  Why is this important? Much work is done to balance data server throughput,
  but I suspect metadata servers are the the source of most delays.

  Data server tests
   - Response time reading or writing a few small (1k) pieces of a large file.
   - Throughput doing significant (10GB) reading and writing of a file.
     Do the operations in large chunks matching stripe length to maximize throughput.
     Track variability within each operation (e.g. writing 10GB in 4MB will take
     2560 writes. Do they take a consistent amount of time?)

  Metadata server tests
   - Browse a large number of files (a source code tree) calling stat() on each.
   - Create a directory, fill it with many files, remove them, and remove the directory.


  Results after a 1 hour test with 180 samples
    OST test avg 13.5, stddev 3.4s. 8 samples more than 1 stddev from the mean
      timestamp               data            read-metadata   write-metadata
      2022-03-13:20:15:34     21.933110       0.015598        0.313226
        slow OSTs 0,2,3,4,6,8,9,10,14,15
      2022-03-13:20:16:37     29.475119       0.015423        0.308891
        slow OSTs 4,11
      2022-03-13:20:17:46     39.422197       0.015102        0.307789
        slow OSTs 3
      2022-03-13:20:19:21     25.757150       0.017961        0.321571
        slow OSTs 4
      2022-03-13:20:20:19     27.556489       0.015681        0.320795
        slow OSTs 4,14
      2022-03-13:20:27:39     29.257917       0.015370        0.309817
        slow OSTs 14
      2022-03-13:20:28:32     28.551908       0.015430        0.312593
        slow OSTs 4,14
      2022-03-13:21:00:58     17.661031       0.019882        0.310800
        slow OSTs 1,2,3,5,7,8,9,15

    Metadata read test avg 18ms, stddev 10.5ms. 5 samples more than 1 stddev from the mean
      timestamp               data            read-metadata   write-metadata
      2022-03-13:20:15:13     15.467214       0.042042        0.310676
      2022-03-13:20:30:24     14.730028       0.127350        0.313080
      2022-03-13:20:37:33     12.020824       0.061102        0.791560
      2022-03-13:20:45:33     13.799884       0.057163        0.313177
      2022-03-13:21:00:40     13.323939       0.073224        0.288278

    Metadata write test avg 311ms, stddev 42ms. 2 samples more than 1 stddev from the mean
      2022-03-13:20:06:46     12.218750       0.016094        0.537424
      2022-03-13:20:37:33     12.020824       0.061102        0.791560

    There were 10 instances where it took more than 1 second for an OST to write 4 MB
      OST  Timestamp            Slowest write in ms
        4  2022-03-13:20:16:37  2192.391
       11  2022-03-13:20:16:37  2287.402
        3  2022-03-13:20:17:46  7719.752
        4  2022-03-13:20:19:21  3612.800
       14  2022-03-13:20:20:19  2276.740
        4  2022-03-13:20:20:19  3758.714
        4  2022-03-13:20:21:13  1735.732
       14  2022-03-13:20:27:39  4099.515
       14  2022-03-13:20:28:32  2830.613
        4  2022-03-13:20:28:32  4097.189



  Ed Karrels, edk@illinois.edu, March 2022
*/


#include <cassert>
#include <cstring>
#include <ctime>
#include <algorithm>
#include <sstream>
#include <string>
#include <errno.h>
#include <fcntl.h>
#include <fts.h>
#include <sys/stat.h>
#include <unistd.h>
#include "common.h"
#include "lustre_wrapper.h"

using std::string;
using std::vector;

struct Options {
  // int small_access_size = 1024;
  int block_size;
  long file_size;
  int lustre_ost_count;
  string scan_directory;
  string test_directory;
  int create_file_count;
  double test_frequency_seconds;
  int test_length_seconds;

  Options() :
    block_size(4 * (1<<20)),
    file_size(16L * (1<<20)),
    lustre_ost_count(1),
    scan_directory("."),
    test_directory("."),
    create_file_count(1000),
    test_frequency_seconds(5),
    test_length_seconds(60)
  {}

  bool parseArgs(int argc, char **argv);
};


void printHelp() {
  fprintf(stderr, "\n  fs_variability <opts>\n"
         "  opts:\n"
         "  -blocksize=<n> : stripe length and size of each read() and write()\n"
         "  -filesize=<n> : amount of data to be written to and read from each data file\n"
         "  -stripes=<n> : number of Lustre stripes to test\n"
         "  -readdir <path> : directory to be scanned with calls to stat()\n"
         "  -writedir <path> : directory in which to create temporary files\n"
         "  -filecount=<n> : number of empty files to create in open() test\n"
         "  -freq=<seconds> : how often tests are run\n"
         "  -time=<hh:mm:ss> : how long to run tests\n"
         "  -h : print this help output\n"
         "\n");
  exit(1);
}


int initDataFiles(const Options &opt, vector<int> &data_file_handles, vector<FILE*> &per_ost_output);
string dataFileName(int index, const Options &opt);
string tempFileName(int index, const Options &opt);

/* Read and write the full contents of the data file on each OST.
   Write individual timer values to per_ost_output[ost_index]
   and log the total time in (timer). */
void testData(const char *timestamp, vector<int> &data_file_handles, vector<FILE*> &per_ost_output,
              const Options &opt, Timer &timer);

/* Recursively scans all the files in directory (root), calling stat() on each.
   Returns number of files scanned. */
int testMetadataRead(const char *root, Timer &timer);

/* Create opt.create_file_count files in opt.test_directory and then remove them. */
void testMetadataWrite(const Options &opt, Timer &timer);

void removeDataFiles(const Options &opt, vector<int> &data_file_handles);

/* Go back to the top of the file. */
void resetFilePos(int fd);
/* Writes copies of (block) until (size) bytes have been written to (fd).
   Returns 0 on success. */
int writeData(int fd, long size, const char *block, int block_size, Timer &timer);
/* Reads data repeatedly into (block) until (size) bytes have been read from (fd).
   Returns 0 on success. */
int readData(int fd, long size, char *block, int block_size, Timer &timer);


int main(int argc, char **argv) {
  Options opt;
  if (!opt.parseArgs(argc, argv)) printHelp();

  printf("# %s fs_variability -blocksize=%d -filesize=%ld -stripes=%d -readdir=\"%s\" -writedir=\"%s\" -filecount=%d -freq=%.3f -time=%d:%02d:%02d\n",
         timestamp(0).c_str(), opt.block_size, opt.file_size, opt.lustre_ost_count, opt.scan_directory.c_str(),
         opt.test_directory.c_str(), opt.create_file_count, opt.test_frequency_seconds,
         opt.test_length_seconds / 3600, (opt.test_length_seconds / 60) % 60, opt.test_length_seconds % 60);
  
  double start_time = getSeconds();
  double end_time = start_time + opt.test_length_seconds;
  vector<int> data_file_handles;
  vector<FILE*> per_ost_output;
  Timer unused_timer;

  if (initDataFiles(opt, data_file_handles, per_ost_output)) {
    // removeDataFiles(opt, data_file_handles);
    return 1;
  }

  int scan_file_count = testMetadataRead(opt.scan_directory.c_str(), unused_timer);

  printf("# timestamp\tdata_sec\tmetadata_read_sec(count=%d)\tmetadata_write_sec\n", scan_file_count);
  
  do {
    string timestamp_str = timestamp(0);
    const char *ts = timestamp_str.c_str();
    Timer data_timer, metadata_read_timer, metadata_write_timer;

    testData(ts, data_file_handles, per_ost_output, opt, data_timer);
    testMetadataRead(opt.scan_directory.c_str(), metadata_read_timer);
    testMetadataWrite(opt, metadata_write_timer);

    printf("%s\t%.6f\t%.6f\t%.6f\n",
           ts, data_timer.getSum(),
           metadata_read_timer.getSum(), metadata_write_timer.getSum());
    fflush(stdout);
    
    sleepSeconds(opt.test_frequency_seconds);
  } while (getSeconds() < end_time);

  removeDataFiles(opt, data_file_handles);
  return 0;
}


bool Options::parseArgs(int argc, char **argv) {
  uint64_t tmp64;
  
  for (int argno = 1; argno < argc && argv[argno][0] == '-'; argno++) {
    const char *arg = argv[argno];

    if (startsWith(arg, "-blocksize=")) {
      arg += 11;
      if (parseSize(arg, &tmp64)
          || tmp64 < 65536
          || (tmp64 & 65535)) {
        fprintf(stderr, "Invalid block size (must be multiple of 64K): %s\n", arg);
        return false;
      }
      block_size = tmp64;
    }

    else if (startsWith(arg, "-filesize=")) {
      arg += 10;
      if (parseSize(arg, &tmp64)) {
        fprintf(stderr, "Invalid file size: %s\n", arg);
        return false;
      }
      file_size = tmp64;
    }

    else if (startsWith(arg, "-stripes=")) {
      arg += 9;
      if (1 != sscanf(arg, "%d", &lustre_ost_count)
          || lustre_ost_count < 1) {
        fprintf(stderr, "Invalid Lustre OST count: %s\n", arg);
        return false;
      }
    }

    else if (!strcmp(arg, "-readdir")) {
      arg = argv[++argno];
      if (!arg || !isDirectory(arg)) {
        fprintf(stderr, "Not a directory: %s\n", arg);
        return false;
      }
      scan_directory = arg;
    }

    else if (!strcmp(arg, "-writedir")) {
      arg = argv[++argno];
      if (!arg || !isDirectory(arg)) {
        fprintf(stderr, "Not a directory: %s\n", arg);
        return false;
      }
      test_directory = arg;
    }

    else if (startsWith(arg, "-filecount=")) {
      arg += 11;
      if (1 != sscanf(arg, "%d", &create_file_count)
          || create_file_count < 1) {
        fprintf(stderr, "Invalid create file count: %s\n", arg);
        return false;
      }
    }

    else if (startsWith(arg, "-freq=")) {
      arg += 6;
      if (1 != sscanf(arg, "%lf", &test_frequency_seconds)
          || test_frequency_seconds <= 0) {
        fprintf(stderr, "Invalid test frequency: %s\n", arg);
        return false;
      }
    }

    else if (startsWith(arg, "-time=")) {
      int hours, minutes, seconds;
      arg += 6;
      if (3 != sscanf(arg, "%d:%d:%d", &hours, &minutes, &seconds)
          || hours < 0 || minutes < 0 || seconds < 0) {
        fprintf(stderr, "Invalid test length (hh:mm:ss): %s\n", arg);
        return false;
      }
      test_length_seconds = seconds + 60 * (minutes + 60 * hours);
    }

    else if (!strcmp(arg, "-h")) {
      printHelp();
    }

    else {
      fprintf(stderr, "Unrecognized argument: %s\n", arg);
      return false;
    }
  }

  if (file_size % block_size) {
    fprintf(stderr, "File size is not a multiple of block size. Accesses will not be consistent sizes.\n");
    return false;
  }
  
  return true;
}


/* Returns 0 iff all files were created successfully.
   If some failed, their entries in data_file_handles will be -1. */
int initDataFiles(const Options &opt, vector<int> &data_file_handles, vector<FILE*> &per_ost_output) {
  Timer unused_timer;
  int err, any_err = 0;
  vector<char> block(opt.block_size, 'x');

  data_file_handles.clear();
  data_file_handles.resize(opt.lustre_ost_count, -1);

  per_ost_output.clear();
  per_ost_output.resize(opt.lustre_ost_count, NULL);
  
  for (int i=0; i < opt.lustre_ost_count; i++) {
    
    // open an output file for the log
    char fnbuf[100];
    sprintf(fnbuf, "fs_variability.out.ost%d", i);
    FILE *outf = fopen(fnbuf, "w");
    if (!outf) {
      fprintf(stderr, "Failed to open output file %s\n", fnbuf);
      return -1;
    }
    per_ost_output[i] = outf;
    fprintf(outf, "# timestamp\tread_mbps\tmin_read_ms\tavg_read_ms\tmax_read_ms\twrite_mbps\tmin_write_ms\tavg_write_ms\tmax_write_ms\n");
    
    int fd = -1;
    string filename_str = dataFileName(i, opt);
    const char *filename = filename_str.c_str();
    // remove the file if it exists, just in case
    remove(filename);
    err = lustre_create_striped(filename, 0600, 1, opt.block_size, i);
    if (err) {
      any_err = 1;
      const char *err_str =
        err == EINVAL ? "invalid parameter"
        : err == EEXIST ? "file already exists"
        : err == ENOTTY ? "not a Lustre file system"
        : "unknown error";
      fprintf(stderr, "Failed to create \"%s\" with %d-byte stripe on OST %d: %s\n",
             filename, opt.block_size, i, err_str);
    } else {
      fd = open(filename, O_RDWR);
      if (fd == -1) {
        any_err = 1;
        fprintf(stderr, "Failed to open \"%s\": %s\n", filename, strerror(errno));
      } else {
        // printf("writing %s\n", filename);
        Timer dummy_timer;
        if (writeData(fd, opt.file_size, block.data(), opt.block_size, dummy_timer)) {
          any_err = 1;
        }
      }
    }
    
    data_file_handles[i] = fd;
  }
  
  return any_err;
}


string dataFileName(int index, const Options &opt) {
  std::ostringstream buf;
  buf << opt.test_directory << "/data.ost." << index;
  return buf.str();
}


string tempFileName(int index, const Options &opt) {
  std::ostringstream buf;
  buf << opt.test_directory << "/tmp_meta." << index;
  return buf.str();
}
  


/* Read the full contents of the data file on each OST, then write them.
   Write individual timer values to per_ost_output[ost_index],
   and log the total time in (timer). */
void testData(const char *timestamp, vector<int> &data_file_handles, vector<FILE*> &per_ost_output,
              const Options &opt, Timer &timer) {
  vector<char> block(opt.block_size, '.');
  vector<Timer> read_timers(opt.lustre_ost_count);
  vector<Timer> write_timers(opt.lustre_ost_count);

  assert(data_file_handles.size() == (size_t)opt.lustre_ost_count);
  assert(per_ost_output.size() == (size_t)opt.lustre_ost_count);

  // read every file
  for (int ost=0; ost < opt.lustre_ost_count; ost++) {
    int fd = data_file_handles[ost];
    resetFilePos(fd);
    timer.start();
    readData(fd, opt.file_size, block.data(), opt.block_size, read_timers[ost]);
    timer.stop();
  }

  // write every file
  for (int ost=0; ost < opt.lustre_ost_count; ost++) {
    int fd = data_file_handles[ost];
    resetFilePos(fd);
    timer.start();
    writeData(fd, opt.file_size, block.data(), opt.block_size, write_timers[ost]);
    timer.stop();

    // write per-file output
    // timestamp, read(mbps, min, avg, max), write(mbps, min, avg, max)
    double read_mbps = opt.file_size / ((1<<20) * read_timers[ost].getSum());
    double write_mbps = opt.file_size / ((1<<20) * write_timers[ost].getSum());
    fprintf(per_ost_output[ost], "%s\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\n",
            timestamp,
            read_mbps, read_timers[ost].getMinimum()*1000, read_timers[ost].getAverage()*1000, read_timers[ost].getMaximum()*1000,
            write_mbps, write_timers[ost].getMinimum()*1000, write_timers[ost].getAverage()*1000, write_timers[ost].getMaximum()*1000);
    fflush(per_ost_output[ost]);
  }
}


// returns number of files scanned
int testMetadataRead(const char *root, Timer &timer) {
  // scan the directory tree at opt.scan_directory, calling stat() on each file
  FTS *fts;
  char *dir_names[] = {(char*)root, NULL};

  timer.start();
  
  // don't follow symlinks, stay on one device
  fts = fts_open(dir_names, FTS_PHYSICAL | FTS_XDEV, NULL);
  if (!fts) {
    fprintf(stderr, "Failed to traverse %s: %s\n", dir_names[0], strerror(errno));
    return 0;
  }

  int file_count = 0;
  FTSENT *entry;
  while ((entry = fts_read(fts)) != NULL) {
    if (entry->fts_info == FTS_F) {
      file_count++;
      // printf("%d(%ld): %s\n", file_count, (long)entry->fts_statp->st_size, entry->fts_path);
    }
  }

  timer.stop();
  fts_close(fts);
  
  return file_count;
}


/* Create opt.create_file_count files in opt.test_directory and then remove them. */
void testMetadataWrite(const Options &opt, Timer &timer) {
  timer.start();

  vector<string> filenames;
  for (int i=0; i < opt.create_file_count; i++) {
    string name = tempFileName(i, opt);
    filenames.push_back(name);
    int fd = creat(name.c_str(), 0600);
    // printf("  created %s (%d)\n", name.c_str(), fd);
    if (fd != -1) close(fd);
  }

  for (int i=0; i < opt.create_file_count; i++) {
    remove(filenames[i].c_str());
    // printf("  removed %s\n", filenames[i].c_str());
  }  
  
  timer.stop();
}


void removeDataFiles(const Options &opt, vector<int> &data_file_handles) {
  for (int i=0; i < opt.lustre_ost_count; i++) {
    if (data_file_handles[i] != -1)
      close(data_file_handles[i]);
    string filename = dataFileName(i, opt);
    remove(filename.c_str());
  }
}


/* Go back to the top of the file. */
void resetFilePos(int fd) {
  off_t pos = lseek(fd, 0, SEEK_SET);
  if (pos == -1) {
    fprintf(stderr, "Error resetting file position on fd %d: %s\n", fd, strerror(errno));
  }
}

    
  


/* Writes copies of (block) until (size) bytes have been written to (fd).
   Returns 0 on success. */
int writeData(int fd, long size, const char *block, int block_size, Timer &timer) {
  long remaining = size;

  while (remaining > 0) {
    timer.start();
    ssize_t bytes_written = write(fd, block, block_size);
    timer.stop();
    if (bytes_written == -1) {
      printf("write() fail: %s\n", strerror(errno));
      return -1;
    } else if (bytes_written != block_size) {
      printf("?? short write to fd %d: %d of %d bytes\n", fd, (int)bytes_written, block_size);
      return -1;
    }
    remaining -= block_size;
  }
  return 0;
}


/* Reads data repeatedly into (block) until (size) bytes have been read from (fd).
   Returns 0 on success. */
int readData(int fd, long size, char *block, int block_size, Timer &timer) {
  long remaining = size;

  while (remaining > 0) {
    timer.start();
    ssize_t bytes_read = read(fd, block, block_size);
    timer.stop();
    if (bytes_read == -1) {
      printf("read() fail: %s\n", strerror(errno));
      return -1;
    } else if (bytes_read != block_size) {
      printf("?? short read to fd %d: %d of %d bytes\n", fd, (int)bytes_read, block_size);
      return -1;
    }
    remaining -= block_size;
  }
  return 0;
}
