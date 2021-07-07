#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <mpi.h>
#include "simple_stats.hh"

int np, rank;
double t0;
MPI_Comm comm;
const char *FILENAME = "test_last_modified.out";

// difference between timespecs in seconds
double timespecDiffSec(const struct timespec &t1, const struct timespec &t2) {
  long sec_diff = t2.tv_sec - t1.tv_sec;
  long ns_diff = t2.tv_nsec - t1.tv_nsec;
  return sec_diff + ns_diff * 1e-9;
}


bool operator == (const struct timespec &a, const struct timespec &b) {
  return a.tv_nsec == b.tv_nsec && a.tv_sec == b.tv_sec;
}


int main(int argc, char **argv) {
  MPI_Init(&argc, &argv);
  comm = MPI_COMM_WORLD;
  MPI_Comm_size(comm, &np);
  MPI_Comm_rank(comm, &rank);
  t0 = MPI_Wtime();

  if (np != 2) {
    if (rank==0) printf("test_last_modified requires 2 processes\n");
    MPI_Finalize();
    return 1;
  }
  
  int n_tests = 100;
  if (argc > 1) {
    if (1 != sscanf(argv[1], "%d", &n_tests) || n_tests <= 0) {
      if (rank == 0)
        printf("Invalid number of tests: %s\n", argv[1]);
      MPI_Finalize();
      return 1;
    }
  }

  if (rank==0) remove(FILENAME);
  MPI_Barrier(comm);
  
  int fd = open(FILENAME, O_RDWR | O_CREAT, 0644);
  if (fd == -1) {
    printf("[%d] failed to open %s\n", rank, FILENAME);
    assert(false);
  }

  struct stat statbuf;
  struct timespec prev_mod_time;
  int dummy = rank, no_change_count = 0;
  Stats time_diff_stats;
  
  for (int test_no=0; test_no <= n_tests; test_no++) {
    if (rank == 1) {
      write(fd, &dummy, sizeof(dummy));
      MPI_Send(&dummy, 1, MPI_INT, 0, 0, comm);
    } else {
      MPI_Recv(&dummy, 1, MPI_INT, 1, 0, comm, MPI_STATUS_IGNORE);
      if (fstat(fd, &statbuf)) {
        printf("[%d] fstat fail\n", rank);
        assert(0);
      }
      if (test_no == 0) {
        prev_mod_time = statbuf.st_mtim;
      } else {
        if (prev_mod_time == statbuf.st_mtim) no_change_count++;
        double diff = timespecDiffSec(prev_mod_time, statbuf.st_mtim);
        time_diff_stats.add(diff);
        prev_mod_time = statbuf.st_mtim;
        // printf("%ld.%09ld %.9f\n", prev_mod_time.tv_sec, prev_mod_time.tv_nsec, diff);
               
      }
    }
  }
      
  if (rank == 0) {
    printf("last-modified timestamp changes between writes\n");
    printf("min=%.3fms avg=%.3fms max=%.3fms\n",
           time_diff_stats.getMinimum() * 1e3,
           time_diff_stats.getAverage() * 1e3,
           time_diff_stats.getMaximum() * 1e3);
    printf("%d of %d tests did not change timestamp\n",
           no_change_count, n_tests);
  }

  MPI_Barrier(comm);
  if (rank == 0) remove(FILENAME);
  
  MPI_Finalize();
  return 0;
}
