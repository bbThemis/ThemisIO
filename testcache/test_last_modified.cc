#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <mpi.h>
#include "simple_stats.hh"
#include <sys/inotify.h>
#include <map>
#include <string>
#include <poll.h>
#include <limits.h>

int np, rank;
double t0;
MPI_Comm comm;
const char *FILENAME = "test_last_modified.out";


/* Use inotify to monitor files for modification. */
class FileModifiedInotify {
public:
  FileModifiedInotify();
  ~FileModifiedInotify();

  // start watching a new file. On error, set errno and return -1.
  // If the call to inotify_init() failed, this will set errno to EIO.
  int watchFile(const char *filename);

  // stop watching a file
  void unwatchFile(const char *filename);

  // returns true if the given file was modified since the
  // last call to hasChanged()
  bool hasChanged(const char *filename);

private:
  int inotify_fd;

  /*
  struct WatchedFile {
    int watch_id;
    bool has_changed;
  };
  */

  // name -> watch_id
  std::map<std::string, int> name_to_watch_id;

  // watch_id -> has_changed flag
  std::map<int, int> has_changed_map;
};


FileModifiedInotify::FileModifiedInotify() {
  inotify_fd = inotify_init();
  if (inotify_fd < 0) {
    printf("inotify_init failed: %s\n", strerror(errno));
  }
}


FileModifiedInotify::~FileModifiedInotify() {
  if (inotify_fd < 0) return;

  // unwatch everything
  for (auto it = has_changed_map.begin(); it != has_changed_map.end(); it++) {
    int watch_id = it->first;
    inotify_rm_watch(inotify_fd, watch_id);
  }
  
  close(inotify_fd);
}


int FileModifiedInotify::watchFile(const char *filename) {
  if (inotify_fd < 0) {
    errno = EIO;
    return -1;
  }

  auto it = name_to_watch_id.find(filename);
  if (it != name_to_watch_id.end()) {
    // already watching this file
    return 0;
  }
  
  int watch_id = inotify_add_watch(inotify_fd, filename, IN_MODIFY);
  if (watch_id == -1) {
    fprintf(stderr, "inotify_add_watch failed: %s\n", strerror(errno));
    return -1;
  }

  name_to_watch_id[filename] = watch_id;
  has_changed_map[watch_id] = 0;
  printf("Adding watch for %s: %d\n", filename, watch_id);
  
  return 0;
}


void FileModifiedInotify::unwatchFile(const char *filename) {
  if (inotify_fd < 0) return;

  auto it = name_to_watch_id.find(filename);

  // do nothing if the file isn't being watched
  if (it == name_to_watch_id.end())
    return;

  int watch_id = it->second;
  if (inotify_rm_watch(inotify_fd, watch_id)) {
    fprintf(stderr, "inotify_rm_watch error: %s\n", strerror(errno));
    return;
  }

  name_to_watch_id.erase(it);
  has_changed_map.erase(watch_id);
}


bool FileModifiedInotify::hasChanged(const char *filename) {
  if (inotify_fd < 0) return false;

  auto it = name_to_watch_id.find(filename);

  // return false if the file isn't being watched
  if (it == name_to_watch_id.end())
    return false;

  int watch_id = it->second;
  assert(has_changed_map.find(watch_id) != has_changed_map.end());

  // read any pending modification events from inotify

  struct pollfd poll_data;
  poll_data.fd = inotify_fd;
  poll_data.events = POLLIN;  // need POLLRDNORM, POLLRDBAND, or POLLPRI?
  poll_data.revents = 0;

  // TODO: use larger buffer when monitoring many files
  char buf[sizeof(struct inotify_event) + NAME_MAX + 1 + 200];
  int event_count = 0;

  while (poll(&poll_data, 1, 0) > 0) {
    assert(poll_data.revents & POLLIN);
    ssize_t bytes_read = read(inotify_fd, buf, sizeof buf);
    // according to poll() there is data, and the buffer should be big enough,
    // so this should not fail.
    assert(bytes_read > 0);
    
    // printf("read %d bytes from inotify_fd\n", (int)bytes_read);

    size_t buf_offset = 0;
    while (buf_offset < bytes_read) {
      struct inotify_event *event = (struct inotify_event*) (buf + buf_offset);
      event_count++;

      auto it_modified = has_changed_map.find(event->wd);
      if (it_modified == has_changed_map.end()) {
        // inotify gave us an unrecognized watch_id. This shouldn't happen,
        // but it isn't fatal.
        fprintf(stderr, "Warning: inotify returned an unrecognized modification event on watch_id %d\n", event->wd);
        continue;
      }

      if ((event->mask & IN_MODIFY) == 0) {
        fprintf(stderr, "Warning: inotify returned an unrecognized event mask %u\n", event->mask);
        // not sure what it is, but to be safe, assume it's a modify event
      }

      has_changed_map[event->wd] = 1;

      buf_offset += sizeof(struct inotify_event) + event->len;
      // printf("event->len=%u, next buf_offset=%d\n", event->len, (int)buf_offset);
    }
  }

  // if (event_count) printf("%d inotify events\n", event_count);
    
  if (has_changed_map[watch_id]) {
    has_changed_map[watch_id] = 0;
    return true;
  } else {
    return false;
  }
}


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

  FileModifiedInotify file_watcher;
  if (rank==0) {
    file_watcher.watchFile(FILENAME);
  }

  struct stat statbuf;
  struct timespec prev_mod_time;
  int dummy = rank, no_change_count = 0;
  int changes_counted = 0, unchanged_counted = 0;
  Stats time_diff_stats;
  
  for (int test_no=0; test_no <= n_tests; test_no++) {
    if (rank == 1) {
      write(fd, &dummy, sizeof(dummy));
      MPI_Send(&dummy, 1, MPI_INT, 0, 0, comm);
      MPI_Recv(&dummy, 1, MPI_INT, 0, 0, comm, MPI_STATUS_IGNORE);
    } else {
      MPI_Recv(&dummy, 1, MPI_INT, 1, 0, comm, MPI_STATUS_IGNORE);
          
      if (fstat(fd, &statbuf)) {
        printf("[%d] fstat fail\n", rank);
        assert(0);
      }
      if (test_no == 0) {
        prev_mod_time = statbuf.st_mtim;
        file_watcher.hasChanged(FILENAME);
      } else {
      
        if (file_watcher.hasChanged(FILENAME)) {
          changes_counted++;
        }

        if (prev_mod_time == statbuf.st_mtim) no_change_count++;
        double diff = timespecDiffSec(prev_mod_time, statbuf.st_mtim);
        time_diff_stats.add(diff);
        prev_mod_time = statbuf.st_mtim;
        // printf("%ld.%09ld %.9f\n", prev_mod_time.tv_sec, prev_mod_time.tv_nsec, diff);
               
      }
      
      if (file_watcher.hasChanged(FILENAME)) {
        unchanged_counted++;
      }

      MPI_Send(&dummy, 1, MPI_INT, 1, 0, comm);
      
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
    printf("changes seen: %d, non-changes seen: %d\n",
           changes_counted, unchanged_counted);
  }

  MPI_Barrier(comm);
  if (rank == 0) remove(FILENAME);
  
  MPI_Finalize();
  return 0;
}
