#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <unistd.h>
#include <climits>
#include <errno.h>
#include <vector>
#include <poll.h>
#include <sys/inotify.h>
#include <map>
#include <string>

using std::vector;

#define FILENAME "/tmp/test_inotify.out"

#define FILE1 "/tmp/test_inotify.1"
#define FILE2 "/tmp/test_inotify.2"
#define FILE3 "/tmp/test_inotify.3"
#define FILE4 "/tmp/test_inotify.4"


/* Use inotify to monitor files for modification. */
class FileModifyNotify {
public:
  FileModifyNotify();
  ~FileModifyNotify();

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


FileModifyNotify::FileModifyNotify() {
  inotify_fd = inotify_init();
  if (inotify_fd < 0) {
    printf("inotify_init failed: %s\n", strerror(errno));
  }
}


FileModifyNotify::~FileModifyNotify() {
  if (inotify_fd < 0) return;

  // unwatch everything
  for (auto it = has_changed_map.begin(); it != has_changed_map.end(); it++) {
    int watch_id = it->first;
    inotify_rm_watch(inotify_fd, watch_id);
  }
  
  close(inotify_fd);
}


int FileModifyNotify::watchFile(const char *filename) {
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


void FileModifyNotify::unwatchFile(const char *filename) {
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


bool FileModifyNotify::hasChanged(const char *filename) {
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

  if (event_count)
    printf("%d inotify events\n", event_count);
    
  if (has_changed_map[watch_id]) {
    has_changed_map[watch_id] = 0;
    return true;
  } else {
    return false;
  }
}


void checkEvents(int inotify_fd, int watch_id, vector<char> &buf,
                 const char *action) {
  struct pollfd poll_data;

  poll_data.fd = inotify_fd;
  poll_data.events = POLLIN;  // need POLLRDNORM, POLLRDBAND, or POLLPRI?
  poll_data.revents = 0;

  // POLLIN=1, POLLRDNORM=64, POLLRDBAND=128, POLLPRI=2
  
  while (true) {
    int poll_result = poll(&poll_data, 1, 0);
    if (poll_result < 0) {
      printf("%s: poll() error: %s\n", action, strerror(errno));
      break;
    } else if (poll_result == 0) {
      // printf("%s: no inotify data\n", action);
      break;
    }

    if ((poll_data.revents & POLLIN) == 0) {
      printf("%s: poll() returned an event (%d), but it isn't POLLIN\n", action, poll_data.revents);
      return;
    }

    char notify_buf[sizeof(struct inotify_event) + NAME_MAX + 1];
    ssize_t bytes_read = read(inotify_fd, notify_buf, buf.size());
    if (bytes_read <= 0) {
      printf("%s: Failed to read from inotify_fd: %s\n",
             action, strerror(errno));
      break;
    }

    struct inotify_event *e = (struct inotify_event*) notify_buf;

    if (e->wd != watch_id) {
      printf("%s: inotify activity, but not on the expected watch id (%d, xp %d)\n", action, e->wd, watch_id);
      break;
    }

    if ((e->mask & IN_MODIFY) == 0) {
      printf("%s: inotify mask=%u, which doesn't include IN_MODIFY\n",
             action, e->mask);
    }
    
    printf("%s: file written to\n", action);
  }

}
  

int main2() {
  system("touch " FILENAME);
  
  int inotify_fd = inotify_init();
  if (inotify_fd < 0) {
    printf("inotify_init failed: %s\n", strerror(errno));
    return 1;
  }

  int watch_id;
  if ((watch_id = inotify_add_watch(inotify_fd, FILENAME, IN_MODIFY)) < 0) {
    printf("inotify_add_watch failed: %s\n", strerror(errno));
    return 1;
  }

  printf("inotify_fd=%d, watch_id=%d\n", inotify_fd, watch_id);
  
  vector<char> event_buf(sizeof (struct inotify_event) + 1000);
  checkEvents(inotify_fd, watch_id, event_buf, "no-file");

  system("ls >> " FILENAME);
  system("ls >> " FILENAME);
  system("ls >> " FILENAME);
  checkEvents(inotify_fd, watch_id, event_buf, "file-written");
  checkEvents(inotify_fd, watch_id, event_buf, "nothing-just-checking");

  inotify_rm_watch(inotify_fd, watch_id);
  close(inotify_fd);
  
  remove(FILENAME);
  
  return 0;
}


int main() {
  system("touch " FILE1);
  system("touch " FILE2);
  system("touch " FILE3);
  system("touch " FILE4);

  {
    FileModifyNotify watcher;

    watcher.watchFile(FILE1);
    watcher.watchFile(FILE2);
    watcher.watchFile(FILE3);
    watcher.watchFile(FILE4);

    for (int i=0; i < 10; i++) {
      sleep(3);
      printf("%d\n", i);
      if (watcher.hasChanged(FILE1)) printf("  %s changed\n", FILE1);
      if (watcher.hasChanged(FILE2)) printf("  %s changed\n", FILE2);
      if (watcher.hasChanged(FILE3)) printf("  %s changed\n", FILE3);
      if (watcher.hasChanged(FILE4)) printf("  %s changed\n", FILE4);
    }
  }

  remove(FILE1);
  remove(FILE2);
  remove(FILE3);
  remove(FILE4);

  return 0;
}
