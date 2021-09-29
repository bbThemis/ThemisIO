#include <cstdlib>
#include <cstdarg>
#include <climits>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <errno.h>
#include <unordered_set>
#include "page_cache.h"


PageCache::Implementation PageCache::system_implementation;

/*
  O_APPEND

  All writes occur at the end of the file, after which the file
  position is at the end of that write. lseek() can be used to move
  the file position, but any write, from any file descriptor on that
  file, will move it back to the end.

  lseek - file position can be past EOF. An lseek past EOF will success,
  as will a write, but reads will just return 0.
 */


PageCache::PageCache
(ConsistencyLevel consistency_,
 int page_size_,
 size_t max_memory_usage,
 Implementation &impl_)
  : consistency(consistency_),
    impl(impl_),
    page_size(page_size_),
    idle_list(entries, LIST_IDLE),
    active_list(entries, LIST_ACTIVE),
    inactive_list(entries, LIST_INACTIVE)
{
  // assume 64 bit sizes
  assert(sizeof(off_t) == sizeof(long));
  assert(sizeof(size_t) == sizeof(long));
  assert(sizeof(ssize_t) == sizeof(long));
  
  // check power of 2
  if ((page_size & (page_size-1)) != 0) {
    assert(!"PageCache page_size must be a power of 2");
  }

  page_bits = log2(page_size);

  int bytes_per_entry = page_size + sizeof(PageCache::Entry);
  int entry_count = max_memory_usage / bytes_per_entry;
  if (entry_count < 1) entry_count = 1;

  entries.resize(entry_count);

  size_t content_size = entries.size() * page_size;
  all_content = (char*) malloc(content_size);
  if (!all_content) {
    fprintf(stderr, "Failed to allocate %lu bytes for page cache data\n",
            (long unsigned)content_size);
    assert(!"error allocating page cache data");
  }
  
  // initialize entry list
  for (int entry_id=0; entry_id < (int)entries.size(); entry_id++) {
    idle_list.pushBack(entry_id);
    assert(!isEntryDirty(entry_id));
    assert(entries[entry_id].isIdle());
  }
  assert(idle_list.size() == entries.size());
  assert(inactive_list.isEmpty());
  assert(active_list.isEmpty());
  assert(fsck());
}


PageCache::~PageCache() {
  // make sure all dirty pages are flushed
  assert(fsck());
  exit();
  free(all_content);
}


/*
  VISIBLE_AFTER_WRITE: there should be no dirty pages in the
    cache. Clean pages are fine, because we'll still check
    last-modified when we re-use them. So, do nothing.
  VISIBLE_AFTER_CLOSE: flush all pages associated with this file, to pick
    up changes made by another process. There may be clean or dirty pages
    in the cache because the file may be open under a different file
    descriptor.
  VISIBLE_AFTER_EXIT: flush nothing
*/  
int PageCache::open(const char *pathname, int flags, ...) {
  Lock lock(mtx);
  int two_args = 1;
  mode_t mode = 0;
  int fd = -1;
  OpenFile *open_file = nullptr;
  FileDescriptor *filedes = nullptr;
  
  if (flags & O_CREAT)	{
    va_list arg;
    va_start(arg, flags);
    mode = va_arg(arg, mode_t);
    va_end(arg);
    two_args=0;
  }

  // bbThemis doesn't assign unique inodes to files, so we'll use
  // the canonical filename to associate duplicates.
  std::string canonical_path = canonicalPath(pathname);

  // see if this file has already been opened
  auto open_it = open_files_by_name.find(canonical_path);
  if (open_it == open_files_by_name.end()) {
    open_file = new OpenFile(canonical_path);
  } else {
    open_file = open_it->second;
    open_file->addref();
  }

  if (consistency == VISIBLE_AFTER_CLOSE)
    flushFilePages(open_file, false);

  // Extract just the access permissions from flags
  // On most systems, (O_WRONLY & O_RDWR) == 0, but it's legal to
  // have (O_WRONLY | O_RDONLY) == O_RDWR. Handle both cases.
  int flags_access = flags & (O_RDONLY | O_WRONLY | O_RDWR);
  assert(flags_access == O_RDONLY ||
         flags_access == O_WRONLY ||
         flags_access == O_RDWR);

  bool deferred_open = false;
  if (deferOpen(canonical_path, flags)) {
    deferred_open = true;
    fd = reserveFileDescriptor(canonical_path, flags, mode);
  } else {

    /* open mode trickiness. See comment in page_cache.hh on OpenFile.fd.
       "current" is open_file->open_mode.

       file access mode (O_RDONLY, O_WRONLY, or O_RDWR)

       In what mode should we attempt to open the file?

                flags
                  O_RDONLY  | O_WRONLY  | O_RDWR
       current  +-----------+-----------+------
       -1       | O_RDONLY  | O_RDWR    | O_RDWR
       O_RDONLY | O_RDONLY  | O_RDWR    | O_RDWR
       O_WRONLY | O_RDWR    | O_WRONLY  | O_RDWR
       O_RDWR   | O_RDONLY  | O_WRONLY  | O_RDWR

       If the file is currently only open for write access, then we
       must have already tried to open it O_RDWR and failed.

       If the file is currently open O_RDWR then we have all the
       access we need, and this call can do what it wants.

       So we'll only try to expand our permissions if it currently
       open read-only and we're now opening it write-only, or
       vice-versa.
    */

    if ( (flags_access == O_WRONLY &&
          ((open_file->access == -1) ||
           (open_file->access == O_RDONLY)))
         ||
         (flags_access == O_RDONLY && 
          open_file->access == O_WRONLY) ) {

      int flags_other = flags & ~(O_RDONLY | O_WRONLY | O_RDWR);
      int flags_attempted = O_RDWR | flags_other;
      
      if (two_args) {
        fd = impl.open(pathname, flags_attempted);
      } else {
        fd = impl.open(pathname, flags_attempted, mode);
      }

      if (fd != -1) {
        // great, we were able to upgrade the permissions
        // save the new file descriptor
        open_file->access = O_RDWR;
        open_file->fd = fd;
      }
    }

    // No need to open the file if we already succeeded in opening it
    // with upgraded permissions.
    if (fd == -1) {
      if (two_args) {
        fd = impl.open(pathname, flags);
      } else {
        fd = impl.open(pathname, flags, mode);
      }
    }

    // return immediately on error
    if (fd == -1) {
      if (open_file->rmref() == 0) {
        // the only place this file could be open is right here from the call
        // that created it above, so we know it isn't in open_files_by_name yet.
        delete open_file;
      }
      return fd;
    }

    // if this is the first call to open() on this file then save the fd
    if (open_file->fd == -1) {
      open_file->fd = fd;
      open_file->access = flags_access;
    }
    
  }

  // set f->length if the file is opened for append
  if ((flags & O_APPEND) && !deferred_open)
    open_file->needLength(impl);    

  open_files_by_name[canonical_path] = open_file;
  
  auto fd_iter = file_fds.find(fd);
  if (fd_iter != file_fds.end()) {
    fprintf(stderr, "Error opening %s: got file descriptor %d, which is in use\n",
            pathname, fd);
  } else {
    filedes = new FileDescriptor(open_file, fd, flags_access);
    if (deferred_open) {
      // save the canonical path in case the current directory changes before we open the file
      filedes->setDeferred(-1, canonical_path, mode);
    }
    file_fds[fd] = filedes;
  }

  return fd;
}


int PageCache::creat(const char *pathname, mode_t mode) {
  return this->open(pathname, O_CREAT|O_WRONLY|O_TRUNC, mode);
}


PageCache::FileDescriptor* PageCache::getFileDescriptor(int fd) {
  auto file_fds_it = file_fds.find(fd);
  if (file_fds_it == file_fds.end()) {
    return nullptr;
  } else {
    return file_fds_it->second;
  }
}
  


ssize_t PageCache::read(int fd, void *buf, size_t count) {
  Lock lock(mtx);
  FileDescriptor *f = getFileDescriptor(fd);
  if (!f) {errno = EBADF; return -1;};
  return pread(f, buf, count, f->position);
}


ssize_t PageCache::pread(int fd, void *buf, size_t count, off_t offset) {
  Lock lock(mtx);
  FileDescriptor *f = getFileDescriptor(fd);
  if (!f) {errno = EBADF; return -1;};
  return pread(f, buf, count, offset);
}


ssize_t PageCache::pread(FileDescriptor *filedes, void *buf, size_t count, off_t offset) {
  if (count == 0) return 0;

  if (filedes->access == O_WRONLY) {
    errno = EBADF;
    return -1;
  }
  
  OpenFile *open_file = filedes->open_file;
  
  // with VISIBLE_AFTER_WRITE, use fstat() before every read() to check
  // if another process has modified the file. If so, flush all cached pages,
  // update length, and update last_mod_nanos.
  if (consistency == VISIBLE_AFTER_WRITE) {
    if (filedes->checkLastModified(impl)) {
      flushFilePages(open_file, false);
    }
  }

  // if we're reading from the file, we need to know where EOF is
  open_file->needLength(impl);

  // already past the end of the file
  if (offset >= open_file->length) return 0;
  
  // don't read past the end of the file
  count = std::min(count, (size_t)(open_file->length - offset));
  
  /* loop through each page of the read */
  long page_id = fileOffsetToPageId(offset);
  int page_offset = fileOffsetToPageOffset(offset);
  char *buf_pos = (char*) buf;
  char *buf_end = buf_pos + count;

  while (buf_pos < buf_end) {
    
    // fetch the next page, either by reading it from the file or by finding
    // it in the cache
    int entry_id = getPageEntry(open_file, page_id, true);

    // error reading file
    if (entry_id < 0)
      break;

    char *page_content = getEntryContent(entry_id);

    int copy_len = std::min((long)page_size - page_offset, buf_end - buf_pos);
    memcpy(buf_pos, page_content + page_offset, copy_len);

    buf_pos += copy_len;
    page_offset = 0;
    page_id++;
  }

  // return the number of bytes written to buf
  long bytes_read = buf_pos - (char*)buf;
  filedes->position += bytes_read;
  return bytes_read;
}


/* With VISIBLE_AFTER_WRITE, do all writes directly, caching nothing.
   TODO: keep written data in clean cache pages, so reads of the data
   this processes write can be served by the cache. */
ssize_t PageCache::write(int fd, const void *buf, size_t count) {
  Lock lock(mtx);
  FileDescriptor *f = getFileDescriptor(fd);
  if (!f) {errno = EBADF; return -1;};
  return pwrite(f, buf, count, f->position);
}

  
ssize_t PageCache::pwrite(int fd, const void *buf, size_t count, off_t offset) {
  Lock lock(mtx);
  FileDescriptor *f = getFileDescriptor(fd);
  if (!f) {errno = EBADF; return -1;};
  return pwrite(f, buf, count, offset);
}


ssize_t PageCache::pwrite(FileDescriptor *filedes, const void *buf, size_t count, off_t offset) {
  if (count == 0) return 0;

  if (filedes->access == O_RDONLY) {
    errno = EBADF;
    return -1;
  }

  OpenFile *open_file = filedes->open_file;

  if (consistency == VISIBLE_AFTER_WRITE) {
    ssize_t result = impl.pwrite(filedes->fd, buf, count, offset);
    if (result != -1) {
      filedes->position = offset + result;
      // Update open_file->length?  No, because we should assume that
      // any other process may change it, so we shouldn't cache a value.
    }
    return result;
  }

  /* loop through each page of the read */
  long page_id = fileOffsetToPageId(offset);
  int page_offset = fileOffsetToPageOffset(offset);
  const char *buf_pos = (const char*) buf;
  const char *buf_end = buf_pos + count;

  while (buf_pos < buf_end) {
    int copy_len = std::min((long)page_size - page_offset, buf_end - buf_pos);

    int entry_id = getCachedPageEntry(open_file, page_id);

    // if the page isn't cached, we're write-only, and we're writing a partial
    // page, then write it directly.
    if (entry_id == -1) {
      if (copy_len < page_size
          && filedes->access == O_WRONLY) {
        long file_offset = offset + (buf_pos - (const char*)buf);
        ssize_t result = impl.pwrite(filedes->fd, buf_pos, copy_len, file_offset);
        if (result != copy_len) {
          fprintf(stderr, "PageCache::pwrite error ::pwrite(%s, %ld, %ld) "
                  "returned %ld\n", open_file->canonical_path.c_str(),
                  (long)count, file_offset, (long)result);
          if (result != -1)
            buf_pos += result;
          break;
        }
        buf_pos += copy_len;
        page_offset = 0;
        page_id++;
        continue;
      } else {

        bool fill_page = false;

        // fill the page if we start past the start of the page
        // or end before the end of the page, and it's not the end of file.
        if (page_offset > 0) {
          fill_page = true;
        } else if (page_offset + copy_len < page_size) {
          long write_pos = offset + (buf_pos - (const char *)buf);
          if (write_pos + copy_len < open_file->length) {
            fill_page = true;
          }
        }
        
        entry_id = getPageEntry(open_file, page_id, fill_page, true);
      }
    }
      
    // int entry_id = getPageEntry(open_file, page_id, fill_page);

    // error reading file
    if (entry_id < 0)
      break;

    char *page_content = getEntryContent(entry_id);

    memcpy(page_content + page_offset, buf_pos, copy_len);
    setEntryDirty(entry_id);
      
    buf_pos += copy_len;
    page_offset = 0;
    page_id++;
  }

  // return the number of bytes written to buf
  long bytes_written = buf_pos - (const char*)buf;
  filedes->position += bytes_written;
  if (filedes->position > open_file->length)
    open_file->length = filedes->position;

  return bytes_written;
}
  
  

/* Seek to negative offset: EINVAL
   Seek to offset greater than file length: ok 
   TODO: check for overflow of off_t
*/
off_t PageCache::lseek(int fd, off_t offset, int whence) {
  off_t new_position;
  Lock lock(mtx);

  FileDescriptor *filedes = getFileDescriptor(fd);
  if (!filedes) {
    errno = EBADF;
    return -1;
  }

  if (whence == SEEK_SET) {
    new_position = offset;
  }

  else if (whence == SEEK_CUR) {
    new_position = filedes->position + offset;
  }

  else if (whence == SEEK_END) {
    filedes->open_file->needLength(impl);
    new_position = filedes->open_file->length + offset;
  }

  else {
    // trigger an EINVAL return
    new_position = -1;
  }
    
  
  if (new_position < 0) {
    errno = EINVAL;
    return -1;
  } else {
    filedes->position = new_position;
    return filedes->position;
  }
}


/*
  VISIBLE_AFTER_WRITE: no dirty pages are cached, so there's nothing to
    do when the file is closed. It's to keep clean pages in the cache, because
    we'll still check last-modified when we re-use them.
  VISIBLE_AFTER_CLOSE: flush only dirty pages, so all my writes are visible.
    Remove clean pages only if there are no other FileDescriptors using them.
  VISIBLE_AFTER_EXIT: flush nothing
*/
int PageCache::close(int fd) {
  Lock lock(mtx);
  
  FileDescriptor *filedes = getFileDescriptor(fd);
  if (!filedes) {
    errno = EBADF;
    return -1;
  }

  OpenFile *open_file = filedes->open_file;

  int other_refs = open_file->rmref();

  if (consistency == VISIBLE_AFTER_WRITE) {
    // do nothing
  } else if (consistency == VISIBLE_AFTER_CLOSE) {
    flushFilePages(open_file, other_refs > 0);
  }

  int result = 0;

  // don't actually close my file descriptor if open_file is using it
  if (fd != open_file->fd) {
    result = impl.close(fd);
  }

  file_fds.erase(fd);
  delete filedes;

  if (!other_refs && consistency <= VISIBLE_AFTER_CLOSE) {
    impl.close(open_file->fd);
    open_files_by_name.erase(open_file->canonical_path);
    delete open_file;
  }
  
  return result;  
}


void PageCache::exit() {
  flushAll();
}


/* Look up a cache entry for a given page_id. Returns -1 if
   this page it not currently cached. */
int PageCache::getCachedPageEntry(OpenFile *f, long page_id) {
  PageKey key(f, page_id);

  // check if the page is in the cache already
  auto table_it = entry_table.find(key);
  if (table_it == entry_table.end()) {
    return -1;
  } else {
    int entry_id = table_it->second;
    
    // move the entry to the active list if it isn't there already
    setPageActive(entry_id);
    return entry_id;
  }
}


/* Look up a cache entry for a given page_id.
   If it's already cached, return the entry_id.
   If not, allocate an entry and read the data. 

   If (fill) is set, then read the page content. from the file.  If
   the caller is going to overwrite the whole page, they can set
   (fill) to false. 

   If known_new is true, then caller has already confirmed that the
   page does not exist, so don't bother looking. */
int PageCache::getPageEntry(OpenFile *f, long page_id, bool fill,
                            bool known_new) {

  PageKey key(f, page_id);

  if (!known_new) {
    // check if the page is in the cache already
    auto table_it = entry_table.find(key);
    if (table_it != entry_table.end()) {

      int entry_id = table_it->second;
    
      // move the entry to the active list if it isn't there already
      setPageActive(entry_id);
      return entry_id;
    }
  }

  // get a new entry
  int entry_id = newEntry(f, page_id);
  entry_table[key] = entry_id;

  if (fill) {
    // we're actually reading the file, so we need to know where EOF is at
    f->needLength(impl);

    long offset = page_id * page_size;
    char *content = getEntryContent(entry_id);

    // reading past EOF? believe it or not, also jail.
    if (offset >= f->length) {
      memset(content, 0, page_size);
    } else {

      // make sure the file is open for reading
      assert(f->access != -1);
      assert(f->fd != -1);
      if (f->access == O_WRONLY) {
        fprintf(stderr, "PageCache::getPageEntry error: "
                "cannot read page at offset %ld for write-only file %s\n",
                page_id * page_size, f->canonical_path.c_str());
      } else {
        assert(f->length >= 0 && offset < f->length);
        int read_len = std::min((long)page_size, f->length - offset);
        int bytes_read = impl.pread(f->fd, content, read_len, offset);
                                    
        if (bytes_read == -1) {
          fprintf(stderr, "PageCache::getPageEntry error reading at offset "
                  "%ld of file %s: %s\n",
                  page_id * page_size, f->canonical_path.c_str(),
                  strerror(errno));
          memset(content, 0, page_size);
        } else if (bytes_read < read_len) {

          fprintf(stderr, "PageCache::getPageEntry error short read at offset "
                  "%ld of file %s: %d of %d bytes\n",
                  page_id * page_size, f->canonical_path.c_str(),
                  bytes_read, page_size);
          
          memset(content + bytes_read, 0, page_size - bytes_read);
        }
      }
    }
  }
    
  return entry_id;
}


int PageCache::newEntry(OpenFile *f, long page_id) {
  int entry_id;

  // check that every entry is in exactly one list
  assert(idle_list.size() + inactive_list.size() + active_list.size() == entries.size());
  
  // do a little balancing
  balanceEntryLists();

  // try to get an unused entry
  if (!idle_list.isEmpty()) {
    entry_id = idle_list.popFront();
  } else {

    // entries on the inactive or active list may be dirty

    // first try to scavenge one from the inactive list.
    // it's the entries that haven't been used more than once or haven't
    // been used recently.
    if (!inactive_list.isEmpty()) {
      entry_id = inactive_list.popFront();
    } else {
      entry_id = active_list.popFront();
    }

    // flush this page, send it to idle list
    removeEntry(entry_id);
  }

  Entry &e = entries[entry_id];
  e.init(f, page_id);

  // new entries start on the inactive list
  inactive_list.pushBack(entry_id);
  
  // check that every entry is still in exactly one list
  assert(idle_list.size() + inactive_list.size() + active_list.size() == entries.size());

  return entry_id;
}


void PageCache::balanceEntryLists() {
  // keep the active list and inactive list at about the same length

  // if imbalanced, move one entry from active to inactive
  if (active_list.size() > 2*(1+inactive_list.size())) {
    // do some list assertions, just to check
    active_list.checkFront();
    int entry_id = active_list.popFront();
    inactive_list.pushBack(entry_id);
    inactive_list.checkBack();
  }
}

int PageCache::writeDirtyEntry(int entry_id) {
  Entry &e = entries[entry_id];
  int err = 0;

  assert(e.file->access != -1);
  if (e.file->access == O_RDONLY) {
    fprintf(stderr, "PageCache::writeDirtyEntry error cannot write dirty "
            "page at offset %ld to read-only file %s\n",
            e.page_id * page_size, e.file->canonical_path.c_str());
    return 1;
  }

  int bytes_written = impl.pwrite(e.file->fd, getEntryContent(entry_id),
                                  page_size, e.page_id * page_size);
                                  
  if (bytes_written == -1) {
    fprintf(stderr, "PageCache::writeDirtyEntry error failed to write dirty "
            "page at offset %ld to read-only file %s: %s\n",
            e.page_id * page_size, e.file->canonical_path.c_str(),
            strerror(errno));
    err = 2;
  } else if (bytes_written < page_size) {
    fprintf(stderr, "PageCache::writeDirtyEntry error short write at offset "
            "%ld of file %s: %d of %d bytes\n",
            e.page_id * page_size, e.file->canonical_path.c_str(),
            bytes_written, page_size);
    err = 3;
  }

  // if there were errors, we can't fix them. Might as well mark the page
  // clean, or other code that depends on this might fail.
  
  e.setClean();
  return err;
}


void PageCache::removeEntry(int entry_id) {
  Entry &e(entries[entry_id]);
  if (e.isIdle()) return;

  if (e.isDirty())
    writeDirtyEntry(entry_id);
  
  PageKey key(e.file, e.page_id);
  assert(entry_table[key] == entry_id);
  entry_table.erase(key);
  
  e.file = nullptr;
  e.page_id = -1;

  moveEntryToList(entry_id, LIST_IDLE);
  assert(!entries[entry_id].isDirty() && entries[entry_id].isIdle());
}


void PageCache::flushFilePages(OpenFile *f, bool dirty_only) {
  for (size_t i=0; i < entries.size(); i++) {
    Entry &e = entries[i];
    if (e.file == f) {
      if (dirty_only) {
        if (e.isDirty()) {
          writeDirtyEntry(i);
        }
      } else {
        removeEntry(i);
      }        
    }
  }
}


int PageCache::flushWriteCache() {
  int any_err = 0;
  for (size_t i=0; i < entries.size(); i++) {
    if (entries[i].isDirty()) {
      if (writeDirtyEntry(i)) {
        any_err = 1;
      }
    }
  }
  return any_err;
}


int PageCache::flushAll() {
  int any_err = 0;
  
  if (flushWriteCache())
    any_err = 1;

  auto f_it = file_fds.begin();
  while (f_it != file_fds.end()) {

    int fd = f_it->first;
    FileDescriptor *filedes = f_it->second;
    OpenFile *open_file = filedes->open_file;

    // TODO handle deferred opens
    assert(!filedes->open_is_deferred);
    
    if (fd != open_file->fd) {
      impl.close(fd);
    }
    delete filedes;

    int refcount = open_file->rmref();
    if (refcount == 0) {
      impl.close(open_file->fd);
      open_files_by_name.erase(open_file->canonical_path);
      delete open_file;
    }
    f_it = file_fds.erase(f_it);
  }

  auto o_it = open_files_by_name.begin();
  while (o_it != open_files_by_name.end()) {
    const std::string &name = o_it->first;
    OpenFile *f = o_it->second;
    printf("in PageCache::exit() lingering open_file \"%s\"\n",
           name.c_str());
    impl.close(f->fd);
    delete f;
    o_it = open_files_by_name.erase(o_it);
    any_err = 1;
  }
  
  return any_err;
}


// Check the data structure for errors. Return true if correct.
bool PageCache::fsck() const {

  assert((1 << page_bits) == page_size);

  // check open_files_by_name
  for (auto it = open_files_by_name.begin();
       it != open_files_by_name.end();
       it++) {
    assert(it->first == it->second->canonical_path);
  }

  // check file_fds
  for (auto it = file_fds.begin(); it != file_fds.end(); it++) {
    assert(it->first == it->second->fd);
  }

  // check entry_table
  for (auto it = entry_table.begin(); it != entry_table.end(); it++) {
    const PageKey &key = it->first;
    int entry_id = it->second;
    assert(entry_id >= 0 && entry_id < entries.size());

    const Entry &e = entries[entry_id];
    assert(key.f == e.file);
    assert(key.page_id == e.page_id);
    assert(e.listNo() == LIST_INACTIVE ||
           e.listNo() == LIST_ACTIVE);

    const std::string &path = key.f->canonical_path;
    auto of_it = open_files_by_name.find(path);
    assert(of_it != open_files_by_name.end());
    assert(of_it->second == key.f);
  }

  // check lists
  for (int list_id=0; list_id < 3; list_id++) {
    const List &list = getList(list_id);
    assert(list.fsck(list_id, entries));
  }

  assert(idle_list.size() + inactive_list.size() + active_list.size()
         == entries.size());

  // check entries
  int list_counters[3] = {0};
  for (int entry_id=0; entry_id < entries.size(); entry_id++) {
    const Entry &e = entries[entry_id];
    int list_no = e.listNo();
    assert(list_no >= 0 && list_no < 3);
    list_counters[list_no]++;
    if (e.isIdle()) {
      assert(e.file == nullptr);
      assert(e.page_id == -1);
    } else {
      assert(e.file != nullptr);
      assert(e.page_id != -1);
    }
  }
  assert(list_counters[0] == idle_list.size());
  assert(list_counters[1] == inactive_list.size());
  assert(list_counters[2] == active_list.size());
  
  return true;
}


bool PageCache::List::fsck(int list_id, const std::vector<Entry> &entries) const {
  assert(listNo() == list_id);
  if (isEmpty()) {
    assert(size() == 0 && head == -1 && tail == -1);
    return true;
  }
  
  assert(size() >= 0 && front() != -1 && back() != -1);

  int count = 0, entry_id = front();
  while (entry_id != -1) {
    count++;
    assert(entries[entry_id].listNo() == list_id);
    int next = entries[entry_id].list_next;
    if (next != -1) {
      assert(entries[next].list_prev == entry_id);
    } else {
      assert(back() == entry_id);
    }
    entry_id = next;
  }
  assert(count == size());

  return true;
}
  
  


long PageCache::OpenFile::needLength(Implementation &impl) {
  if (length != -1) return length;
  
  struct stat statbuf;
  if (impl.fstat(fd, &statbuf)) {
    fprintf(stderr, "Error calling stat() in PageCache::OpenFile::needLength "
            " on %s: %s\n", canonical_path.c_str(), strerror(errno));
    length = 0;
  } else {
    length = statbuf.st_size;
  }
  return length;
}


#if 0
long PageCache::FileDescriptor::statLength() {
  struct stat s;
  if (impl.fstat(fd, &s)) {
    fprintf(stderr, "Error calling stat in PageCache::FileDescriptor::statLength(): %s\n",
            strerror(errno));
    return 0;
  } else {
    return s.st_size;
  }
}


long PageCache::FileDescriptor::statLastModifiedNanos() {
  struct stat s;
  if (impl.fstat(fd, &s)) {
    fprintf(stderr, "Error calling stat in PageCache::FileDescriptor::statLength(): %s\n",
            strerror(errno));
    return 0;
  } else {
    struct timespec &t = s.st_mtim;
    return t.tv_sec * (long)1000000000 + t.tv_nsec;
  }
}
#endif


/* Use fstat() to check if the file has been modified by another process.
   If so, update open_file->{last_mod_nanos,length}. */
bool PageCache::FileDescriptor::checkLastModified(Implementation &impl) {
  struct stat statbuf;
  if (impl.fstat(fd, &statbuf)) {
    fprintf(stderr, "Error calling fstat() in PageCache::FileDescriptor::checkLastModified(): %s\n", strerror(errno));
    return false;
  }
  
  struct timespec &t = statbuf.st_mtim;
  long new_last_mod = t.tv_sec * (long)1000000000 + t.tv_nsec;

  // no change
  if (new_last_mod == open_file->last_mod_nanos)
    return false;

  // yes change
  open_file->last_mod_nanos = new_last_mod;
  open_file->length = statbuf.st_size;

  return true;
}
  
  





int PageCache::Implementation::open(const char *pathname, int flags, ...) {
  if (flags & O_CREAT) {
    va_list arg;
    va_start(arg, flags);
    mode_t mode = va_arg(arg, mode_t);
    va_end(arg);
    return ::open(pathname, flags, mode);
  } else {
    return ::open(pathname, flags);
  }
}


int PageCache::Implementation::openat(int dirfd, const char *pathname, int flags, ...) {
  if (flags & O_CREAT) {
    va_list arg;
    va_start(arg, flags);
    mode_t mode = va_arg(arg, mode_t);
    va_end(arg);
    return ::openat(dirfd, pathname, flags, mode);
  } else {
    return ::openat(dirfd, pathname, flags);
  }
}


ssize_t PageCache::Implementation::write(int fd, const void *buf, size_t count) {
  return ::write(fd, buf, count);
}


/* Notes on semantics:
    - When the file is opened for O_APPEND, the offset argument of
      pwrite() is ignored. 
    - When the file is opened for O_RDONLY, calls to write return -1
      and set errno to EBADF. Ditto O_WRONLY and read().
*/
ssize_t PageCache::Implementation::pwrite(int fd, const void *buf, size_t count, off_t offset) {
  return ::pwrite(fd, buf, count, offset);
}


ssize_t PageCache::Implementation::read(int fd, void *buf, size_t count) {
  return ::read(fd, buf, count);
}


ssize_t PageCache::Implementation::pread(int fd, void *buf, size_t count, off_t offset) {
  return ::pread(fd, buf, count, offset);
}


int PageCache::Implementation::fstat(int fd, struct stat *statbuf) {
  return ::fstat(fd, statbuf);
}


int PageCache::Implementation::close(int fd) {
  return ::close(fd);
}


std::string PageCache::currentDir() {
  // TODO this could be fancier, where first we try using a fix-size
  // buffer, then if that fails, retry with increasingly larger
  // buffers
  char cwd[PATH_MAX];
  if (!getcwd(cwd, PATH_MAX)) {
    fprintf(stderr, "In PageCache::currentDir(), error calling getcwd(buf, %d): %s\n", PATH_MAX, strerror(errno));
    strcpy(cwd, ".");
  }
  return std::string(cwd);
}


std::string PageCache::canonicalPath(const char *path) {

  if (!*path) return currentDir();

  std::string result;
  const char *p = path;
  
  if (*p != '/') {
    // relative path
    result = currentDir();
  }

  // result will be in the form (/xxx)*

  while (*p) {
    // printf("    result=\"%s\"  p=\"%s\"\n", result.c_str(), p);
    
    // skip repeated slashes
    while (*p == '/') p++;
    if (!*p) break;

    // find end of this component, whether it's a slash or the end of the string
    const char *end = p+1;
    while (*end && *end != '/') end++;

    if (*p == '.') {
      
      // handle "./"
      if (end-p == 1) {
        p++;
        continue;
      }

      // handle "../"
      else if (p[1] == '.' && end-p==2) {
        p += 2;

        // cannot go higher than root directory
        if (result.length() == 0) continue;

        // if it isn't empty it must start with a slash, so find_last_of will not fail
        assert(result[0] == '/');
        size_t last_slash = result.find_last_of('/');

        result.resize(last_slash);
        continue;
      }
    }

    // append everything up to but not including the next slash
    result += '/';
    result.append(p, end);
    p = end;
  }

  if (result.length() == 0) result += '/';

  return result;
}
