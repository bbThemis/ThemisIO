#ifndef __PAGE_CACHE_H__
#define __PAGE_CACHE_H__

/*
  page_cache.h

  Simple page cache for a single file with adjustable levels of consistency.

  This is designed to be an intermediate layer between application
  code and an implementation of POSIX file I/O calls. For testing, an
  application can use this API directly with direct POSIX I/O calls as
  the backend, but the intended target is in the wrapper layer of the
  bbThemis implementation. POSIX file I/O calls will be
  intercepted. Anything related to a bbThemis server will be routed to
  this API, and this API will use the bbThemis server as its backend.

  application calls write()
    -> wrapper intercept
    -> PageCache::write
    -> bbThemis server

  visible-after-write
    POSIX default. All writes are guaranteed to be visible after the
    call to write() completes. All writes are write-through, with
    clean pages cached.  Every read checks the last-modifield field,
    and if the value has changed since our last write, the read cache
    is emptied.

    This will perform poorly when processes make frequent reads or
    writes, because every call will require contacting the file
    system.

  visible-after-close
    Writes may be visible immediately, but they are not guaranteed to
    be visible unless the reading process opens the file after the
    writing process closes it. There is a partial ordering of calls to
    close() that happen before calls to open(). In other words, if one
    process closes a file at about the same time as another opens it,
    they will both agree on the order of the open and close.

    When the file is opened the cache is empty. All writes are cached
    until the file is closed, the cache is filled, or a call to sync(),
    syncfs(), fsync(), or fdatasync(). All reads are cached until the
    file is closed.
    
    This will improve the performance of small reads and writes, because
    they can complete without contacting the file system.

  visible-after-task
    Writes may be visible immediately, but they are not guaranteed to be
    visible unless the reading process starts after the writing process
    exits. 

    Also, this will support deferred calls to open(). It can be a
    burden on a filesystem when many processes all call open() at
    about the same time. If this is set, and the processes are all opening
    the file for O_WRONLY | O_CREAT | O_TRUNC, we will just buffer data
    in memory for a while before actually calling open() on the backend.

    Caches are maintained even when files are closed, and writes are not
    flushed until the process exits. 

    This will improve the performance of processes that frequently close
    and reopen the same file.


  This cache will store pages of files, use up to some set amount of
  memory. The page size and memory limit will be set in the constructor,
  but the amount of memory can be changed at any time.
  
  This will handle multiple files, so all open files will share the
  memory limit. If a file is opened multiple times by one process,
  consistency must be maintained across all open instances. If each
  open file used an independent cache, this could lead to inconsistent
  file accesses.

  As a file can be opened multiple times each with a distinct file
  descriptor, it is not sufficient to associate the file with the file
  descriptor. It is also not sufficient to use the name of the file,
  as multiple names can refer to the same file via hard links,
  symbolic links, directory references ("../../foo/bar/gonzo" and
  "../bar/gonzo"), and non-canonical file references ("foo/bar" and
  "foo//bar"). Thus a file's inode number should be used to associate
  a it with its cache entries.

  However, this is being built on top of a system that doesn't yet
  assign unique inodes to each file, so we don't have that
  option. Thus we'll use canonical filenames for now.

  TODO: identify unique files by inode rather than canonical filename.

  All data will be accessed in pages, so if the user requests one byte,
  a full page will be read.

  For efficient lookup, pages references will be stored in a hash
  table with the key (inode, page number). The linux kernel uses a
  radix tree for this, which is more cache-friendly and allows for
  finer-grained locks when multithreaded. For now, a hash table is
  easy and we probably won't have to deal with threaded
  performance. I'll try to structure the code so it can be easy to
  swap in a different container type later.

  Does this need to be thread-safe? Yes, because the client could use
  multiple threads even on a single file descriptor. To avoid overhead
  when thread support is not needed, set the PAGE_CACHE_THREAD_SAFE
  macro to 1 when thread synchronization calls are desired.

  -- Structure --

  OpenFile : a file that is currently being managed by the cache.
    Uniquely identified by the canonical pathname of the file, but
    when inode numbers are supported it will use those.
    Lookup table: open_files_by_name

  FileDescriptor : one instance of an open file. Uniquely identified
    by an integer file descriptor returned by open(). Multiple FileDescriptors
    can reference one OpenFile.
    Lookup table: file_fds

  Entry : a slot in which one page of data can be cached. The number of
    available Entry objects is set when the maximum memory usage is set.
    (entries.count() = floor( max_memory / (page_size + sizeof(Entry)) )).
    All entry objects are stored in one vector, and are identified by
    an integer index. The actual data is stored in one large char array
    of size (page_size * entries.count()), where entry[i] manages the data
    at offset (i*page_size).
    Lookup table: entry_table

  page_id : index of a page in a file. Given a file offset, the page_id
    is floor(offset / page_size).

  idle_list, inactive_list, active_list : doubly-linked lists of entries,
    Every entry is in exactly one of these three lists.
      idle: unallocated
      inactive: the page is infrequently used
      active: the page is frequently used

  Implementation - layer of virtual functions that is the backend
    implementation.  The static instance sample_implementation
    references POSIX I/O calls directly. This is where a bbThemis
    backend implementation will be interfaced.

  Ed Karrels
  edk@illinois.edu
*/

#include <cassert>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <string>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <mutex>

#ifndef PAGE_CACHE_THREAD_SAFE
#define PAGE_CACHE_THREAD_SAFE 0
#endif


class PageCache {
public:

  enum ConsistencyLevel
    {VISIBLE_AFTER_WRITE,
     VISIBLE_AFTER_CLOSE,
     VISIBLE_AFTER_EXIT} ;

  class Implementation;

  /* One instance of PageCache can cache multiple files. It is designed
     to have one instance per process.
    - page_size must be a power of 2 */
  PageCache(ConsistencyLevel consistency,
            int page_size_ = DEFAULT_PAGE_SIZE,
            size_t max_memory_usage = DEFAULT_MEM_SIZE,
            Implementation &impl = system_implementation);

  ~PageCache();


  /* If VISIBLE_AFTER_CLOSE, all pages for the file will be flushed
     when the file is opened. */
  int open(const char *pathname, int flags, ...);
  
  int creat(const char *pathname, mode_t mode);

  
  /* This is difficult because given dirfd, we can't determine what the
     canonical path name is, so we can't tell whether this file is cached.
     We could trace calls to open(..., O_DIRECTORY) opendir(), dup(), and 
     possibly more, so we know where the dirfd came from. This seems like
     a bad idea. Until we have inodes, I'll ignore the dirfd when computing
     the canonical pathname. */
  int openat(int dirfd, const char *pathname, int flags, ...);

  
  ssize_t write(int fd, const void *buf, size_t count);
  ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset);
  ssize_t read(int fd, void *buf, size_t count);
  ssize_t pread(int fd, void *buf, size_t count, off_t offset);

  off_t lseek(int fd, off_t offset, int whence);
  
  /* If VISIBLE_AFTER_CLOSE, flush all the pages for this file. */
  int close(int fd);
  
  /* If VISIBLE_AFTER_EXIT, flush all */
  void exit();
  
  int getPageSize() {return page_size;}
  
  // Writes all dirty pages, while leaving clean pages in read cache.
  // Return 0 on success or errno on error.
  int flushWriteCache();
  
  // Writes all dirty pages and removes all pages from read cache.
  // Return 0 on success or errno on error.
  int flushAll();

  // Call this when there is a little idle time. Up to bytes/page_size
  // dirty pages will be flushed;
  // Return 0 on success or errno on error.
  // TODO: not implemented yet
  int flushSomeWriteCache(long bytes);

  // Increase or decrease maximum memory usage. May cause page flushes.
  // TODO: not implemented yet
  void setMaxMemoryUsage(long bytes);

  // Check the data structure for errors. Return true if correct.
  bool fsck() const;
  
  static std::string currentDir();

  // Make path into an absolute path in the form (/name)*
  // No trailing slash, no "../" or "./" or "//".
  static std::string canonicalPath(const char *path);

  class Implementation {
  public:
    virtual int open(const char *pathname, int flags, ...);
    virtual int openat(int dirfd, const char *pathname, int flags, ...);
    virtual ssize_t write(int fd, const void *buf, size_t count);
    virtual ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset);
    virtual ssize_t read(int fd, void *buf, size_t count);
    virtual ssize_t pread(int fd, void *buf, size_t count, off_t offset);
    virtual int fstat(int fd, struct stat *statbuf);
    virtual int close(int fd);
  };
  static Implementation system_implementation;



  
private:

  /* As part of supporting deferred opens, we need to handle the case
     where a process opens a file for writing and opens it a second
     time for reading. May need to keep a table of name of files that
     have been opened via deferred opens, so another call to open can
     use the same data.

     Multiple file descriptors can refer to the same file. To keep
     cached data consistent across multiple file descriptors, all
     those file descriptors will reference one OpenFile object if
     they're all referring to the same file.
  */
  class OpenFile {
  public:
    const std::string canonical_path;

    /* Used to share the length across all file descriptors, because
       multiple file descriptors may have it opened for O_APPEND.
       When a file is opened, this will be initialized to -1, and will
       only be set if the length is needed or discovered. */
    long length;

    /* Time in nanoseconds file was last modified.
       Used when consistency <= VISIBLE_AFTER_WRITE to check if
       someone else has modified the file
       FYI A 64-bit signed long can store 292 years worth of nanoseconds. */
    long last_mod_nanos;

    /* The first time this file is opened, this object will retain the
       file descriptor returned by impl.open().

       If the inital open was read-only and a later open() succeeds
       with O_RDWR or O_WRONLY, retain that file descriptor so we can
       process later calls to flush dirty pages with calls to write().

       If a file is opened write-only and a write() updates only part
       of a page, we would need to read the rest of the page in order
       to accurately write the whole page back to storage.  So, in
       that case the write will not be cached and will be immediately
       processed. */
    int fd;

    /* access = O_RDONLY, O_WRONLY, or O_RDWR. Other flags such as O_CREAT
       have been stripped out.
       ! If this is O_WRONLY, then partial page writes cannot be cached. */
    int access;

    OpenFile(const std::string &canonical_path_) :
      canonical_path(canonical_path_), length(-1), last_mod_nanos(0),
      fd(-1), access(0), refcount(1) {}

    // if length still isn't set, use fstat() to set it now
    long needLength(Implementation &impl);

    /* Keep a reference count of each FileDescriptor referencing this,
       so it can be removed from open_files_by_name and deallocated
       when they're all closed. */
    int addref() {return ++refcount;}
    int rmref() {return --refcount;}


  private:
    // Number of file descriptors that have the file open.
    // Delete this entry when the value drops to 0.
    int refcount;
  };

  
  /* This encapsulates data associated with the file descriptor.
     Multiple FileDescriptors can point to the same OpenFile. */
  class FileDescriptor {
  public:
    OpenFile * const open_file;
    long position;
    int fd;
    int access; // O_RDONLY, O_RDWR, or O_WRONLY

    /* if open is deferred (consistency = VISIBLE_AFTER_EXIT), then
       open_is_deferred will be true, and the arguments to open() will
       be saved in these fields. */
    bool open_is_deferred;
    int dirfd;
    std::string path;  // the argument to open, not the canonical path
                       // XXX Maybe it should be the canonical path, in
                       // case the current directory is changed before
                       // the file is opened?
    int flags;
    mode_t mode;

    FileDescriptor(OpenFile *f, int fd_, int access_) : 
      open_file(f), position(0), fd(fd_), access(access_),
      open_is_deferred(false), dirfd(-1), mode(0) {}

    void setDeferred(int dirfd_, const std::string &path_, mode_t mode_) {
      open_is_deferred = true;
      dirfd = dirfd_;
      path = path_;
      mode = mode_;
    }

    // use impl.fstat() to check st_mtim to check if my OpenFile has changed.
    // Flush the cache if it has.
    bool checkLastModified(Implementation &impl);
  };

  /* Each cache entry can be on one of three lists:
      - unused (page_id=-1, content=NULL, flags=0)
      - inactive (page_id>=0, content!=NULL, flags&IS_USED != 0)
      - active (page_id>=0, content!=NULL, flags&IS_ACTIVE != 0)

     Any inactive or active page can also be dirty
       flags&IS_DIRTY != 0
  */

  enum EntryListEnum {LIST_IDLE, LIST_INACTIVE, LIST_ACTIVE};

  class Entry {
  public:
    Entry() : file(nullptr), page_id(-1), list_prev(-1), list_next(-1),
              flags(0) {}
    
    OpenFile *file;

    // file offset = page_size * page_id + page_offset
    long page_id;

    // linked list pointers for whatever list this entry
    // is currently in
    int list_prev, list_next;

    // bits 0,1: current list 00=idle, 01=inactive, 10=active
    // bit 2: dirty bit
    unsigned flags;

    void init(OpenFile *file_, long page_id_) {
      file = file_;
      page_id = page_id_;
      list_prev = list_next = -1;
      flags = 0;
    }

    bool isDirty() const {return flags & 4;}
    void setClean() {
      flags &= ~((unsigned)4);
    }
    void setDirty() {
      flags |= 4;
    }

    // note: all idle pages must be clean
    bool isIdle() const {return listNo() == LIST_IDLE;}
    void setListIdle() {setListNo(LIST_IDLE);}
    
    bool isInactive() const {return listNo() == LIST_INACTIVE;}
    void setListInactive() {setListNo(LIST_INACTIVE);}

    bool isActive() const {return listNo() == LIST_ACTIVE;}
    void setListActive() {setListNo(LIST_ACTIVE);}

    int listNo() const {return flags & 3;}
    void setListNo(int list_no) {
      assert(list_no >=0 && list_no < 3);
      unsigned mask = 3;
      mask = ~mask;
      flags = (flags & mask) | list_no;
    }

  };


  /* Doubly-linked list of entries, referenced via indices in entries vector */
  class List {
    int head, tail, size_, my_list_no;
    std::vector<Entry> &entries;
    
  public:
    List(std::vector<Entry> &entries_, int my_list_no_)
      : head(-1), tail(-1), size_(0), my_list_no(my_list_no_), entries(entries_){}

    int listNo() const {return my_list_no;}
    bool isEmpty() const {return size()==0;}
    
    int size() const {return size_;}

    int front() const {
      assert(!isEmpty());
      return head;
    }

    void checkFront() const {
      assert(size()==0 || entries[front()].listNo() == my_list_no);
    }

    // When an entry is remove from the list, don't bother invalidating
    // its list_next and list_prev pointers, because it will be immediately
    // placed on a different list and they will be set again.
    int popFront() {
      assert(!isEmpty());
      int tmp = head;
      if (head == tail) {
        head = tail = -1;
      } else {
        head = entries[head].list_next;
        // entries[head].list_next = entries[head].list_prev = -1;
      }
      size_--;
      return tmp;
    }

    void pushFront(int id) {
      assert(id >= 0 && id < entries.size());
      Entry &e = entries[id];
      if (isEmpty()) {
        e.list_next = e.list_prev = -1;
        head = tail = id;
      } else {
        entries[head].list_prev = id;
        e.list_prev = -1;
        e.list_next = head;
        head = id;
      }
      size_++;
      e.setListNo(my_list_no);
    }

    int back() const {
      assert(!isEmpty());
      return tail;
    }

    void checkBack() const {
      assert(size()==0 || entries[back()].listNo() == my_list_no);
    }
    
    int popBack() {
      assert(!isEmpty());
      int tmp = tail;
      if (head == tail) {
        head = tail = -1;
      } else {
        tail = entries[tail].list_prev;
        // entries[head].list_next = entries[head].list_prev = -1;
      }
      size_--;
      return tmp;
    }

    void pushBack(int id) {
      assert(id >= 0 && id < entries.size());
      Entry &e = entries[id];
      if (isEmpty()) {
        e.list_next = e.list_prev = -1;
        head = tail = id;
      } else {
        entries[tail].list_next = id;
        e.list_next = -1;
        e.list_prev = tail;
        tail = id;
      }
      size_++;
      e.setListNo(my_list_no);
    }

    // caller asserts id is in this list
    void removeDirect(int id) {
      assert(!isEmpty());
      Entry &e = entries[id];
      if (head == id) {
        if (tail == id) {
          head = tail = -1;
        } else {
          head = e.list_next;
        }
      } else if (tail == id) {
        tail = e.list_prev;
      }
      size_--;
    }

    bool fsck(int list_id, const std::vector<Entry> &entries) const;
  };


  // Key for identifying a page in the cache: (file handle, page index)
  struct PageKey {
    const OpenFile *f;
    const long page_id;

    PageKey(const PageKey &x) : f(x.f), page_id(x.page_id) {}
    PageKey(OpenFile *f_, long page_id_) : f(f_), page_id(page_id_) {}

    size_t hash() const {
      const size_t factor = 11400714819323198329ull;
      size_t h = (size_t)f;
      h ^= page_id + factor + (h << 12) + (h >> 4);
      return h;
    }

    bool operator == (const PageKey &x) const {
      return f == x.f && page_id == x.page_id;
    }
  };

  
  struct PageKeyHash {
    size_t operator() (const PageKey &p) const {
      return p.hash();
    }
  };


  FileDescriptor *getFileDescriptor(int fd);

  // internal implementations, after mapping an integer fd to the FileDescriptor object
  ssize_t pread(FileDescriptor *filedes, void *buf, size_t count, off_t offset);
  ssize_t pwrite(FileDescriptor *filedes, const void *buf, size_t count, off_t offset);


  void lock() {
#if PAGE_CACHE_THREAD_SAFE
    mtx.lock();
#endif
  }


  void unlock() {
#if PAGE_CACHE_THREAD_SAFE
    mtx.unlock();
#endif
  }

  class NoLockGuard {
  public:
    NoLockGuard(std::recursive_mutex &m) {}
  };

  
  bool deferOpen(const std::string &path, int flags) {
    return false;
    /*
    return consistency >= VISIBLE_AFTER_EXIT
      && open_files_by_name.find(path) == open_files_by_name.end()
      && (flags & O_CREAT)
      && (FLAGS & O_TRUNC);
    */
  }


  int reserveFileDescriptor(const std::string &path, int flags, mode_t mode) {
    return -1;
  }
  

  // simple log2
  static int log2(int x) {
    // assume x is a positive power of 2
    assert((x > 0) && ((x & (x-1)) == 0));
    int b = 1, c = 0;
    while (b < x && c < 31) {
      b = (b << 1) | 1;
      c++;
    }
    return c;
  }

  long fileOffsetToPageId(long offset) {
    return offset >> page_bits;
  }

  int fileOffsetToPageOffset(long offset) {
    return offset & (page_size-1);
  }

  // return the actual data for one page in the cache
  char *getEntryContent(int entry_id) {
    assert(entry_id >= 0 && entry_id < entries.size());
    return all_content + (size_t)entry_id * page_size;
  }

  bool isEntryDirty(int entry_id) {
    return entries[entry_id].isDirty();
  }

  void setEntryDirty(int entry_id) {
    entries[entry_id].setDirty();
  }

  const List& getList(/*EntryListEnum*/ int list_id) const {
    switch (list_id) {
    case LIST_ACTIVE: return active_list;
    case LIST_INACTIVE: return inactive_list;
    case LIST_IDLE: default: return idle_list;
    }
  }

  List& getList(/*EntryListEnum*/ int list_id) {
    return const_cast<List&>(static_cast<const PageCache &>(*this).getList(list_id));
  }

  void moveEntryToList(int entry_id, EntryListEnum list_id) {
    Entry &e = entries[entry_id];
    if (e.listNo() == list_id) return;
    List &src_list = getList(e.listNo());
    List &dest_list = getList(list_id);

    src_list.removeDirect(entry_id);
    dest_list.pushBack(entry_id);
  }


  /* Look up a cache entry for a given page_id. Returns -1 if
     this page it not currently cached. */
  int getCachedPageEntry(OpenFile *f, long page_id);
  

  /* Look up a cache entry for a given page_id.
     If it's already cached, return the entry_id.
     If not, allocate an entry and read the data. 

     If (fill) is set, then read the page content. from the file.  If
     the caller is going to overwrite the whole page, they can set
     (fill) to false.

     If known_new is true, then caller has already confirmed that the
     page does not exist, so don't bother looking. */
  int getPageEntry(OpenFile *f, long page_id, bool fill,
                   bool known_new = false);

  /* Get an unused entry, either by taking one off the idle list or by
     evicting someone else */
  int newEntry(OpenFile *f, long page_id);

  // writes an entry if its dirty, disassociates with from its page,
  // and returns it to the idle list.
  void removeEntry(int entry_id);

  void balanceEntryLists();

  // write dirty entry to disk and mark it clean
  int writeDirtyEntry(int entry_id);

  // Find every cache entry associated with this file.
  // If dirty_only == true, just write every dirty entry.
  // Otherwise write every dirty entry and remove all entries.
  void flushFilePages(OpenFile *f, bool dirty_only);

  // Write every dirty page
  void flushAllDirty();

  void markPageDirty(int entry_id) {
    entries[entry_id].setDirty();
  }

  // If this entry isn't already on the active list, move it there.
  void setPageActive(int entry_id) {
    Entry &e = entries[entry_id];
    int current_list_no = e.listNo();
    if (current_list_no == LIST_ACTIVE) return;

    // can't move a page from idle to active
    assert(current_list_no == LIST_INACTIVE);
    inactive_list.removeDirect(entry_id);
    active_list.pushBack(entry_id);
  }

  
  static const int DEFAULT_PAGE_SIZE = 1024 * 1024;
  static const int DEFAULT_MEM_SIZE = 200 * (DEFAULT_PAGE_SIZE + sizeof(Entry));

  ConsistencyLevel consistency;
  
  Implementation &impl;

  // note lock() and unlock() do nothing if (!PAGE_CACHE_THREAD_SAFE)
  std::recursive_mutex mtx;
#if PAGE_CACHE_THREAD_SAFE
  using Lock = std::lock_guard<std::recursive_mutex>;
#else
  using Lock = NoLockGuard;
#endif
  
  // Size of each page, in bytes.
  // Does not need to match system memory page size.
  int page_size;

  // page_bits = log2(page_size)
  int page_bits;

  // lookup by canonicalized filename
  std::unordered_map<std::string,OpenFile*> open_files_by_name;

  // lookup by file descriptor
  std::unordered_map<int,FileDescriptor*> file_fds;

  // lookup entries by OpenFile and page_id
  using EntryTableType = std::unordered_map<PageKey,int,PageKeyHash>;
  EntryTableType entry_table;

  std::vector<Entry> entries;

  // the data for all cache entries in one large block
  // of size entries.size() * page_size
  // Manually allocating memory rather than using vector<char> so
  // we can provide a helpful error message if the allocation fails.
  char *all_content;

  // all page cache entries are in one of these three lists
  List idle_list, active_list, inactive_list;
};


#endif // __PAGE_CACHE_H__
