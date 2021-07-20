#ifndef __PAGE_CACHE_H__
#define __PAGE_CACHE_H__

/*
  page_cache.h

  Simple page cache for a single file with adjustable levels of consistency.

  This is designed to be an intermediate layer between application
  code and an implementation of POSIX file I/O calls. For testing, an
  application can use this API directly with direct POSIX I/O calls as
  the backend, but the intended target is in the wrapper layer of the
  Themis-IO implementation. POSIX file I/O calls will be
  intercepted. Anything related to a Themis-IO server will be routed to
  this API, and this API will use the Themis-IO server as its backend.

  application calls write()
    -> wrapper intercept
    -> PageCache::write
    -> Themis-IO server

  visible-after-write
    POSIX default. All writes are guaranteed to be visible after the
    call to write() completes. No pages are cached. All reads and
    writes contact the file system directly.

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


  This cache will store pages of files, using up to some set amount of
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
  "foo//bar"). However, every distinct file has a distinct inode
  number.  Thus a file's inode number should be used to associate it
  with its cache entries.

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

  File : a file that is currently being managed by the cache.
    Uniquely identified by the inode of the file. Maintains two lists
    of cached pages: clean and dirty (either or both may be empty).
    Lookup table: inode_to_file

  Handle : one instance of an open file. Uniquely identified by an
    integer file descriptor returned by open(). Multiple Handles can
    reference one File.  Lookup table: fd_to_handle

  Entry : a slot in which one page of data can be cached. The number of
    available Entry objects is set when the maximum memory usage is set.
    (entries.count() = floor(max_memory / (page_size + sizeof(Entry)))).
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

  Implementation - a layer of virtual functions that is the backend
    implementation.  The static instance sample_implementation
    references POSIX I/O calls directly. This is where a Themis-IO
    backend implementation can be incorporated.

  Ed Karrels
  edk@illinois.edu
*/

#include <cassert>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <string>
#include <iostream>
#include <sstream>
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
  int openat(int dirfd, const char *pathname, int flags, ...);

  
  ssize_t write(int fd, const void *buf, size_t count);
  ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset);
  ssize_t read(int fd, void *buf, size_t count);
  ssize_t pread(int fd, void *buf, size_t count, off_t offset);

  off_t lseek(int fd, off_t offset, int whence);

  int ftruncate(int fd, off_t length);
  
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
  bool fsck();

  // Used for testing, this returns true iff the data at this offset
  // of the file is cached.
  bool isCached(int fd, long file_offset) const;

  // Used for testing, this returns true if the data at this offset
  // of the file is cached and the page is dirty.
  bool isPageDirty(int fd, long file_offset) const;


  /* TODO: add diagnostics? */
  int cacheHitCount();
  int cacheMissCount();
  
  
  
  static std::string currentDir();

  // Make path into an absolute path in the form (/name)*
  // No trailing slash, no "../" or "./" or "//".
  static std::string canonicalPath(const char *path);

  class Implementation {
  public:
    virtual int open(const char *pathname, int flags, ...);
    virtual int openAndStat(const char *pathname, int flags, mode_t mode,
                            struct stat *statbuf);
    virtual int openat(int dirfd, const char *pathname, int flags, ...);
    virtual ssize_t write(int fd, const void *buf, size_t count);
    virtual ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset);
    virtual ssize_t read(int fd, void *buf, size_t count);
    virtual ssize_t pread(int fd, void *buf, size_t count, off_t offset);
    virtual int fstat(int fd, struct stat *statbuf);
    virtual int ftruncate(int fd, off_t length);
    virtual int close(int fd);
  };
  static Implementation system_implementation;



  
private:

  class Entry;

  /* Using a template to support multiple independent sets of
     prev/next pointers in each object, so each cache entry can
     be in multiple lists (global idle/inactive/active lists) and
     (File clean/dirty lists). */
  template <class ListHandles>
  class MultiList {
  public:
    const int list_no_;
    int head_, tail_, size_;
    std::vector<Entry> &entries_;
    
  public:
    MultiList(std::vector<Entry> &entries, int list_no)
      : list_no_(list_no), head_(-1), tail_(-1), size_(0),
        entries_(entries){}

    int listNo() const {return list_no_;}
    bool empty() const {return size()==0;}
    
    int size() const {return size_;}

    int& prev(int id) {return ListHandles().prev(entries_[id]);}
    int& prev(Entry &e) {return ListHandles().prev(e);}
    int& next(int id) {return ListHandles().next(entries_[id]);}
    int& next(Entry &e) {return ListHandles().next(e);}

    int front() const {
      return head_;
    }

    // When an entry is remove from the list, don't bother invalidating
    // its list_next and list_prev pointers, because it will be immediately
    // placed on a different list and they will be set again.
    int popFront() {
      assert(!empty());
      int tmp = head_;
      if (head_ == tail_) {
        head_ = tail_ = -1;
      } else {
        head_ = next(entries_[head_]);
      }
      size_--;
      return tmp;
    }

    void pushFront(int id) {
      assert(id >= 0 && id < entries_.size());
      Entry &e = entries_[id];
      if (empty()) {
        next(e) = prev(e) = -1;
        head_ = tail_ = id;
      } else {
        prev(head_) = id;
        prev(e) = -1;
        next(e) = head_;
        head_ = id;
      }
      size_++;
    }

    int back() const {
      return tail_;
    }
    
    int popBack() {
      assert(!empty());
      int tmp = tail_;
      if (head_ == tail_) {
        head_ = tail_ = -1;
      } else {
        tail_ = prev(tail_);
      }
      size_--;
      return tmp;
    }

    void pushBack(int id) {
      assert(id >= 0 && id < entries_.size());
      Entry &e = entries_[id];
      if (empty()) {
        next(e) = prev(e) = -1;
        head_ = tail_ = id;
      } else {
        next(tail_) = id;
        next(e) = -1;
        prev(e) = tail_;
        tail_ = id;
      }
      size_++;
    }

    // caller asserts id is in this list
    // if clear_ptrs is true, reset prev and next
    void removeDirect(int id, bool clear_ptrs = false) {
      assert(!empty());
      Entry &e = entries_[id];
      if (head_ == id) {
        if (tail_ == id) {
          head_ = tail_ = -1;
        } else {
          head_ = next(e);
        }
      } else if (tail_ == id) {
        tail_ = prev(e);
      }
      size_--;
      if (clear_ptrs) {
        prev(e) = next(e) = -1;
      }
    }
  };

  // encapsulate linked list prev and next pointers
  struct ListPtrs {
    int prev, next;
    ListPtrs() {reset();}
    void reset() {
      prev = next = -1;
    }
  };

  /* For use with MultiList, given an Entry e this accesses
     e.global_list.{prev,next} */
  struct ListHandlesGlobal {
    int& prev(Entry &e) {return e.global_list.prev;}
    int& next(Entry &e) {return e.global_list.next;}
  };

  /* For use with MultiList, given an Entry e this accesses
     e.file_list.{prev,next} */
  struct ListHandlesFile {
    int& prev(Entry &e) {return e.file_list.prev;}
    int& next(Entry &e) {return e.file_list.next;}
  };

  // doubly-linked list of Entry objects using Entry::global_list
  using GlobalListBase = MultiList<ListHandlesGlobal>;
  
  // doubly-linked list of Entry objects using Entry::file_list
  using FileList = MultiList<ListHandlesFile>;

  
  /* Encapsulate one file, where a file is uniquely identified by
     inode number. If a file is opened multiple times, each will yield
     a different file descriptor and Handle, but all with reference
     the same File object, and their caches pages will be managed together.

     As part of supporting deferred opens, we need to handle the case
     where a process opens a file for writing and opens it a second
     time for reading. May need to keep a table of name of files that
     have been opened via deferred opens, so another call to open can
     use the same data.
  */
  class File {
  public:
    const ino_t inode;
    
    // Not needed now that inode is used to uniquely identify files,
    // but this is handy for debugging.
    const std::string canonical_path;

    /* The first time this file is opened, this object will retain the
       file descriptor returned by impl.open().

       The first opened file descriptor with read permission is stored
       in read_fd. The first writer in write_fd.

       If a file is opened write-only and a write() updates only part
       of a page, we would need to read the rest of the page in order
       to accurately write the whole page back to storage.  If read_fd
       has not been set, then that write will not be cached and will
       be immediately processed. */
    int read_fd, write_fd;

    // all cached pages for this file are either on clean_list or dirty_list
    PageCache::FileList clean_list, dirty_list;

    File(ino_t inode_, const std::string &canonical_path_, long length_,
         std::vector<Entry> &entries) :
      inode(inode_), canonical_path(canonical_path_),
      read_fd(-1), write_fd(-1),
      clean_list(entries, 0), dirty_list(entries, 1),
      length(length_), length_dirty(false),
      refcount(0) {}

    bool isReadable() {return read_fd != -1;}
    bool isReadOnly() {return isReadable() && !isWritable();}
    bool isWritable() {return write_fd != -1;}
    bool isWriteOnly() {return isWritable() && !isReadable();}
    bool isReadWrite() {return isReadable() && isWritable();}

    // if length still isn't set, use fstat() to set it now
    // not-needed long needLength(Implementation &impl);

    /* Keep a reference count of each Handle referencing this,
       so it can be removed from open_files_by_name and deallocated
       when they're all closed.
       flags: the 'flags' argument from a call to open(). */
    void addref(int fd, int flags) {
      ++refcount;
      flags &= O_ACCMODE;
      
      // try to use just one fd to minimize the number of open fd's
      if (flags == O_RDWR) {
        if (read_fd == -1 || read_fd != write_fd) {
          read_fd = write_fd = fd;
        }
      } else if (flags == O_RDONLY) {
        if (read_fd == -1)
          read_fd = fd;
      } else {
        assert(flags == O_WRONLY);
        if (write_fd == -1)
          write_fd = fd;
      }
    }
    int rmref() {return --refcount;}
    int getref() {return refcount;}

    bool isFileDescriptorInUse(int fd) {
      return fd == read_fd || fd == write_fd;
    }

    // return "inode=<inode>.path=<canonical_path>"
    std::string name();

    /* Used to share the length across all Handles, because multiple
       Handles may have it opened for O_APPEND.  When a file is
       opened, fstat() will be called right away to get the initial
       value. Use accessor functions to make it easier to trace where
       the value is read and written. */
    long getLength() {return length;}

    /* Update the length of the file.
       This can be caused by writing past EOF or by ftruncate.
       With VISIBLE_AFTER_CLOSE or VISIBLE_AFTER_EXIT, a file length
       change can be deferred. If a file is shortened with ftruncate,
       impl.ftruncate will be needed to make the change. If a file is
       lengthened with ftruncate, impl.ftruncate will be needed but only
       if the last byte never gets written. There can also be tricky
       cases where is shortened but then extended a little bit with a
       write. To keep things simple, if an ftruncate is ever deferred,
       just call impl.ftruncate when changes are flushed to disk. 
       Another tricky case: when the file is extended with ftruncate
       and the uncached data is read from the file, the system should know
       how large the file on disk is and not try to read past EOF.
       This is too many complications than I want to deal with right now,
       and I think deferring ftruncates is low-priority, so for now
       ftruncates will not be deferred.
       TODO: enable deferred ftruncate. */
    void setLength(long new_length, bool update_deferred) {
      length = new_length;
      // if (update_deferred) length_dirty = true;
    }

    // if the length was changed, call ftruncate now
    // TODO: this will be a no-op until deferred ftruncates are implemented
    void flushLengthChange(Implementation &impl);

    bool fsck(std::vector<Entry> &entries);

    /* Close this file. All clean and dirty pages must already be flushed.
       If the file length has been changed with ftruncate then the file
       length will also be updated. */
    void close(Implementation &impl);


  private:
    long length;

    /* True iff the file length has been changed with ftruncate and
       that the change has been deferred. */
    bool length_dirty;
    
    // Number of Handles referencing this file
    int refcount;
  };

  
  /* This encapsulates data associated with an open file descriptor.
     Multiple Handles can point to the same File. */
  class Handle {
  public:
    File * const file;
    long position;
    int fd;
    int open_flags;  // the "flags" argument to open()
    mode_t open_mode;  // the "mode" argument to open(), if O_CREAT

    /* if open is deferred (consistency = VISIBLE_AFTER_EXIT), then
       open_is_deferred will be true, and the arguments to open() will
       be saved in these fields. */
    bool open_is_deferred;
    int dirfd;  // >= 0 iff openat() was called
    
    /* canonical version of the pathname argument to open, in case the
       current directory is changed before the file is opened. */
    std::string path;

    Handle(File *f, int fd_, int open_flags_) : 
      file(f), position(0), fd(fd_), open_flags(open_flags_),
      open_mode(0), open_is_deferred(false), dirfd(-1) {}

    void setDeferred(int dirfd_, const std::string &path_,
                     mode_t mode_) {
      open_is_deferred = true;
      dirfd = dirfd_;
      path = path_;
      open_mode = mode_;
    }

    // return file access: O_RDONLY, O_RDWR, or O_WRONLY
    int getAccess() {
      return open_flags & O_ACCMODE;
    }
    
    bool isReadable() {
      int access = getAccess();
      return access == O_RDONLY || access == O_RDWR;
    }

    bool isWritable() {
      int access = getAccess();
      return access == O_WRONLY || access == O_RDWR;
    }
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
    Entry() : file(nullptr), page_id(-1), flags(0) {}
              
    
    File *file;

    // file offset = page_size * page_id + page_offset
    long page_id;

    /* global_list: prev/next indices for this entry on 
         PageCache.idle_list, PageCache.inactive_list, or PageCache.active_list.
         inactive, or active lists
       file_list: prev/next indices for this entry on the clean or
         dirty lists on the File object */
    ListPtrs global_list, file_list;

    // bits 0,1: current list 00=idle, 01=inactive, 10=active
    // bit 2: dirty bit
    unsigned flags;

    /* Assign file and page_id, and make sure page is marked clean.
       Don't change the listNo(). */
    void init(File *file_, long page_id_) {
      file = file_;
      page_id = page_id_;
      setClean();
    }

    bool isDirty() const {return (flags & 4) != 0;}
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

    // return current global_list: 0=idle 1=inactive 2=active
    int listNo() const {return flags & 3;}
    void setListNo(int list_no) {
      assert(list_no >=0 && list_no < 3);
      unsigned mask = 3;
      mask = ~mask;
      flags = (flags & mask) | list_no;
    }

  };


  // This adds a fsck function for GlobalLists
  class GlobalList : public GlobalListBase {
  public:
    GlobalList(std::vector<Entry> &entries, int list_no)
      : GlobalListBase(entries, list_no) {}

    bool fsck(int list_id, std::vector<Entry> &entries);

    using Handler = ListHandlesGlobal;
  };



  // Key for identifying a page in the cache: (file handle, page index)
  struct PageKey {
    const ino_t inode;
    const long page_id;

    PageKey(const PageKey &x) : inode(x.inode), page_id(x.page_id) {}
    PageKey(ino_t inode_, long page_id_) : inode(inode_), page_id(page_id_) {}

    size_t hash() const {
      const size_t factor = 11400714819323198329ull;
      size_t h = (size_t)inode;
      h ^= page_id + factor + (h << 12) + (h >> 4);
      return h;
    }

    bool operator == (const PageKey &x) const {
      return inode == x.inode && page_id == x.page_id;
    }
  };

  
  struct PageKeyHash {
    size_t operator() (const PageKey &p) const {
      return p.hash();
    }
  };


  /* Given a file descriptor returned by open, return the associated
     Handle*. This value comes from the user application, so
     fd may be invalid. */
  Handle *getHandle(int fd) const;

  // internal implementations, after mapping an integer fd to the
  // Handle object
  ssize_t pread(Handle *filedes, void *buf, size_t count, off_t offset);
  ssize_t pwrite(Handle *filedes, const void *buf, size_t count, off_t offset);


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


  /* TODO
     To support deferred opens, we'll need to return some file descriptor
     when open() is called. Either make something up that will be translated
     to a real FD when the file is actually opened, or reserve a FD that
     will eventually become the real one. */
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

  const GlobalList& getList(int list_id) const {
    switch (list_id) {
    case LIST_ACTIVE: return active_list;
    case LIST_INACTIVE: return inactive_list;
    case LIST_IDLE: default: return idle_list;
    }
  }

  GlobalList& getList(int list_id) {
    return const_cast<GlobalList&>(static_cast<const PageCache &>(*this).getList(list_id));
  }

  void moveEntryToList(int entry_id, GlobalList &dest_list) {
    Entry &e = entries[entry_id];
    int dest_list_no = dest_list.listNo();
    if (e.listNo() == dest_list_no) return;
    GlobalList &src_list = getList(e.listNo());

    src_list.removeDirect(entry_id);
    dest_list.pushBack(entry_id);
    e.setListNo(dest_list_no);
  }


  /* Look up a cache entry for a given page_id. Returns -1 if
     this page is not currently cached. */
  int getCachedPageEntry(File *f, long page_id);

  /* Look up a cache entry for a given page_id.
     If it's already cached, return the entry_id.
     If not, allocate a new entry and return the entry_id.

     If fill is set, then read the page content from the file.  If
     the caller is going to overwrite the whole page, they can set
     fill to false.

     If known_new is true, then caller has already confirmed that the
     page does not exist, so don't bother looking. */
  int getPageEntry(File *f, long page_id, bool fill,
                   bool known_new = false);

  /* Get an unused entry, either by taking one off the idle list or by
     evicting someone else. */
  int newEntry(File *f, long page_id);

  /* Writes an entry if it's dirty, disassociates it from its page,
     and returns it to the idle list. If clear_ptrs is true, then
     reset the list pointers for the per-file dirty/clean lists.  Only
     do this if the entry is not going to immediately be reassigned to
     another File. */
  void removeEntry(int entry_id, bool clear_ptrs = false);

  /* Given a dirty entry, write it to disk, mark it clean, and move it
     to the clean list. */
  int writeDirtyEntry(int entry_id);

  /* If the active list is much longer than the inactive list, move
     some of the older entries from active to inactive. */
  void balanceEntryLists();

  /* Find every cache entry associated with this file.  If
     dirty_only == true, just write every dirty entry.  Otherwise
     write every dirty entry and remove all entries. 
     this is used:
      - when a file is opened in VISIBLE_AFTER_CLOSE mode, to clear the
        read cache
      - when a file is closed in VISIBLE_AFTER_CLOSE mode, to flush dirty
        pages
      - in closeFileIfDone, when all handles to a file are closed
  */
  void flushFilePages(File *file, bool dirty_only);

  /* When to delete an File?
     if consistency <= ON_CLOSE: 
       only when close() is called on the last FD
     if consistency == ON_EXIT: 
       when the last page (clean an dirty lists are empty) is moved to idle,
       and all FD's are closed

     Returns true iff file was closed.
  */
  bool closeFileIfDone(File *file);
  
  
  static const int DEFAULT_PAGE_SIZE = 1024 * 1024;
  static const int DEFAULT_MEM_SIZE = 200 * (DEFAULT_PAGE_SIZE + sizeof(Entry));

  ConsistencyLevel consistency;
  
  Implementation &impl;

  // note lock() and unlock() do nothing if (!PAGE_CACHE_THREAD_SAFE)
  mutable std::recursive_mutex mtx;
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

  // lookup by inode
  std::unordered_map<ino_t,File*> inode_to_file;

  // lookup by file descriptor
  std::unordered_map<int,Handle*> fd_to_handle;

  // lookup entries by File and page_id
  using EntryTableType = std::unordered_map<PageKey,int,PageKeyHash>;
  EntryTableType entry_table;

  std::vector<Entry> entries;

  // the data for all cache entries in one large block
  // of size entries.size() * page_size
  // Manually allocating memory rather than using vector<char> so
  // we can provide a helpful error message if the allocation fails.
  char *all_content;

  // all page cache entries are in one of these three lists
  GlobalList idle_list, active_list, inactive_list;
};


#endif // __PAGE_CACHE_H__
