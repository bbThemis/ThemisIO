/*
Written by Lei Huang  (huang@tacc.utexas.edu) 
       and Zhao Zhang (zzhang@tacc.utexas.edu) 
       at TACC.
All rights are reserved.
*/

//#define _GNU_SOURCE

#include <stdio.h>
#include <execinfo.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <sys/stat.h>
#include <malloc.h>
#include <pthread.h>
#include <sys/time.h>
#include <errno.h>
#include <elf.h>
#include <termios.h>
#include <sys/syscall.h>
#include <stdarg.h>

#include <assert.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <dlfcn.h>
#include <link.h>

#include <sys/statfs.h>
#include <sys/statvfs.h>

#include <stddef.h>

#include "ucx_client.h"
// #include "qp_client.h"

#include "io_ops_common.h"
#include "xxhash.h"

#define min(a,b)        ( ((a)<(b)) ? (a) : (b) )
#define max(a,b)        ( ((a)>(b)) ? (a) : (b) )

#define PATH_MAX        4096
#define MAX_FILE_NAME_LEN	(256)
#define MAX_FD_DUP2ED	(6)
#define MAX_LEN_DIR_ENTRY_BUFF  (2*1024*1024)

#define MAX_OPENED_FILE	(2048)
#define MAX_OPENED_FILE_M1	((MAX_OPENED_FILE) - 1)
#define MAX_OPENED_DIR	(512)
#define MAX_OPENED_DIR_M1	((MAX_OPENED_DIR) - 1)

//pthread_mutex_t global_lock;
static const int IO_Msg_Size_op = ( offsetof(IO_CMD_MSG,op) + sizeof(int) );

struct statfs fs_stat_shm;
struct statvfs vfs_stat_shm;
int mpi_rank=-1;
static int pid, uid, Inited=0;
static int nQp=0;
static void *p_patch=NULL;

static int IsLibpthreadLoaded=0;
static unsigned long int img_mylib_base=0, img_libc_base=0, img_libpthread_base=0;

static unsigned long page_size, filter;

int Get_Fd_Redirected(int fd);
void Send_IO_Request(int idx_fs);
int Wait_For_IO_Request_Result(ucp_worker_h data_worker, int Tag_Magic);

//start	to compile list of memory blocks in /proc/pid/maps
#define MAX_NUM_SEG	(1024)
static int nSeg=0;
uint64_t Addr_libc=0;
uint64_t Addr_Min[MAX_NUM_SEG], Addr_Max[MAX_NUM_SEG];
static void Get_Module_Maps(void);
static void* Query_Available_Memory_BLock(void *);
//end	to compile list of memory blocks in /proc/pid/maps


//#define NHOOK	(6)
#define NHOOK	(8)
#define NHOOK_IN_PTHREAD	(5)

static long int func_addr[NHOOK], func_addr_in_lib_pthread[NHOOK_IN_PTHREAD];
static long int func_len[NHOOK], func_len_in_lib_pthread[NHOOK_IN_PTHREAD];
static long int org_func_addr[NHOOK], org_func_len[NHOOK];
static long int org_code_saved_lib_pthread[NHOOK_IN_PTHREAD];

#define MIN_MEM_SIZE	(0x1000*2)
#define NULL_RIP_VAR_OFFSET	(0x7FFFFFFF)
#define BOUNCE_CODE_LEN	(25)
#define OFFSET_HOOK_FUNC	(7)

extern "C"	{
#include "udis86.h"
}

#define MAX_INSTUMENTS	(24)
#define MAX_LEN_FUNC_NAME	(32)

char szRW_Home_Dir[32]=MYFS_ROOT_DIR;

static int Max_Bytes_Disassemble=24;

typedef struct
{
  int fd;                     /* File descriptor.  */
  size_t allocation;          /* Space allocated for the block.  */
  size_t size;                /* Total valid data in the block.  */	// nEntries
  size_t offset;              /* Current offset into the block.  */	// index of current enetry
  off_t filepos;              /* Position of next entry to read.  */
  // adding entries of my own data after this struct!!!
  // int dirfd	// the index in DirList[]
  // int nEntries;
  // pEntryOffsetList
  // pEntryNameBuff
}DIR;

#define LEN_DIR_NAME	(160)
typedef struct
{
  int fd;                     // File descriptor
  size_t allocation;          // Space allocated for the block.
  size_t size;                // Total valid data in the block. // nEntries
  size_t offset;              // Current offset into the block.	// index of current enetry
  off_t filepos;              // Position of next entry to read.
  // adding entries of my own data after this struct!!!
  int size_of_Data;	// the number of bytes of extra data for dir. 
  // char szDirName[LEN_DIR_NAME];
  //// int idx_fs;	// the index of file server
  // int nEntries;
  //// int IdxMin, IdxMax;	// the first and last entry cached on local client! When offset is out of the range, request more if available. 
  // pEntryOffsetList. int[nEntries]
  // pEntryNameBuff. char[]
}MYDIR;

typedef struct
{
  int fd;                     // File descriptor
  size_t allocation;          // Space allocated for the block.
  size_t size;                // Total valid data in the block. // nEntries
  size_t offset;              // Current offset into the block.	// index of current enetry
  off_t filepos;              // Position of next entry to read.
  // adding entries of my own data after this struct!!!
  int size_of_Data;	// the number of bytes of extra data for dir. 
  char szDirName[LEN_DIR_NAME];
  int idx_fs;	// the index of file server
  int nEntries;
  int IdxMin, IdxMax;	// the first and last entry cached on local client! When offset is out of the range, request more if available. 
  int EntryOffsetList;
  // pEntryNameBuff. char[]
}MYDIR_REAL;

typedef struct
 {
    uint64_t d_ino;
    uint64_t d_off;
    unsigned short int d_reclen;
    unsigned char d_type;
    char d_name[256];           /* We must not include limits.h! */
}dirent;

typedef struct	{
	int fd;			// the fd on file server
	int idx_fs;		// the index of file server holding the current file
	int OpenFlag;	// the flag used in open()
	int pad;
	off_t offset;
	off_t MaxDataRange;	// the largest offset in write(). 
	size_t st_ino, FileSize;	// inode and file size
	struct timespec modification_time;	// most recent modification time. 16 bytes. 
	char szFileName[192];
//	SEEK_OP_PARAM SeekList[MAX_DEFERRED_SEEK_OP];		// current status
}FILESTATUS, *PFILESTATUS;

typedef struct	{
	int fd;
	int idx_fs;
	int OpenFlag;	// the flag used in open()
	int Init_by_open;
	DIR *pDir;
	char szPath[MAX_FILE_NAME_LEN];
}DIRSTATUS, *PDIRSTATUS;

__thread int Inited_fd_List=0;
//__thread FILESTATUS FileList[MAX_OPENED_FILE];
//__thread DIRSTATUS DirList[MAX_OPENED_DIR];
__thread FILESTATUS *FileList=NULL;
__thread DIRSTATUS *DirList=NULL;

__thread int Next_Av_fd=0, Last_fd=-1, nFd=0;	// Last_fd==-1 means the list is empty. No active fd in list. 
__thread int Next_Av_dirfd=0, Last_dirfd=-1, nDirfd=0;

int Find_Next_Available_fd(void);
int Find_Next_Available_dirfd(void);
void Free_fd(int idx);
void Free_dirfd(int idx);

typedef struct	{
	int fd_src, fd_dest, Closed;
}FD_DUP2ED, *PFD_DUP2ED;

static int nFD_Dup2ed=0;
FD_DUP2ED Fd_Dup2_List[MAX_FD_DUP2ED];

int Query_Fd_Forward_Dest(int fd_src);	// return dest fd
int Query_Fd_Forward_Src(int fd_dest, int *Closed);	// return src fd
void Close_Duped_All_Fd(void);

void Remove_Dot_Dot(char szPath[]);
void Remove_Dot(char szPath[]);

static void Find_Func_Addr(char szPathLib[], char szFunc_List[][MAX_LEN_FUNC_NAME], long int func_addr[], long int func_len[], long int img_base_addr, int nhook);
static int callback(struct dl_phdr_info *info, size_t size, void *data);
static int input_hook_x(ud_t* u);
static ud_t ud_obj;
static int ud_idx;
static void Setup_Trampoline(void);
static void Init_udis86(void);
static size_t Set_Block_Size(void *pAddr);
static char *szOp=NULL;

int bDbg=0;


#define MAX_INSN_LEN (15)
#define MAX_JMP_LEN (5)
#define MAX_TRAMPOLINE_LEN	((MAX_INSN_LEN) + (MAX_JMP_LEN))

static unsigned char szOp_Bounce[]={0x48,0x89,0x44,0x24,0xf0,0x48,0xb8,0x78,0x56,0x34,0x12,0x78,0x56,0x34,0x12,0x48,0x89,0x44,0x24,0xf8,0x48,0x83,0xec,0x08,0xc3};
// p[7] int type should be replaced by hook function address. 

typedef struct {
	unsigned char trampoline[MAX_TRAMPOLINE_LEN];	// save the orginal function entry code and jump instrument
	unsigned char bounce[BOUNCE_CODE_LEN+3];			// the code can jmp to my hook function. +3 for padding
	void *pOrgFunc;
//	void *pNewFunc;
	int nBytesCopied;	// the number of bytes copied. Needed to remove hook
	int  Offset_RIP_Var;	// the offset of global variable. Relative address has to be corrected when copied into trampoline from original address
	long int OrgCode;
}TRAMPOLINE, *PTRAMPOLINE;

//0:  48 89 44 24 f0          mov    QWORD PTR [rsp-0x10],rax
//5:  48 b8 78 56 34 12 78    movabs rax,0x1234567812345678	// Put my new function pointer here!!!!!
//c:  56 34 12
//f:  48 89 44 24 f8          mov    QWORD PTR [rsp-0x8],rax
//14: 48 83 ec 08             sub    rsp,0x8
//18: c3                      ret

TRAMPOLINE *pTrampoline;

long int FirstBlockSize=0;
static char szFunc_List[NHOOK][MAX_LEN_FUNC_NAME]={"my_open", "my_close_nocancel", "my_read", "my_write", "my_lseek", "my_unlink", "my_fxstat", "my_xstat"};	// C 
static char szOrgFunc_List[NHOOK][MAX_LEN_FUNC_NAME]={"open64", "__close_nocancel", "__read", "__write", "lseek64", "unlink", "__fxstat", "__xstat64"};

extern "C" int my_fxstat(int vers, int fd, struct stat *buf);

typedef int (*org_system)(const char *command);
static org_system real_system=NULL;

typedef int (*org_open)(const char *pathname, int oflags,...);
static org_open real_open=NULL;

typedef int (*org_close)(int fd);
static org_close real_close=NULL;

typedef int (*org_close_nocancel)(int fd);
static org_close_nocancel real_close_nocancel=NULL;
//int my_close_nocancel(int fd);

typedef ssize_t (*org_read)(int fd, void *buf, size_t count);
static org_read real_read=NULL;

typedef ssize_t (*org_write)(int fd, const void *buf, size_t count);
static org_write real_write=NULL;

typedef int (*org_lxstat)(int __ver, const char *__filename, struct stat *__stat_buf);
static org_lxstat real_lxstat=NULL;

typedef int (*org_xstat)(int __ver, const char *__filename, struct stat *__stat_buf);
static org_xstat real_xstat=NULL;

typedef int (*org_fxstat)(int vers, int fd, struct stat *buf);
static org_fxstat real_fxstat=NULL;

typedef int (*org_fxstatat)(int __ver, int dirfd, const char *__filename, struct stat *__stat_buf, int flags);
static org_fxstatat real_fxstatat=NULL;

typedef int (*org_faccessat)(int dirfd, const char *pathname, int mode, int flags);
static org_faccessat real_faccessat=NULL;

typedef off_t (*org_lseek)(int fd, off_t offset, int whence);
static org_lseek real_lseek=NULL;

typedef DIR * (*org_opendir)(const char *__name);
static org_opendir real_opendir=NULL;

typedef int (*org_closedir)(DIR *__dirp);
static org_closedir real_closedir=NULL;

typedef dirent* (*org_readdir)(DIR *__dirp);
static org_readdir real_readdir=NULL;

typedef int (*org_ferror)(FILE *stream);
static org_ferror real_ferror=NULL;

typedef size_t (*org_fwrite)(const void *ptr, size_t size, size_t nmemb, FILE *stream);
static org_fwrite real_fwrite=NULL;

typedef size_t (*org_fread)(void *ptr, size_t size, size_t nmemb, FILE *stream);
static org_fread real_fread=NULL;

typedef int (*org_isatty)(int fd);
static org_isatty real_isatty=NULL;

typedef int (*org_tcgetattr)(int fd, struct termios *termios_p);
static org_tcgetattr real_tcgetattr=NULL;

typedef int (*org_mkdir)(const char *path, mode_t mode);
static org_mkdir real_mkdir=NULL;

typedef int (*org_mkdirat)(int dirfd, const char *pathname, mode_t mode);
static org_mkdirat real_mkdirat=NULL;

typedef int (*org_rmdir)(const char *path);
static org_rmdir real_rmdir=NULL;

typedef int (*org_unlink)(const char *path);
static org_unlink real_unlink=NULL;

typedef int (*org_dup2)(int oldfd, int newfd);
static org_dup2 real_dup2=NULL;


static __thread char szCurDir[PATH_MAX]=""; // current dir

typedef int (*org_chdir)(const char *path);
static org_chdir real_chdir=NULL;

typedef int (*org_fchdir)(int fd);
static org_fchdir real_fchdir=NULL;

typedef char * (*org_getcwd)(char *buf, size_t size);
static org_getcwd real_getcwd=NULL;

typedef int (*org_openat)(int dirfd, const char *pathname, int flags,...);
org_openat real_openat=NULL;

typedef int (*org_openat_2)(int dirfd, const char *pathname, int flags);
org_openat_2 real_openat_2=NULL;

typedef DIR * (*org_fdopendir)(int fd);
org_fdopendir real_fdopendir=NULL;

typedef int (*org_fallocate)(int fd, int mode, off_t offset, off_t len);
org_fallocate real_fallocate=NULL;

typedef int (*org_posix_fallocate)(int fd, off_t offset, off_t len);
org_posix_fallocate real_posix_fallocate=NULL;

typedef int (*org_truncate)(const char *path, off_t length);
org_truncate real_truncate=NULL;

typedef int (*org_ftruncate)(int fd, off_t length);
org_ftruncate real_ftruncate=NULL;

typedef int (*org_fsync)(int fd);
org_fsync real_fsync=NULL;

typedef int (*org_posix_fadvise)(int fd, off_t offset, off_t len, int advice);
org_posix_fadvise real_posix_fadvise=NULL;

typedef ssize_t (*org_pread)(int fd, void *buf, size_t count, off_t offset);
org_pread real_pread=NULL;

typedef ssize_t (*org_pwrite)(int fd, const void *buf, size_t count, off_t offset);
org_pwrite real_pwrite=NULL;

typedef int (*org_futimens)(int fd, const struct timespec times[2]);
org_futimens real_futimens=NULL;

typedef int (*org_utime)(const char *pathname, const struct utimbuf *times);
org_utime real_utime=NULL;

typedef int (*org_utimes)(const char *pathname, const struct timeval times[2]);
org_utimes real_utimes=NULL;

typedef int (*org_utimensat)(int dirfd, const char *pathname, const struct timespec times[2], int flags);
org_utimensat real_utimensat=NULL;

typedef int (*org_fchmod)(int fd, mode_t mode);
org_fchmod real_fchmod=NULL;

typedef int (*org_fchmodat)(int dirfd, const char *pathname, mode_t mode, int flags);
org_fchmodat real_fchmodat=NULL;

typedef int (*org_access)(const char *pathname, int mode);
org_access real_access=NULL;

typedef int (*org_statfs)(const char *pathname, struct statfs *buf);
org_statfs real_statfs=NULL;

typedef int (*org_statvfs)(const char *pathname, struct statvfs *buf);
org_statvfs real_statvfs=NULL;

typedef char * (*org_realpath)(const char *pathname, char *resolved_path);
//org_realpath real_realpath=NULL, real_realpath_2_3=NULL, real_realpath_2_2_5=NULL;
org_realpath real_realpath=NULL;

typedef int (*org_rename)(const char *OldName, const char *NewName);
org_rename real_rename=NULL;

/*
double t_MPI_Init, t_MPI_Finalize;

typedef int (*org_MPI_Init)(int *argc, char ***argv);
org_MPI_Init real_MPI_Init=NULL;

typedef int (*org_MPI_Finalize)(void);
org_MPI_Finalize real_MPI_Finalize=NULL;
*/

typedef int (*org_flock)(int fd, int operation);
org_flock real_flock=NULL;


#define F_LINUX_SPECIFIC_BASE  1024
#define F_ADD_SEALS     (F_LINUX_SPECIFIC_BASE + 9)
#define F_OFD_GETLK    36
#define F_OFD_SETLK    37
#define F_OFD_SETLKW   38

typedef int (*org_fcntl)(int fd, int cmd, ...);
org_fcntl real_fcntl=NULL;

typedef int (*org_unlinkat)(int dirfd, const char *pathname, int flags);
org_unlinkat real_unlinkat=NULL;

static void Update_CWD(void);

static int Get_Position_of_Next_Line(char szBuff[], int iPos, int nBuffSize);
static void Query_Original_Func_Address(void);

extern "C" DIR *opendir(const char *szDirName);

static char szPathLibc[128]="";
static char szPathLibpthread[128];
static char szPathMyLib[256];  // the name of myself


/*
inline void Send_IO_Request(int idx_fs)
{
	IO_CMD_MSG *pIO_Cmd = (IO_CMD_MSG *)loc_buff;

	// send the IO request first
	pIO_Cmd->tag_magic = rand();
	pClient_qp[idx_fs]->IB_Put(loc_buff, pClient_qp[idx_fs]->mr_loc->lkey, (void*)(pClient_qp[idx_fs]->remote_addr_IO_CMD + sizeof(IO_CMD_MSG)*pClient_qp[idx_fs]->Idx_fs), pClient_qp[idx_fs]->pal_remote_mem.key, sizeof(IO_CMD_MSG));

	// send a msg to notify that a new IO quest is coming.
	loc_buff[0] = TAG_NEW_REQUEST;
	pClient_qp[idx_fs]->IB_Put(loc_buff, pClient_qp[idx_fs]->mr_loc->lkey, (void*)(pClient_qp[idx_fs]->remote_addr_new_msg + sizeof(char)*pClient_qp[idx_fs]->Idx_fs), pClient_qp[idx_fs]->pal_remote_mem.key, 1);

	Wait_For_IO_Request_Result(pIO_Cmd->tag_magic);
}
*/

inline int Get_Fd_Redirected(int fd)
{
	if(fd >= 3)	{
		return fd;
	}
	else if(fd == 0)	{
		if(fd_stdin > 0)	{
			return fd_stdin;
		}
	}
	else if(fd == 1)	{
		if(fd_stdout > 0)	{
			return fd_stdout;
		}
	}
	else if(fd == 2)	{
		if(fd_stderr > 0)	{
			return fd_stderr;
		}
	}

	return fd;
}

inline void Test_UCX_Put(int idx_fs) 
{
	// fprintf(stdout, "Test_UCX_Put started\n");
	sprintf((char*)ucx_loc_buff, "Test_UCX_Put%d", idx_fs);
	pClient_ucx[idx_fs]->UCX_Put(ucx_loc_buff, (void*)(pClient_ucx[idx_fs]->remote_addr_IO_CMD), pClient_ucx[idx_fs]->pal_remote_mem.rkey, IO_Msg_Size_op);
	ucx_loc_buff[0] = TAG_NEW_REQUEST;
	pClient_ucx[idx_fs]->UCX_Put(ucx_loc_buff, (void*)(pClient_ucx[idx_fs]->remote_addr_new_msg), pClient_ucx[idx_fs]->pal_remote_mem.rkey, 1);
	// fprintf(stdout, "Test_UCX_Put finished\n");
}



inline void PrintIOType(IO_CMD_MSG *pIO_Cmd) {
	int Op_Tag = pIO_Cmd->op & 0xFF;
	switch(Op_Tag)	{
	case RF_RW_OP_OPEN:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_OPEN\n");
		break;
	case RF_RW_OP_CLOSE:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_CLOSE\n");
		break;
	case RF_RW_OP_OPENDIR:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_OPENDIR\n");
		break;
	case RF_RW_OP_READ:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_READ\n");
		break;
	case RF_RW_OP_WRITE:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_WRITE\n");
		break;
	case RF_RW_OP_STAT:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_STAT\n");
		break;
	case RF_RW_OP_LSTAT:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_LSTAT\n");
		break;
	case RF_RW_OP_FSTAT:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_FSTAT\n");
		break;
	case RF_RW_OP_REMOVE_FILE:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_REMOVE_FILE\n");
		break;
	case RF_RW_OP_REMOVE_DIR:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_REMOVE_DIR\n");
		break;
	case RF_RW_OP_MKDIR:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_MKDIR\n");
		break;
	case RF_RW_OP_TRUNCATE:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_TRUNCATE\n");
		break;
	case RF_RW_OP_FTRUNCATE:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_FTRUNCATE\n");
		break;
	case RF_RW_OP_FUTIMENS:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_FUTIMENS\n");
		break;
	case RF_RW_OP_UTIMES:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_UTIMES\n");
		break;
	case RF_RW_OP_ADDENTRY_PARENT_DIR:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_ADDENTRY_PARENT_DIR\n");
		break;
	case RF_RW_OP_REMOVEENTRY_PARENT_DIR:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_REMOVEENTRY_PARENT_DIR\n");
		break;

	case RF_RW_OP_DIR_EXIST:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_DIR_EXIST\n");
		break;
	case RF_RW_OP_FREE_STRIPE_DATA:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_FREE_STRIPE_DATA\n");
		break;
	case RF_RW_OP_PRINT_MEM:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_PRINT_MEM\n");
		break;
	case RF_RW_OP_HELLO:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_HELLO\n");
		break;
	case RF_RW_OP_STAT_FS:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_STAT_FS\n");
		break;
	case RF_RW_OP_READ_DIR_ENTRIES:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_READ_DIR_ENTRIES\n");
		break;
	case RF_RW_OP_DISCONNECT:
		fprintf(stdout, "DBG> PrintIOType:RF_RW_OP_DISCONNECT\n");
		break;
	default:
		printf("ERROR> Unknown Op_Tag = %d in PrintIOType().\n", Op_Tag);
		break;
	}
}

inline void Send_IO_Request(int idx_fs)
{
	IO_CMD_MSG *pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
	RW_FUNC_RETURN *pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
	int bTimeout;
	PrintIOType(pIO_Cmd);
	fprintf(stdout, "DBG> Send_IO_Request begins loc %p rem %p\n", pIO_Cmd, pResult);
	while(1)	{
		// send the IO request first
		pIO_Cmd->tag_magic = rand();
//		pClient_qp[idx_fs]->IB_Put(loc_buff, pClient_qp[idx_fs]->mr_loc->lkey, (void*)(pClient_qp[idx_fs]->remote_addr_IO_CMD), pClient_qp[idx_fs]->pal_remote_mem.key, sizeof(IO_CMD_MSG));
		// pClient_qp[idx_fs]->IB_Put(loc_buff, mr_loc->lkey, (void*)(pClient_qp[idx_fs]->remote_addr_IO_CMD), pClient_qp[idx_fs]->pal_remote_mem.key, IO_Msg_Size_op);
		pClient_ucx[idx_fs]->UCX_Put(ucx_loc_buff,  (void*)(pClient_ucx[idx_fs]->remote_addr_IO_CMD), (pClient_ucx[idx_fs]->pal_remote_mem.rkey), IO_Msg_Size_op);
		// send a msg to notify that a new IO quest is coming.
		ucx_loc_buff[0] = TAG_NEW_REQUEST;
		// pClient_qp[idx_fs]->IB_Put(loc_buff, mr_loc->lkey, (void*)(pClient_qp[idx_fs]->remote_addr_new_msg), pClient_qp[idx_fs]->pal_remote_mem.key, 1);
		pClient_ucx[idx_fs]->UCX_Put(ucx_loc_buff,  (void*)(pClient_ucx[idx_fs]->remote_addr_new_msg), (pClient_ucx[idx_fs]->pal_remote_mem.rkey), 1);
		bTimeout = Wait_For_IO_Request_Result(pClient_ucx[idx_fs]->ucp_worker, pIO_Cmd->tag_magic);
		if(bTimeout==0)	break;
		if( pIO_Cmd->op == RF_RW_OP_DISCONNECT)	break;	// NEVER send multiple RF_RW_OP_DISCONNECT command!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		else if( pIO_Cmd->op == RF_RW_OP_CLOSE)	{
			printf("Warning> Time out in close().\n");
			pResult->ret_value = 0;
			break;	// NEVER send multiple RF_RW_OP_DISCONNECT command!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		}
	}
	fprintf(stdout, "DBG> Send_IO_Request finished loc %p rem %p\n", pIO_Cmd, pResult);
}
/*
static inline int fetch_and_add(int* variable, int value)
{
    __asm__ volatile("lock; xaddl %0, %1"
      : "+r" (value), "+m" (*variable) // input + output
      : // No input-only
      : "memory"
    );
    return value;
}
*/
static int callback(struct dl_phdr_info *info, size_t size, void *data)
{
	if(strstr(info->dlpi_name, "/libc.so"))	{
		strcpy(szPathLibc, info->dlpi_name);
		img_libc_base = (info->dlpi_addr + info->dlpi_phdr[0].p_vaddr) & filter;
	}
	else if(strstr(info->dlpi_name, "/libpthread.so"))	{
		strcpy(szPathLibpthread, info->dlpi_name);
		img_libpthread_base = (info->dlpi_addr + info->dlpi_phdr[0].p_vaddr) & filter;
		IsLibpthreadLoaded = 1;
	}
	
	return 0;
}

inline void Standardlize_Path(const char *szInput, char *szFullPath)
{
	if( szInput[0] == '/')	{	// absolute path
		strcpy(szFullPath, szInput);
	}
//	else if( szInput[0] == '.')	{
//		strcpy(szFullPath, szCurDir);
//	}
	else	{	// relative path
		sprintf(szFullPath, "%s/%s", szCurDir, szInput);
	}

	Remove_Dot_Dot(szFullPath);
	Remove_Dot(szFullPath);
}

inline void Gather_FileName_Info(char szName[], int *nLenFileName, int *nLenParentDirName, unsigned long long *pFile_hash, unsigned long long *pParent_dir_hash, int *pIdx_fs)
{
	int i, nReadItem;
	unsigned long long fn_hash;

	if(strcmp(MYFS_ROOT_DIR, szName)==0)	{
		*nLenFileName = strlen(szName);
		*nLenParentDirName = 0;	// root
		*pFile_hash = XXH64(szName, *nLenFileName, 0);
		*pIdx_fs = (*pFile_hash) % (UCXFileServerListLocal.nFSServer);
		if(pParent_dir_hash)	*pParent_dir_hash = 0;
		return;	// root dir
	}

	for(i=0; ; i++)	{
		if(szName[i]==0)	{
			*nLenFileName = i;
			break;
		}
	}
	for(; i>=1 ;i--)	{
		if( (szName[i] == '/') && (szName[i-1] != '/') )	{
			break;
		}
	}
	*nLenParentDirName = i;

	*pIdx_fs = -1;	// set an invalid index
	if( ((*nLenFileName)>=3) && (szName[(*nLenFileName)-3] == '_') && (szName[(*nLenFileName)-2] == 'S') )	{	// specific tag used to choose specific server!!!!!!!!!!!!!!!!!!!!
		nReadItem = sscanf(szName+(*nLenFileName)-1, "%x", pIdx_fs);
		if(nReadItem != 1)	{
			printf("Warning> Failed to extract server index from %s\n", szName+(*nLenFileName)-3);
			*pIdx_fs = -1;
		}
		else	{
			if((*pIdx_fs) >= UCXFileServerListLocal.nFSServer)	{
				printf("Warning> idx_Server (%d) > nFSServer (%d) in %s\n", *pIdx_fs, UCXFileServerListLocal.nFSServer, szName+(*nLenFileName)-3);
				*pIdx_fs = -1;
//				exit(1);
			}
		}
	}
	if( (*nLenFileName > 10) && (szName[(*nLenFileName)-9] == '.') && (szName[(*nLenFileName)-8] == '0')  && (szName[(*nLenFileName)-7] == '0') )	{
		nReadItem = sscanf(szName+(*nLenFileName)-6, "%d", pIdx_fs);
		if(nReadItem != 1)	{
			printf("Warning> Failed to extract server index from %s\n", szName+(*nLenFileName)-4);
			*pIdx_fs = -1;
		}
		else	{
			*pIdx_fs = (*pIdx_fs) % (UCXFileServerListLocal.nFSServer);
		}
//		printf("DBG> Name %s idx_fs = %d\n", szName, *pIdx_fs);
	}

	
	if(pFile_hash)	{
		*pFile_hash = XXH64(szName, *nLenFileName, 0);
		if(*pIdx_fs < 0)	*pIdx_fs = (*pFile_hash) % (UCXFileServerListLocal.nFSServer);
	}
	if(pParent_dir_hash)	*pParent_dir_hash = XXH64(szName, *nLenParentDirName, 0);

	assert( ((*pIdx_fs) >= 0) && ((*pIdx_fs) < UCXFileServerListLocal.nFSServer ) );
}
// QP_CLIENT
/*
inline void Setup_QP_if_Needed(int idx_fs) 
{
	if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
	//	CLIENT_QUEUEPAIR *pQP;
	//	pQP = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));	
	//	pQP->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!
		pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
		pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
		fetch_and_add(&nQp, 1);	// atomically add the counter
	}
}
*/
inline void Setup_UCX_if_Needed(int idx_fs) {
	if(pClient_ucx[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
		pClient_ucx[idx_fs] = (CLIENT_UCX *)malloc(sizeof(CLIENT_UCX));
		pClient_ucx[idx_fs]->Setup_UCP_Connection(idx_fs, (char*)ucx_loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)ucx_rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
		fetch_and_add(&nQp, 1);	// atomically add the counter
	}
}

extern "C" int my_open(const char *pathname, int oflags, ...)
{
	int mode = 0, two_args=1, fd, idx_fs, idx_fd;
	char *p_szDirEntryName=NULL;
	char szFullPath[MAX_FILE_NAME_LEN];
//	unsigned char szDirEntryBuff[MAX_LEN_DIR_ENTRY_BUFF];// entry data buffer
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	unsigned long long fn_hash;
	DIR *pDir;
	size_t *pInode_and_FileSize;

	if (oflags & O_CREAT)	{
		va_list arg;
		va_start (arg, oflags);
		mode = va_arg (arg, int);
		va_end (arg);
		two_args=0;
	}
	Standardlize_Path(pathname, szFullPath);
	
	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{
		if(ucx_loc_buff == NULL)	{
			// Allocate_loc_rem_buff();
			Allocate_ucx_loc_rem_buff();
		}
		if(FileList == NULL)	{
			FileList = (FILESTATUS *)malloc(sizeof(FILESTATUS)*MAX_OPENED_FILE);
			DirList = (DIRSTATUS *)malloc(sizeof(DIRSTATUS)*MAX_OPENED_DIR);
			memset(FileList, 0, sizeof(FILESTATUS)*MAX_OPENED_FILE);
			memset(DirList, 0, sizeof(DIRSTATUS)*MAX_OPENED_DIR);
		}
		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;

		if(oflags & O_DIRECTORY)	{	// directory
			pDir = opendir(szFullPath);
			if(pDir)	{
				idx_fd = Find_Next_Available_dirfd();
				assert(idx_fd >= 0);
				Gather_FileName_Info(szFullPath, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), NULL, &idx_fs);
//				printf("DBG> nFSServer = %d File %s idx_fs = %d\n", FileServerListLocal.nFSServer, szFullPath, idx_fs);
				// Setup_QP_if_Needed(idx_fs);
				Setup_UCX_if_Needed(idx_fs);
//				idx_fs = pIO_Cmd->file_hash % pFileServerList->nFSServer;	// the index of which file server holding this file
				DirList[idx_fd].fd = idx_fd;
				DirList[idx_fd].idx_fs = idx_fs;
				DirList[idx_fd].OpenFlag = oflags;
				DirList[idx_fd].Init_by_open = 1;
				DirList[idx_fd].pDir = pDir;
				strcpy(DirList[idx_fd].szPath, szFullPath);
//				printf("DBG> File %s idx_fs = %d\n", szFullPath, idx_fs);
				return (idx_fd+FD_DIR_BASE);
			}
			else	{
				errno = ENOENT;
				return (-1);
			}
		}

		if( (! two_args) && ( oflags & O_CREAT ) )	{
			Gather_FileName_Info(szFullPath, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), &(pIO_Cmd->parent_dir_hash), &idx_fs);	// create a new file
			if( (pIO_Cmd->nLen_FileName - pIO_Cmd->nLen_Parent_Dir_Name - 1) >= MAX_ENTRY_NAME_LEN)	{
				printf("ERROR> The length of entry name >= MAX_ENTRY_NAME_LEN (%d).\n%s\nQuit\n", MAX_ENTRY_NAME_LEN, szFullPath);
				exit(1);
			}
		}
		else	{
			Gather_FileName_Info(szFullPath, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), NULL, &idx_fs);
			pIO_Cmd->parent_dir_hash = 0;
		}
		// Setup_QP_if_Needed(idx_fs);
		Setup_UCX_if_Needed(idx_fs);
//		printf("DBG> open(%s) idx_fs = %d bCreate = %d\n", szFullPath, idx_fs, oflags & O_CREAT);
		
		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		// pIO_Cmd->rkey = mr_rem->rkey;
		CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
		// pIO_Cmd->rem_buff = mr_rem->addr;
		pIO_Cmd->rem_buff = ucx_rem_buff;
		strcpy(pIO_Cmd->szName, szFullPath);
		pIO_Cmd->flag = oflags;
		pIO_Cmd->mode = mode;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_OPEN;
//		pIO_Cmd->nTokenNeeded = 0;
		Send_IO_Request(idx_fs);
		// Test_UCX_Put(idx_fs);
		fd = (pResult->ret_value) & 0xFFFFFFFF;
		if(fd < 0)	{
			errno = pResult->myerrno;
			return fd;
		}
//		if(oflags & O_DIRECTORY)	{	// directory
//			pDir = ;
//			idx_fd = Find_Next_Available_dirfd();
//			assert(idx_fd >= 0);
//			DirList[idx_fd].fd = fd;
//			DirList[idx_fd].idx_fs = idx_fs;
//			DirList[idx_fd].OpenFlag = oflags;
//			DirList[idx_fd].Init_by_open = 1;
//			DirList[idx_fd].pDir = NULL;	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//			strcpy(DirList[idx_fd].szPath, szFullPath);
//			return (idx_fd+FD_DIR_BASE);
//		}
//		else	{	// regular file
			pInode_and_FileSize = (size_t *)( (char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int) );
			idx_fd = Find_Next_Available_fd();
			assert(idx_fd >= 0);
			FileList[idx_fd].fd = fd;
			FileList[idx_fd].idx_fs = idx_fs;
			FileList[idx_fd].OpenFlag = oflags;
			FileList[idx_fd].offset = 0;	// NEED to set at the end of file if O_APPEND!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			FileList[idx_fd].MaxDataRange = 0;
			FileList[idx_fd].st_ino = idx_fs;
			FileList[idx_fd].st_ino = FileList[idx_fd].st_ino << 32;
			FileList[idx_fd].st_ino |= pInode_and_FileSize[0];
			FileList[idx_fd].FileSize = pInode_and_FileSize[1];
			FileList[idx_fd].modification_time.tv_sec = 0;
			FileList[idx_fd].modification_time.tv_nsec = 0;
			strcpy(FileList[idx_fd].szFileName, szFullPath);
//			printf("DBG> File %s idx_fs = %d\n", szFullPath, idx_fs);
//			printf("DBG> File %s inode = %lx size = %ld\n", szFullPath, FileList[idx_fd].st_ino, FileList[idx_fd].FileSize);

			return (idx_fd+FD_FILE_BASE);
//		}
	}

	// orginal open() for non-listed file
	if(two_args)    {
		fd = real_open(pathname, oflags);
	}
	else    {
		fd = real_open(pathname, oflags, mode);
	}

	return fd;
}
/*
extern "C" int system(const char *command)
{
	if(real_system==NULL)	{
		real_system = (org_system)dlsym(RTLD_NEXT, "__libc_system");
		assert(real_system != NULL);
	}

	printf("INFO> MPI_RANK = %d system(%s)\n", mpi_rank, command);

	return real_system(command);
}
extern "C" int __libc_system(const char *command) __attribute__ ( (alias ("system")) );
*/
typedef int (*org_mkostemp)(char *name_template, int flags);
org_mkostemp real_mkostemp=NULL;

extern "C" int mkostemp(char *name_template, int flags)
{
	char szMyTemplate[]="/dev/shm/mkostemp_XXXXXX";
	printf("INFO> mkostemp(%s, %d)\n", name_template, flags);
	if(real_mkostemp==NULL)   {
		real_mkostemp = (org_mkostemp)dlsym(RTLD_NEXT, "mkostemp64");
		assert(real_mkostemp != NULL);
	}
	
	if(strncmp(name_template, "/myfs/", 6)==0)      {
		return real_mkostemp(szMyTemplate, flags);
	}
	
	return real_mkostemp(name_template, flags);
}
extern "C" int mkostemp64(char *name_template, int flags) __attribute__ ( (alias ("mkostemp")) );


extern "C" int close(int fd)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	META_DATA_ON_CLOSE *pMetaData_OnClose;
	int ret, fd_real, idx_fs, fd_idx;
	
	if(real_close==NULL)    {
		real_close = (org_close)dlsym(RTLD_NEXT, "close");
		assert(real_close != NULL);
	}
	
	if(Inited == 0) {       // init() not finished yet
		return real_close(fd);
	}
	
	if(fd >= FD_DIR_BASE)   {       // directory
		if(fd == DUMMY_FD_DIR)  {
			printf("ERROR> Unexpected fd == DUMMY_FD_DIR in close().\n");
			return 0;
		}
		Free_dirfd(fd-FD_DIR_BASE);
		return 0;
	}
	else if(fd >= FD_FILE_BASE)     {       // regular file
		fd_real = GetFileFD(fd);
		assert(fd_real >= 0);
		idx_fs = GetFileFSIdx(fd);
		
		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0; // init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data.
		
		pIO_Cmd->fd = fd_real;
		// pIO_Cmd->rkey = mr_rem->rkey;
		CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
		// pIO_Cmd->rem_buff = mr_rem->addr;
		pIO_Cmd->rem_buff = ucx_rem_buff;
//		pIO_Cmd->nTokenNeeded = 0;

		fd_idx = fd-FD_FILE_BASE;
		pMetaData_OnClose = (META_DATA_ON_CLOSE *)pIO_Cmd->szName;

		pMetaData_OnClose->MaxDataRange = FileList[fd_idx].MaxDataRange;
		pMetaData_OnClose->modification_time.tv_sec = FileList[fd_idx].modification_time.tv_sec;
		pMetaData_OnClose->modification_time.tv_nsec = FileList[fd_idx].modification_time.tv_nsec;

//		printf("DBG> pMetaData_OnClose->MaxDataRange = %ld\n", pMetaData_OnClose->MaxDataRange);
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_CLOSE;
		Send_IO_Request(idx_fs);
		
		ret = (pResult->ret_value) & 0xFFFFFFFF;
		if(ret < 0)     errno = pResult->myerrno;
		Free_fd(fd-FD_FILE_BASE);
		return ret;
	}
	
	return real_close(fd);
}
extern "C" int __close(int fd) __attribute__ ( (alias ("close")) );


extern "C" int my_close_nocancel(int fd)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	META_DATA_ON_CLOSE *pMetaData_OnClose;
	int ret, fd_real, idx_fs, fd_idx;

	if(fd >= FD_DIR_BASE)	{	// directory
		if(fd == DUMMY_FD_DIR)	{
			printf("ERROR> Unexpected fd == DUMMY_FD_DIR in close().\n");
			return 0;
		}
		Free_dirfd(fd-FD_DIR_BASE);
		return 0;
	}
	else if(fd >= FD_FILE_BASE)	{	// regular file
		fd_real = GetFileFD(fd);
		idx_fs = GetFileFSIdx(fd);
		
		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		pIO_Cmd->fd = fd_real;
		// pIO_Cmd->rkey = mr_rem->rkey;
		CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
		// pIO_Cmd->rem_buff = mr_rem->addr;
		pIO_Cmd->rem_buff = ucx_rem_buff;
		
//		pIO_Cmd->nTokenNeeded = 0;
		fd_idx = fd-FD_FILE_BASE;
		pMetaData_OnClose = (META_DATA_ON_CLOSE *)pIO_Cmd->szName;

		pMetaData_OnClose->MaxDataRange = FileList[fd_idx].MaxDataRange;
		pMetaData_OnClose->modification_time.tv_sec = FileList[fd_idx].modification_time.tv_sec;
		pMetaData_OnClose->modification_time.tv_nsec = FileList[fd_idx].modification_time.tv_nsec;
		
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_CLOSE;
		Send_IO_Request(idx_fs);
		
		ret = (pResult->ret_value) & 0xFFFFFFFF;
		if(ret < 0)	errno = pResult->myerrno;
		return ret;
	}

	return real_close_nocancel(fd);
//	char szMsg[]="DBG> In my_close_nocancel()\n";
//	real_write(STDERR_FILENO, szMsg, strlen(szMsg));
/*
	int fd_idx, idx_func_result, myerrno, fd_dup2ed_Src, fd_dup2ed_Dest, fd_Closed;
	int ret;
	RF_OP_MSG Op_Msg;
	FUNC_RESULT *p_Func_Result;

	if(fd >= FAKE_FD)	{
		fd_dup2ed_Src = Query_Fd_Forward_Src(fd, &fd_Closed);
		if( (fd_dup2ed_Src >= 0) && (fd_Closed==0) )	{
			return 0;	// doing nothing
		}
	}
	else	{
		fd_dup2ed_Dest = Query_Fd_Forward_Dest(fd);
		if(fd_dup2ed_Dest >= FAKE_FD)	{
			ret = real_close_nocancel(fd);
			if(ret == -1)	{
				printf("ERROR: fail to close fd %d\n", fd);
			}
			fd = fd_dup2ed_Dest;	// keep going. To close the real associated fd
		}
	}

	if(fd >= FAKE_RW_DIR_FD)	{	// RW Dir
		fd_idx = fd-FAKE_RW_DIR_FD;
		if(DirStatus[fd_idx].pData)	free(DirStatus[fd_idx].pData);
		DirStatus[fd_idx].pData = 0;
		return 0;
	}
	else if(fd >= FAKE_RW_FD)	{	// RW file on remote host
		fd_idx = fd - FAKE_RW_FD;
		
		Op_Msg.op = RF_RW_OP_CLOSE;
		Op_Msg.fd = FileStatus[fd_idx].fd;
		Op_Msg.src_node_idx = *p_myrank;
		Op_Msg.dest_node_idx = FileStatus[fd_idx].host_idx;
		Op_Msg.tid = t_info.tid;

		idx_func_result = t_info.tid % N_USER_FUNC_RESULT;
		p_Func_Result = &(p_shm_User_Func_Result[idx_func_result]);
		
		if (pthread_mutex_lock(&(p_User_Func_Result_lock[idx_func_result])) != 0) {// blocking operation!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			perror("pthread_mutex_lock");
			exit(2);
		}
		
		p_Func_Result->status = FUNC_RESULT_STATUS_INVALID;	// server will update the status
		enqueue_RW_File_Op(&Op_Msg);	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		Wait_for_Func_Result(&(p_Func_Result->status), FUNC_RESULT_STATUS_READY);
		myerrno = p_Func_Result->func_errno;
		ret = p_Func_Result->ret_value;
		p_Func_Result->status = FUNC_RESULT_STATUS_COMPLETE_SUCCESS;	// Already got the result and errno. Time to release the lock
		Wait_for_Func_Result(&(p_Func_Result->status), FUNC_RESULT_STATUS_BARRIER);
		if (pthread_mutex_unlock(&(p_User_Func_Result_lock[idx_func_result])) != 0) {
			perror("pthread_mutex_unlock");
			exit(2);
		}
		if(ret < 0)	errno = myerrno;	// Set errno only if call fails
		FileStatus[fd_idx].pData = 0;	// recylce fd
		return ret;
	}
	else if(fd >= FAKE_FD)	{	// already in buffer
		fd_idx = fd - FAKE_FD;
//		printf("To push close into queue.\n");
		if(p_MetaData[FileStatus[fd_idx].idx].idx_fs == *p_myrank)	{	// local

			free(FileStatus[fd_idx].pData); // !!!!!!!!!!!!!!!!!!!!!!!!!
			
			FileStatus[fd_idx].pData = 0;	// recycle this fd
		}
		else	{	// remote file
			if( (FileStatus[fd_idx].pData) && (fd_idx<MAX_OPENED_FILE) ) enqueue(FileStatus[fd_idx].idx, RF_RO_OP_CLOSE, -1);
			FileStatus[fd_idx].pData = 0;	// recycle this fd
		}
		return 0;	// success!!
	}
	else	{
		ret = real_close_nocancel(fd);
		if(bDbg) printf("DBG> In my close_nocancel(). fd = %d  ret = %d errno = %d\n", fd, ret, errno);
		return ret;
	}
*/
	return real_close_nocancel(fd);
}

ssize_t stripe_read(int fd, char *szFileName, void *buf, size_t size, off_t offset, int idx_fs)
{
	// struct ibv_mr *mr_loc_buf=NULL;
	ucp_mem_h mr_loc_buf = NULL;
	size_t nBytesRead=0, nBytesToRead=size, nBytesReadOneTime;
	off_t offset_loc=offset;
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int idx_fs_shift, residue, idx_fs_data;
	
	pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
	pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;

	while(1)	{
		mr_loc_buf = NULL;
		idx_fs_shift = offset_loc / FILE_STRIPE_SIZE;
		residue = offset_loc % FILE_STRIPE_SIZE;
		nBytesReadOneTime = min(nBytesToRead, FILE_STRIPE_SIZE-residue);
		idx_fs_data = (idx_fs + idx_fs_shift) % pUCXFileServerList->nFSServer;

		// Setup_QP_if_Needed(idx_fs_data);
		Setup_UCX_if_Needed(idx_fs_data);
		pIO_Cmd->fd = (idx_fs_data == idx_fs) ? fd : (-1);
		if(nBytesReadOneTime > DATA_COPY_THRESHOLD_SIZE)	{
			// mr_loc_buf = CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote((char*)buf+nBytesRead, nBytesReadOneTime);
			// pIO_Cmd->rkey = mr_loc_buf->rkey;
			// pIO_Cmd->rem_buff = mr_loc_buf->addr;
			printf("stripe_read > DATA_COPY_THRESHOLD_SIZE\n");
			CLIENT_UCX::RegisterBuf_RW_Local_Remote((char*)buf+nBytesRead, nBytesReadOneTime, &mr_loc_buf);
			CLIENT_UCX::UCX_Pack_Rkey(mr_loc_buf, pIO_Cmd->rkey_buffer);
			pIO_Cmd->rem_buff = (void*)((char*)buf+nBytesRead);
		}
		else	{
			// pIO_Cmd->rkey = mr_rem->rkey;
			// pIO_Cmd->rem_buff = mr_rem->addr;
			printf("stripe_read <= DATA_COPY_THRESHOLD_SIZE\n");
			CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
			pIO_Cmd->rem_buff = ucx_rem_buff;
		}
		strcpy(pIO_Cmd->szName, szFileName);
		
		pIO_Cmd->mode = idx_fs_shift % (pUCXFileServerList->nFSServer);
		pIO_Cmd->offset = offset_loc;	// always put offset parameter here!!! Same as pread() now. 
		pIO_Cmd->nLen = nBytesReadOneTime;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_READ;
		
		Send_IO_Request(idx_fs_data);
		if(mr_loc_buf) ucp_mem_unmap(CLIENT_UCX::ucp_main_context, mr_loc_buf);
		// if(mr_loc_buf)	ibv_dereg_mr(mr_loc_buf);	// data were written to buf already
		else	{
			if(pResult->ret_value > 0)	memcpy((char*)buf+nBytesRead, (char*)pResult+sizeof(RW_FUNC_RETURN)-sizeof(int), pResult->ret_value);
		}

		if(pResult->ret_value >= 0)	{
			nBytesRead += pResult->ret_value;
			offset_loc += pResult->ret_value;
			nBytesToRead -= pResult->ret_value;
			if( (nBytesToRead <= 0) || (pResult->ret_value == 0) )	{	// finish reading or no more data available
				pResult->ret_value = nBytesRead;
				return nBytesRead;
			}
		}
		else	{	// stop. End of reading!
			pResult->ret_value = nBytesRead;
			errno = pResult->myerrno;
			return pResult->ret_value;
		}		
	}
}

extern "C" ssize_t my_read(int fd, void *buf, size_t size)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_loc_buf=NULL;
	ucp_mem_h mr_loc_buf = NULL;
	size_t nBytesRead;
	int fd_real, fd_Directed, idx_fs, fd_idx;

	if(size == 0)	return 0;	// NO NEED to do anything!!!!

	fd_Directed = Get_Fd_Redirected(fd);

	if(fd_Directed >= FD_FILE_BASE)	{	// regular file
		fd_real = GetFileFD(fd_Directed);
		idx_fs = GetFileFSIdx(fd_Directed);
	
		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		fd_idx = fd_Directed-FD_FILE_BASE;
		pIO_Cmd->fd = fd_real;

//		if( (FileList[fd_idx].offset + size) > FILE_STRIPE_SIZE )	{
			pResult->ret_value = stripe_read(fd_real, FileList[fd_idx].szFileName, buf, size, FileList[fd_idx].offset, idx_fs);
			nBytesRead = pResult->ret_value;
//		}
/*		
		else	{
			if(size > DATA_COPY_THRESHOLD_SIZE)	{
				mr_loc_buf = CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote(buf, size);
				pIO_Cmd->rkey = mr_loc_buf->rkey;
				pIO_Cmd->rem_buff = mr_loc_buf->addr;
				strcpy(pIO_Cmd->szName, FileList[fd_idx].szFileName);
			}
			else	{
				pIO_Cmd->rkey = mr_rem->rkey;
				pIO_Cmd->rem_buff = mr_rem->addr;
				strcpy(pIO_Cmd->szName, FileList[fd_idx].szFileName);
			}

			pIO_Cmd->mode = 0;	// no shift
			pIO_Cmd->offset = FileList[fd_Directed-FD_FILE_BASE].offset;	// always put offset parameter here!!! Same as pread() now. 
			pIO_Cmd->nLen = size;
			pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_READ;
			
			Send_IO_Request(idx_fs);
			nBytesRead = pResult->ret_value;
			
			if(mr_loc_buf)	ibv_dereg_mr(mr_loc_buf);	// data were written to buf already
			else	memcpy(buf, (char*)pResult+sizeof(RW_FUNC_RETURN)-sizeof(int), nBytesRead);
		}
*/		
		if(nBytesRead < 0)	errno = pResult->myerrno;
		else	FileList[fd_Directed-FD_FILE_BASE].offset += nBytesRead;	// update offset
				
		return nBytesRead;
	}
	return real_read(fd, buf, size);
}

extern "C" ssize_t pread(int fd, void *buf, size_t size, off_t offset)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_loc_buf=NULL;
	ucp_mem_h mr_loc_buf = NULL;
	size_t nBytesRead;
	int fd_real, fd_Directed, idx_fs, fd_idx;
	
	if(size == 0)	return 0;	// NO NEED to do anything!!!!

	if(real_pread==NULL)	{
		real_pread = (org_pread)dlsym(RTLD_NEXT, "pread64");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_pread(fd, buf, size, offset);
	}
	fd_Directed = Get_Fd_Redirected(fd);

	if(fd_Directed >= FD_FILE_BASE)	{	// regular file
		fd_real = GetFileFD(fd_Directed);
		idx_fs = GetFileFSIdx(fd_Directed);
		
		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		fd_idx = fd_Directed-FD_FILE_BASE;
		pIO_Cmd->fd = fd_real;

//		if( (offset + size) > FILE_STRIPE_SIZE )	{
			pResult->ret_value = stripe_read(fd_real, FileList[fd_idx].szFileName, buf, size, offset, idx_fs);
			nBytesRead = pResult->ret_value;
//		}
/*
		else	{
			if(size > DATA_COPY_THRESHOLD_SIZE)	{
				mr_loc_buf = CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote(buf, size);
				pIO_Cmd->rkey = mr_loc_buf->rkey;
				pIO_Cmd->rem_buff = mr_loc_buf->addr;
				strcpy(pIO_Cmd->szName, FileList[fd_idx].szFileName);
			}
			else	{
				pIO_Cmd->rkey = mr_rem->rkey;
				pIO_Cmd->rem_buff = mr_rem->addr;
				strcpy(pIO_Cmd->szName, FileList[fd_idx].szFileName);
			}
					
			pIO_Cmd->mode = 0;	// no shift
			pIO_Cmd->offset = offset;	// always put offset parameter here!!! Same as pread() now. 
			pIO_Cmd->nLen = size;
			pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_READ;
			
			Send_IO_Request(idx_fs);
			nBytesRead = pResult->ret_value;

			if(mr_loc_buf)	ibv_dereg_mr(mr_loc_buf);	// data were written to buf already
			else	memcpy(buf, (char*)pResult+sizeof(RW_FUNC_RETURN)-sizeof(int), nBytesRead);
		}
*/		
		if(nBytesRead < 0)	errno = pResult->myerrno;
		else	FileList[fd_Directed-FD_FILE_BASE].offset += nBytesRead;	// update offset
				
		return nBytesRead;
	}
	return real_pread(fd, buf, size, offset);
}
extern "C" ssize_t pread64(int fd, void *buf, size_t size, off_t offset) __attribute__ ( (alias ("pread")) );
extern "C" ssize_t __pread64(int fd, void *buf, size_t size, off_t offset) __attribute__ ( (alias ("pread")) );


ssize_t stripe_write(int fd, char *szFileName, const void *buf, size_t size, off_t offset, int idx_fs)
{
	// struct ibv_mr *mr_loc_buf=NULL;
	ucp_mem_h mr_loc_buf = NULL;
	size_t nBytesWritten=0, nBytesToWrite=size, nBytesWriteOneTime;
	off_t offset_loc=offset;
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int idx_fs_shift, residue, idx_fs_data;
	
	pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
	pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;

	while(1)	{
		mr_loc_buf = NULL;
		idx_fs_shift = offset_loc / FILE_STRIPE_SIZE;
		residue = offset_loc % FILE_STRIPE_SIZE;
		nBytesWriteOneTime = min(nBytesToWrite, FILE_STRIPE_SIZE-residue);
		idx_fs_data = (idx_fs + idx_fs_shift) % pUCXFileServerList->nFSServer;
		// Setup_QP_if_Needed(idx_fs_data);
		Setup_UCX_if_Needed(idx_fs_data);

		pIO_Cmd->fd = (idx_fs_data == idx_fs) ? fd : (-1);
		if(nBytesWriteOneTime > DATA_COPY_THRESHOLD_SIZE)	{
			// mr_loc_buf = CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote((char*)buf+nBytesWritten, nBytesWriteOneTime);
			// pIO_Cmd->rkey = mr_loc_buf->rkey;
			// pIO_Cmd->rem_buff = mr_loc_buf->addr;
			printf("stripe_write > DATA_COPY_THRESHOLD_SIZE buf:%p offset:%d size:%d\n", buf, nBytesWritten, nBytesWriteOneTime);
			CLIENT_UCX::RegisterBuf_RW_Local_Remote((char*)buf+nBytesWritten, nBytesWriteOneTime, &mr_loc_buf);
			CLIENT_UCX::UCX_Pack_Rkey(mr_loc_buf, pIO_Cmd->rkey_buffer);
			pIO_Cmd->rem_buff = (void*)((char*)buf+nBytesWritten);
			strcpy(pIO_Cmd->szName, szFileName);
		}
		else	{
			// pIO_Cmd->rkey = mr_rem->rkey;
			printf("stripe_write <= DATA_COPY_THRESHOLD_SIZE\n");
			CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
			// pIO_Cmd->rem_buff = mr_rem->addr;
			pIO_Cmd->rem_buff = ucx_rem_buff;
			strcpy(pIO_Cmd->szName, szFileName);
			memcpy((char*)pResult+sizeof(RW_FUNC_RETURN), (char*)buf+nBytesWritten, nBytesWriteOneTime);
		}
		
		pIO_Cmd->mode = idx_fs_shift % (pUCXFileServerList->nFSServer);
		pIO_Cmd->offset = offset_loc;	// always put offset parameter here!!! Same as pread() now. 
		pIO_Cmd->nLen = nBytesWriteOneTime;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_WRITE;
		
		Send_IO_Request(idx_fs_data);
		// if(mr_loc_buf)	ibv_dereg_mr(mr_loc_buf);	// data were written to buf already
		if(mr_loc_buf) ucp_mem_unmap(CLIENT_UCX::ucp_main_context, mr_loc_buf);
		if(pResult->ret_value >= 0)	{
			nBytesWritten += pResult->ret_value;
			offset_loc += pResult->ret_value;
			nBytesToWrite -= pResult->ret_value;
			if( (nBytesToWrite <= 0) || (pResult->ret_value == 0) )	{	// finish reading or no more data available
				pResult->ret_value = nBytesWritten;
				return nBytesWritten;
			}
		}
		else	{	// stop. End of reading!
			pResult->ret_value = nBytesWritten;
			errno = pResult->myerrno;
			return pResult->ret_value;
		}		
	}
}

extern "C" ssize_t my_write(int fd, const void *buf, size_t size)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_loc_buf=NULL;
	ucp_mem_h mr_loc_buf = NULL;
	size_t nBytesWritten;
	int fd_real, fd_Directed, idx_fs, fd_idx;
	struct timespec t_modify;

	if(size == 0)	return 0;	// NO NEED to do anything!!!!

	fd_Directed = Get_Fd_Redirected(fd);

	if(fd_Directed >= FD_FILE_BASE)	{	// regular file
		fd_real = GetFileFD(fd_Directed);
		idx_fs = GetFileFSIdx(fd_Directed);
		
		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		fd_idx = fd_Directed-FD_FILE_BASE;
		pIO_Cmd->fd = fd_real;

//		if( (FileList[fd_idx].offset + size) > FILE_STRIPE_SIZE )	{
			pResult->ret_value = stripe_write(fd_real, FileList[fd_idx].szFileName, buf, size, FileList[fd_idx].offset, idx_fs);
			if( pResult->ret_value > 0 )	{
				if( FileList[fd_idx].MaxDataRange < (FileList[fd_idx].offset + pResult->ret_value) )	{
					FileList[fd_idx].MaxDataRange = FileList[fd_idx].offset + pResult->ret_value;
				}
			}
//		}
/*
		else	{
			if(size > DATA_COPY_THRESHOLD_SIZE)	{
				mr_loc_buf = CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote((void*)buf, size);
				pIO_Cmd->rkey = mr_loc_buf->rkey;
				pIO_Cmd->rem_buff = mr_loc_buf->addr;
				strcpy(pIO_Cmd->szName, FileList[fd_idx].szFileName);
			}
			else	{
				pIO_Cmd->rkey = mr_rem->rkey;
				pIO_Cmd->rem_buff = mr_rem->addr;
				strcpy(pIO_Cmd->szName, FileList[fd_idx].szFileName);
				
				memcpy((char*)pResult+sizeof(RW_FUNC_RETURN), buf, size);
			}
			
			pIO_Cmd->mode = 0;	// no shift
			pIO_Cmd->offset = FileList[fd_idx].offset;	// always put offset parameter here!!! 
			pIO_Cmd->nLen = size;
			pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_WRITE;
			
			Send_IO_Request(idx_fs);
			if(mr_loc_buf)	ibv_dereg_mr(mr_loc_buf);	// data were written to buf already

			if( pResult->ret_value > 0 )	{
				if( FileList[fd_idx].MaxDataRange < (FileList[fd_idx].offset + pResult->ret_value) )	{
					FileList[fd_idx].MaxDataRange = FileList[fd_idx].offset + pResult->ret_value;
				}
			}
		}
*/
		nBytesWritten = pResult->ret_value;
		if(nBytesWritten < 0)	errno = pResult->myerrno;
		else	FileList[fd_idx].offset += nBytesWritten;	// update offset

		if(nBytesWritten > 0)	{
			clock_gettime(CLOCK_REALTIME, &t_modify);
			FileList[fd_idx].modification_time.tv_sec = t_modify.tv_sec;
			FileList[fd_idx].modification_time.tv_nsec = t_modify.tv_nsec;
		}
		
		return nBytesWritten;
	}

	return real_write(fd, buf, size);
}

extern "C" ssize_t pwrite(int fd, const void *buf, size_t size, off_t offset)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_loc_buf=NULL;
	ucp_mem_h mr_loc_buf = NULL;
	size_t nBytesWritten;
	int fd_real, fd_Directed, idx_fs, fd_idx;
	struct timespec t_modify;

	if(size == 0)	return 0;	// NO NEED to do anything!!!!

	if(real_pwrite==NULL)	{
		real_pwrite = (org_pwrite)dlsym(RTLD_NEXT, "pwrite64");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_pwrite(fd, buf, size, offset);
	}

	fd_Directed = Get_Fd_Redirected(fd);

	if(fd_Directed >= FD_FILE_BASE)	{	// regular file
		fd_real = GetFileFD(fd_Directed);
		idx_fs = GetFileFSIdx(fd_Directed);
		
		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		fd_idx = fd_Directed-FD_FILE_BASE;
		pIO_Cmd->fd = fd_real;

//		if( (offset + size) > FILE_STRIPE_SIZE )	{
			pResult->ret_value = stripe_write(fd_real, FileList[fd_idx].szFileName, buf, size, offset, idx_fs);
			if( pResult->ret_value > 0 )	{
				if( FileList[fd_idx].MaxDataRange < (FileList[fd_idx].offset + pResult->ret_value) )	{
					FileList[fd_idx].MaxDataRange = FileList[fd_idx].offset + pResult->ret_value;
				}
			}
//		}
/*
		else	{
			if(size > DATA_COPY_THRESHOLD_SIZE)	{
				mr_loc_buf = CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote((void*)buf, size);
				pIO_Cmd->rkey = mr_loc_buf->rkey;
				pIO_Cmd->rem_buff = mr_loc_buf->addr;
				strcpy(pIO_Cmd->szName, FileList[fd_idx].szFileName);
			}
			else	{
				pIO_Cmd->rkey = mr_rem->rkey;
				pIO_Cmd->rem_buff = mr_rem->addr;
				strcpy(pIO_Cmd->szName, FileList[fd_idx].szFileName);
				
				memcpy((char*)pResult+sizeof(RW_FUNC_RETURN), buf, size);
			}
			
			pIO_Cmd->mode = 0;	// no shift
			pIO_Cmd->offset = offset;	// always put offset parameter here!!! 
			pIO_Cmd->nLen = size;
			pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_WRITE;
			
			Send_IO_Request(idx_fs);
			if(mr_loc_buf)	ibv_dereg_mr(mr_loc_buf);	// data were written to buf already
			
			if( pResult->ret_value > 0 )	{
				if( FileList[fd_idx].MaxDataRange < (FileList[fd_idx].offset + pResult->ret_value) )	{
					FileList[fd_idx].MaxDataRange = FileList[fd_idx].offset + pResult->ret_value;
				}
			}
		}
*/
		nBytesWritten = pResult->ret_value;
		if(nBytesWritten < 0)	errno = pResult->myerrno;
		else	FileList[fd_idx].offset += nBytesWritten;	// update offset

		if(nBytesWritten > 0)	{
			clock_gettime(CLOCK_REALTIME, &t_modify);
			FileList[fd_idx].modification_time.tv_sec = t_modify.tv_sec;
			FileList[fd_idx].modification_time.tv_nsec = t_modify.tv_nsec;
		}
		
		return nBytesWritten;
	}

	return real_pwrite(fd, buf, size, offset);
}
extern "C" ssize_t pwrite64(int fd, const void *buf, size_t size, off_t offset) __attribute__ ( (alias ("pwrite")) );
extern "C" ssize_t __pwrite64(int fd, const void *buf, size_t size, off_t offset) __attribute__ ( (alias ("pwrite")) );

extern "C" off_t my_lseek(int fd, off_t offset, int whence)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, fd_real, fd_Directed, idx_fs, idx_fd;
	off_t new_offset;

	fd_Directed = Get_Fd_Redirected(fd);
	
	if(fd_Directed >= FD_FILE_BASE)	{
		fd_real = GetFileFD(fd_Directed);
		idx_fs = GetFileFSIdx(fd_Directed);
		idx_fd = fd_Directed-FD_FILE_BASE;
		
		switch(whence)	{
		case SEEK_SET:
			new_offset = offset;
			break;
		case SEEK_CUR:
			new_offset = FileList[idx_fd].offset + offset;
			break;
		case SEEK_END:	// There is NO reliable ways to accurately estimate the current file size. Have to execute it now!!!
/*
			pIO_Cmd = (IO_CMD_MSG *)loc_buff;
			pResult = (RW_FUNC_RETURN *)rem_buff;
			pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 

			pIO_Cmd->fd = fd_real;
			pIO_Cmd->rkey = mr_rem->rkey;
			pIO_Cmd->rem_buff = mr_rem->addr;
			pIO_Cmd->flag = whence;
			pIO_Cmd->offset = offset;
//			pIO_Cmd->nTokenNeeded = 0;
			pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_SEEK;
			Send_IO_Request(idx_fs);
*/
			struct stat fstat;
			ret = my_fxstat(1, fd, &fstat);		
			if(ret != 0)	return (-1);
			else	{
				FileList[idx_fd].offset = fstat.st_size + offset;
				return (FileList[idx_fd].offset);
			}
			break;
		default:
			printf("ERROR> Unknown parameter for lseek(). (%d, %td, %d)\n", fd, offset, whence);
			errno = EINVAL;
			return (-1);
//			break;
		}

		if(new_offset < 0)	{
			errno = EINVAL;
			return (-1);
		}
		else	{
			FileList[idx_fd].offset = new_offset;
			return new_offset;
		}

	}

	return real_lseek(fd, offset, whence);
}

extern "C" int dup2(int oldfd, int newfd)
{
	int i, fd_Directed, idx_io_redirect, pid;
	unsigned long long fn_hash;
	char szExeName[64];

	if(real_dup2==NULL)	{
		real_dup2 = (org_dup2)dlsym(RTLD_NEXT, "__dup2");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_dup2(oldfd, newfd);
	}

	if( (oldfd >= FD_FILE_BASE) && (newfd < 3) )	{	// 0, 1, 2
		Get_Exe_Name(szExeName);
		if( (strcmp(szExeName, "bash")==0) || (strcmp(szExeName, "sh")==0) || (strcmp(szExeName, "csh")==0)  || (strcmp(szExeName, "tcsh")==0) )	{	// very likely a new process created by shell!!!
			pid = getpid();
			idx_io_redirect = pHT_IO_Redirect_UCX->DictSearch(pid, &elt_list_IO_Redirect_UCX, &ht_table_IO_Redirect_UCX, &fn_hash);
			if(idx_io_redirect < 0)	{	// NOT existing!
				pthread_mutex_lock(&(pUCXFileServerList->lock_IO_Redirect));
				idx_io_redirect = pHT_IO_Redirect_UCX->DictInsertAuto(pid, &elt_list_IO_Redirect_UCX, &ht_table_IO_Redirect_UCX);
				pthread_mutex_unlock(&(pUCXFileServerList->lock_IO_Redirect));
			}
			assert(idx_io_redirect >= 0);
			
			if(newfd == 0)	{	// stdin
				strcpy(pIO_Redirect_List_UCX[idx_io_redirect].fNameStdin, FileList[oldfd-FD_FILE_BASE].szFileName);
			}
			else if(newfd == 1)	{	// stdout
				strcpy(pIO_Redirect_List_UCX[idx_io_redirect].fNameStdout, FileList[oldfd-FD_FILE_BASE].szFileName);
			}
			else if(newfd == 2)	{	// stdout
				strcpy(pIO_Redirect_List_UCX[idx_io_redirect].fNameStderr, FileList[oldfd-FD_FILE_BASE].szFileName);
			}
		}
	}

	fd_Directed = Get_Fd_Redirected(oldfd);

	for(i=0; i<MAX_FD_DUP2ED; i++)	{
		if( (Fd_Dup2_List[i].fd_dest != oldfd) && (Fd_Dup2_List[i].fd_src==newfd) && (Fd_Dup2_List[i].Closed == 0) )	{	// dup2 again
			Fd_Dup2_List[i].Closed = 1;
			close(Fd_Dup2_List[i].fd_dest);	// close previous opened fd
			Fd_Dup2_List[i].fd_src = -1;
			Fd_Dup2_List[i].fd_dest = -1;
		}
	}

	if( (fd_Directed>=FD_FILE_BASE) && (newfd<FD_FILE_BASE) )	{
		if(nFD_Dup2ed >= MAX_FD_DUP2ED)	{
			printf("ERROR: nFD_Dup2ed >= MAX_FD_DUP2ED\n");
			errno = EBADF;
			return -1;
		}
		else	{
			for(i=0; i<MAX_FD_DUP2ED; i++)	{
				if(Fd_Dup2_List[i].fd_src == -1)	{	// available
					Fd_Dup2_List[i].fd_src = newfd;
					Fd_Dup2_List[i].fd_dest = fd_Directed;
					Fd_Dup2_List[i].Closed = 0;
					nFD_Dup2ed++;
					return newfd;
				}
			}
		}
	}
	else if( (fd_Directed == newfd) && (fd_Directed>=FD_FILE_BASE) )	{
		return newfd;
	}
	else	return real_dup2(oldfd, newfd);
}
extern "C" int __dup2(int oldfd, int newfd) __attribute__ ( (alias ("dup2")) );

extern "C" int my_unlink(const char *pathname)
{
	char szFullPath[MAX_FILE_NAME_LEN];
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	unsigned long long fn_hash;
	int idx_fs;

	Standardlize_Path(pathname, szFullPath);
	
	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{
		if(ucx_loc_buff == NULL)	Allocate_ucx_loc_rem_buff();

		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		Gather_FileName_Info(szFullPath, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), NULL, &idx_fs);
		pIO_Cmd->parent_dir_hash = 0;
//		idx_fs = pIO_Cmd->file_hash % pFileServerList->nFSServer;	// the index of which file server holding this file

		// Setup_QP_if_Needed(idx_fs);
		Setup_UCX_if_Needed(idx_fs);
//		if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
//			pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
//			pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
//			fetch_and_add(&nQp, 1);	// atomically add the counter
//		}
		
//		printf("DBG> unlink(%s) idx_fs = %d\n", szFullPath, idx_fs);
		
		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		// pIO_Cmd->rkey = mr_rem->rkey;
		CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
		// pIO_Cmd->rem_buff = mr_rem->addr;
		pIO_Cmd->rem_buff = ucx_rem_buff;
		strcpy(pIO_Cmd->szName, szFullPath);
//		pIO_Cmd->nTokenNeeded = 0;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_REMOVE_FILE;
		Send_IO_Request(idx_fs);
		return ((pResult->ret_value) & 0xFFFFFFFF);
	}

	return real_unlink(pathname);
}


extern "C" int my_xstat(int __ver, const char *__filename, struct stat *__stat_buf)
//extern "C" int xstat(int __ver, const char *__filename, struct stat *__stat_buf)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, idx_fs=0;
	char szFullPath[MAX_FILE_NAME_LEN];
	unsigned long int ino_idx_fs;

	if(Inited == 0) {       // init() not finished yet
		return real_xstat(__ver, __filename, __stat_buf);
	}

	Standardlize_Path((char *)__filename, szFullPath);
	
	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{
		if(ucx_loc_buff == NULL)	Allocate_ucx_loc_rem_buff();

		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		Gather_FileName_Info(szFullPath, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), NULL, &idx_fs);
//		idx_fs = pIO_Cmd->file_hash % pFileServerList->nFSServer;	// the index of which file server holding this file		
//		char szHostName[128];
//		int flag = 1;
//		if(strncmp(szFullPath,"/myfs/bert/input/books_wiki_en_corpus",37)==0)	{
//			gethostname(szHostName, 63);
//			printf("DBG> xstat(%s) on host %s pid = %d tid = %d\n", szFullPath, szHostName, getpid(), syscall(SYS_gettid));
//			fflush(stdout);
//			while(flag)	{
//				sleep(1);
//			}
//		}

		// Setup_QP_if_Needed(idx_fs);
		Setup_UCX_if_Needed(idx_fs);
//		if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
//			pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
//			pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
//			fetch_and_add(&nQp, 1);	// atomically add the counter
//		}
		
		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		// pIO_Cmd->rkey = mr_rem->rkey;
		// pIO_Cmd->rem_buff = mr_rem->addr;
		CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
		pIO_Cmd->rem_buff = ucx_rem_buff;
		strcpy(pIO_Cmd->szName, szFullPath);
//		pIO_Cmd->nTokenNeeded = 0;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_STAT;
		Send_IO_Request(idx_fs);
		
		ret = (pResult->ret_value) & 0xFFFFFFFF;
		if(ret < 0)	{
			errno = pResult->myerrno;
			return ret;
		}
//		pStatFile = (struct stat *)((char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int));
		memcpy(__stat_buf, (char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int), sizeof(struct stat));
		ino_idx_fs = idx_fs;
		ino_idx_fs = ino_idx_fs << 32;
		__stat_buf->st_ino = __stat_buf->st_ino | ino_idx_fs;
		__stat_buf->st_blksize = DEFAULT_FILE_STAT_BLOCKSIZE;
		return ret;
	}

	return real_xstat(__ver, __filename, __stat_buf);
}
//extern "C" int __xstaT64(int __ver, const char *__filename, struct stat *__stat_buf) __attribute__ ( (alias ("xstat")) );

extern "C" int lxstat(int __ver, const char *__filename, struct stat *__stat_buf)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
//	struct stat *pStatFile;
	int ret, idx_fs=0;
	char szFullPath[MAX_FILE_NAME_LEN];
	unsigned long int ino_idx_fs;

	if(real_lxstat==NULL)	{
		real_lxstat = (org_lxstat)dlsym(RTLD_NEXT, "__lxstat64");
		assert(real_lxstat != NULL);
	}

	if(Inited == 0) {       // init() not finished yet
		return real_lxstat(__ver, __filename, __stat_buf);
	}

	Standardlize_Path(__filename, szFullPath);
	
	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{
		if(ucx_loc_buff == NULL)	Allocate_ucx_loc_rem_buff();

		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		Gather_FileName_Info(szFullPath, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), NULL, &idx_fs);
//		idx_fs = pIO_Cmd->file_hash % pFileServerList->nFSServer;	// the index of which file server holding this file		
		
		// Setup_QP_if_Needed(idx_fs);
		Setup_UCX_if_Needed(idx_fs);
//		if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
//			pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
//			pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
//			fetch_and_add(&nQp, 1);	// atomically add the counter
//		}

		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		// pIO_Cmd->rkey = mr_rem->rkey;
		// pIO_Cmd->rem_buff = mr_rem->addr;
		CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
		pIO_Cmd->rem_buff = ucx_rem_buff;
		strcpy(pIO_Cmd->szName, szFullPath);
//		pIO_Cmd->nTokenNeeded = 0;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_LSTAT;	// NEED work on server side!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		Send_IO_Request(idx_fs);
		
		ret = (pResult->ret_value) & 0xFFFFFFFF;
		if(ret < 0)	{
			errno = pResult->myerrno;
			return ret;
		}
//		pStatFile = (struct stat *)((char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int));
		memcpy(__stat_buf, (char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int), sizeof(struct stat));
		ino_idx_fs = idx_fs;
		ino_idx_fs = ino_idx_fs << 32;
		__stat_buf->st_ino = __stat_buf->st_ino | ino_idx_fs;
		__stat_buf->st_blksize = DEFAULT_FILE_STAT_BLOCKSIZE;
		
		return ret;
	}

	return real_lxstat(__ver, __filename, __stat_buf);
}
extern "C" int _lxstat(int __ver, const char *__filename, struct stat *__stat_buf) __attribute__ ( (alias ("lxstat")) );
extern "C" int __lxstat(int __ver, const char *__filename, struct stat *__stat_buf) __attribute__ ( (alias ("lxstat")) );
extern "C" int ___lxstat(int __ver, const char *__filename, struct stat *__stat_buf) __attribute__ ( (alias ("lxstat")) );
extern "C" int __lxstaT64(int __ver, const char *__filename, struct stat *__stat_buf) __attribute__ ( (alias ("lxstat")) );

extern "C" int my_fxstat(int vers, int fd, struct stat *buf)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, fd_real, fd_Directed, idx_fs;
	unsigned long int ino_idx_fs;

	fd_Directed = Get_Fd_Redirected(fd);

	if(fd_Directed < FD_FILE_BASE)	{	// regular file id
		return real_fxstat(vers, fd, buf);
	}
	else if(fd_Directed < FD_DIR_BASE)	{	// my file id
		fd_real = GetFileFD(fd_Directed);
		idx_fs = GetFileFSIdx(fd_Directed);

		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		pIO_Cmd->fd = fd_real;
		// pIO_Cmd->rkey = mr_rem->rkey;
		// pIO_Cmd->rem_buff = mr_rem->addr;
		CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
		pIO_Cmd->rem_buff = ucx_rem_buff;
//		pIO_Cmd->nTokenNeeded = 0;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_FSTAT;
		Send_IO_Request(idx_fs);
		
		ret = (pResult->ret_value) & 0xFFFFFFFF;
		if(ret < 0)	{
			errno = pResult->myerrno;
			return ret;
		}
		memcpy(buf, (char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int), sizeof(struct stat));
		ino_idx_fs = idx_fs;
		ino_idx_fs = ino_idx_fs << 32;
		buf->st_ino = buf->st_ino | ino_idx_fs;
		return ret;
	}
	else	{
		return my_xstat(vers, DirList[fd - FD_DIR_BASE].szPath, buf);
	}	

}

extern "C" int __fxstatat(int ver, int dirfd, const char *filename, struct stat *stat_buf, int flags)
{
	char szFullPath[MAX_FILE_NAME_LEN];

	if(real_fxstatat==NULL)	{
		real_fxstatat = (org_fxstatat)dlsym(RTLD_NEXT, "__fxstatat64");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_fxstatat(ver, dirfd, filename, stat_buf, flags);
	}

	if(dirfd >= FD_DIR_BASE)	{
		sprintf(szFullPath, "%s/%s", DirList[dirfd - FD_DIR_BASE].szPath, filename);
		return my_xstat(ver, szFullPath, stat_buf);
	}
	else if(dirfd == AT_FDCWD)	{
		if(strncmp(filename, MYFS_ROOT_DIR, 5) == 0)	{
			return my_xstat(ver, filename, stat_buf);
		}
		else	{
			return real_fxstatat(ver, dirfd, filename, stat_buf, flags);
		}
	}
	else	return real_fxstatat(ver, dirfd, filename, stat_buf, flags);
}
extern "C" int __fxstaTat64(int ver, int dirfd, const char *filename, struct stat *stat_buf, int flags) __attribute__ ( (alias ("__fxstatat")) );
/*
int rename(const char *OldName, const char *NewName)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, idx_fs=0, i;
	char szFullPathOld[MAX_FILE_NAME_LEN], szFullPathNew[MAX_FILE_NAME_LEN];

	if(real_rename==NULL)	{
		real_rename = (org_rename)dlsym(RTLD_NEXT, "rename");
	}
	if(Inited == 0) {       // init() not finished yet
		return real_rename(OldName, NewName);
	}

	Standardlize_Path(OldName, szFullPathOld);
	Standardlize_Path(NewName, szFullPathNew);
	
	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{
		if(loc_buff == NULL)	Allocate_loc_rem_buff();

		pIO_Cmd = (IO_CMD_MSG *)loc_buff;
		Gather_FileName_Info(szFullPathOld, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), NULL, &idx_fs);
//		idx_fs = pIO_Cmd->file_hash % pFileServerList->nFSServer;	// the index of which file server holding this file		
		
		if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
			pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
			pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
			fetch_and_add(&nQp, 1);	// atomically add the counter
		}

		pResult = (RW_FUNC_RETURN *)rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		pIO_Cmd->rem_buff = pClient_qp[idx_fs]->mr_rem->addr;
		pIO_Cmd->rkey = pClient_qp[idx_fs]->mr_rem->rkey;
		strcpy(pIO_Cmd->szName, szFullPathOld);
		pIO_Cmd->nLen = strlen(szFullPathNew);
		
//		pIO_Cmd->nTokenNeeded = 0;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_OPENDIR;
		Send_IO_Request(idx_fs);
		
		pDir = (MYDIR *)malloc(sizeof(MYDIR) + pResult->ret_value);	// Replace malloc by my own memory pool allocation later!!!
		memcpy((char*)pDir+sizeof(MYDIR), (char*)pNumEntry, pResult->ret_value);

		return (DIR*)pDir;
	}

	return real_rename(OldName, NewName);
}
*/

#define OUT_OF_SPACE	(-111)

ssize_t read_all(int fd, void *buf, size_t count);
ssize_t write_all(int fd, const void *buf, size_t count);

ssize_t read_all(int fd, void *buf, size_t count)
{
	ssize_t ret, nBytes=0;
	char *p_buf=(char*)buf;

	if(fd >= FD_FILE_BASE)	return my_read(fd, buf, count);	// file on /myfs

	while (count != 0 && (ret = read(fd, p_buf, count)) != 0) {
		if (ret == -1) {
			if (errno == EINTR)
				continue;
			perror ("read");
			break;
		}
		nBytes += ret;
		count -= ret;
		p_buf += ret;
	}
	return nBytes;
}

ssize_t write_all(int fd, const void *buf, size_t count)
{
	ssize_t ret, nBytes=0;
	char *p_buf=(char*)buf;

	if(fd >= FD_FILE_BASE)	return my_write(fd, buf, count);	// file on /myfs

	while (count != 0 && (ret = write(fd, p_buf, count)) != 0) {
		if (ret == -1) {
			if (errno == EINTR)	{
				continue;
			}
			else if (errno == ENOSPC)	{	// out of space. Quit immediately!!!
				return OUT_OF_SPACE;
			}

			perror ("write");
			break;
		}
		nBytes += ret;
		count -= ret;
		p_buf += ret;
	}
	return nBytes;
}

#undef OUT_OF_SPACE


extern "C" int rename(const char *OldName, const char *NewName)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, idx_fs=0, i, ret_stat_old, ret_stat_new;
	char szFullPathOld[MAX_FILE_NAME_LEN], szFullPathNew[MAX_FILE_NAME_LEN];
	struct stat fstat_old, fstat_new;
	int Is_Dir_OldFile, Is_Dir_NewFile;
	int Is_OldFile_on_Myfs, Is_NewFile_on_Myfs;
	size_t nChunk, nBytesLeft;
	unsigned char *szFileBuff=NULL;
//	unsigned char szFileBuff[FILE_STRIPE_SIZE];
	int fd_Old, fd_New, nBytesRead, nByteWritten;

	if(real_rename==NULL)	{
		real_rename = (org_rename)dlsym(RTLD_NEXT, "rename");
	}
	if(Inited == 0) {       // init() not finished yet
		return real_rename(OldName, NewName);
	}

	Standardlize_Path(OldName, szFullPathOld);
	Standardlize_Path(NewName, szFullPathNew);

	Is_OldFile_on_Myfs = (strncmp(szFullPathOld, MYFS_ROOT_DIR, 5) == 0) ? (1) : (0);
	Is_NewFile_on_Myfs = (strncmp(szFullPathNew, MYFS_ROOT_DIR, 5) == 0) ? (1) : (0);
	
	if( (Is_OldFile_on_Myfs == 0) && (Is_NewFile_on_Myfs == 0) )	{
		return real_rename(szFullPathOld, szFullPathNew);
	}

	ret_stat_old = stat(szFullPathOld, &fstat_old);
	if(ret_stat_old != 0)	{	// does not exist
		return -1;	// error to get the src file
	}
	Is_Dir_OldFile = ( ( fstat_old.st_mode & S_IFMT) == S_IFDIR ) ? (1) : (0);

	if(Is_Dir_OldFile & Is_OldFile_on_Myfs)	{
		printf("Error> rename dir on /myfs is not implemented yet.\n");
		return (-1);
	}

	ret_stat_new = stat(szFullPathNew, &fstat_new);

	if(ret_stat_new == 0)	{	// destination file exists.
		Is_Dir_NewFile = ( (fstat_new.st_mode & S_IFMT) == S_IFDIR ) ? (1) : (0);
		if(Is_Dir_NewFile)	{
			errno = EISDIR;
			return (-1);
		}
		else	{
			unlink(szFullPathNew);	// remove the destination file if it exists.
		}
	}

	fd_Old = my_open(szFullPathOld, O_RDONLY);
	if(fd_Old < 0)	{
		printf("Error> Fail to open source file %s\n", szFullPathOld);
		return (-1);
	}

	fd_New = my_open(szFullPathNew, O_WRONLY | O_CREAT | O_TRUNC, fstat_old.st_mode);
	if(fd_New < 0)	{
		close(fd_Old);
		printf("Error> Fail to open destination file %s\n", szFullPathNew);
		return (-1);
	}

	szFileBuff = (unsigned char*)malloc(FILE_STRIPE_SIZE);
	assert(szFileBuff != NULL);

	if(fstat_old.st_size > FILE_STRIPE_SIZE)	{
		nChunk = fstat_old.st_size / FILE_STRIPE_SIZE;
		nBytesLeft = fstat_old.st_size % FILE_STRIPE_SIZE;

		for(i=0; i<nChunk; i++)	{
			nBytesRead = read_all(fd_Old, szFileBuff, FILE_STRIPE_SIZE);
			if(nBytesRead != FILE_STRIPE_SIZE)	{
				printf("Error> nBytesRead(%d) != CHUNK_SIZE(%d) during reading file %s\n", nBytesRead, FILE_STRIPE_SIZE, szFullPathOld);
			}
			nByteWritten = write_all(fd_New, szFileBuff, nBytesRead);
			if(nByteWritten != nBytesRead)	{
				printf("Error> nByteWritten(%d) != nBytesRead(%d) during reading file %s\n", nByteWritten, nBytesRead, szFullPathOld);
			}
		}
	}
	else	{
		nBytesLeft = fstat_old.st_size;
	}

	nBytesRead = read_all(fd_Old, szFileBuff, nBytesLeft);
	if(nBytesRead != nBytesLeft)	{
		printf("Error> nBytesRead(%d) != nBytesLeft(%d) during reading file %s\n", nBytesRead, nBytesLeft, szFullPathOld);
	}
	nByteWritten = write_all(fd_New, szFileBuff, nBytesRead);
	if(nByteWritten != nBytesRead)	{
		printf("Error> nByteWritten(%d) != nBytesRead(%d) during reading file %s\n", nByteWritten, nBytesRead, szFullPathOld);
	}

	free(szFileBuff);

	close(fd_Old);
	close(fd_New);

	unlink(szFullPathOld);	// Remove the old file after finish writing the new file. 

	return 0;
}

//int renameat(int olddirfd, const char *oldpath,int newdirfd, const char *newpath);

extern "C" DIR *opendir(const char *szDirName)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	RW_FUNC_RETURN_EXT *pResult_Ext;
	int ret, idx_fs=0, i, *pNumEntry, *pIdx_fs;
//	int ret, idx_fs=0, i, *pNumEntry, *pEntryOffsetList;
	char szFullPath[MAX_FILE_NAME_LEN], *pDirName=NULL;
//	char *pEntryNameBuff;
	MYDIR *pDir=NULL;
	// struct ibv_mr *mr_loc_buf=NULL;
	ucp_mem_h mr_loc_buf = NULL;

	if(real_opendir==NULL)	{
		real_opendir = (org_opendir)dlsym(RTLD_NEXT, "opendir");
	}
	if(Inited == 0) {       // init() not finished yet
		return real_opendir(szDirName);
	}

	Standardlize_Path(szDirName, szFullPath);
	
	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{
		if(ucx_loc_buff == NULL)	Allocate_ucx_loc_rem_buff();

		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		Gather_FileName_Info(szFullPath, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), NULL, &idx_fs);
//		idx_fs = pIO_Cmd->file_hash % pFileServerList->nFSServer;	// the index of which file server holding this file		
		
		// Setup_QP_if_Needed(idx_fs);
		Setup_UCX_if_Needed(idx_fs);
//		if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
//			pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
//			pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
//			fetch_and_add(&nQp, 1);	// atomically add the counter
//		}

		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult_Ext = (RW_FUNC_RETURN_EXT *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		// pIO_Cmd->rkey = mr_rem->rkey;
		// pIO_Cmd->rem_buff = mr_rem->addr;
		CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
		pIO_Cmd->rem_buff = ucx_rem_buff;
		strcpy(pIO_Cmd->szName, szFullPath);
//		pIO_Cmd->nTokenNeeded = 0;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_OPENDIR;
		Send_IO_Request(idx_fs);

		if(pResult->ret_value < 0)	{	// error
			errno = pResult->myerrno;
			return NULL;
		}
				
//		printf("There are %d entries under %s.\n", *pNumEntry, szDirName);
//		for(i=0; i<(*pNumEntry); i++)	{
//			printf(" %d %s\n", i, pEntryNameBuff+pEntryOffsetList[i] + 1);
//		}
//		printf("\n");

		pDir = (MYDIR *)malloc(sizeof(MYDIR) + LEN_DIR_NAME + sizeof(int) + pResult->ret_value);	// Replace malloc by my own memory pool allocation later!!!
		assert(pDir != NULL);
		pDir->fd = DUMMY_FD_DIR;	// All Dir* have same fd. pDir added buffer already contains all entry list!!!
		pDirName = (char*)pDir+sizeof(MYDIR);
		strcpy(pDirName, szFullPath);
		pIdx_fs = (int *)((char*)pDir+offsetof(MYDIR_REAL, idx_fs));
		*pIdx_fs = idx_fs;

		if(pResult->nDataSize < IO_RESULT_BUFFER_SIZE)	{
			pNumEntry = (int*)( (char*)pResult + sizeof(RW_FUNC_RETURN)-sizeof(int) );
			pDir->size = *pNumEntry;
			pDir->offset = 0;	// starting from the first entry
			pDir->size_of_Data = pResult->ret_value;
			memcpy((char*)pDir+offsetof(MYDIR_REAL, nEntries), (char*)pNumEntry, pResult->ret_value);
		}
		else	{
			pDir->size = pResult_Ext->nEntry;
			// mr_loc_buf = CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote((void*)pDir, sizeof(MYDIR) + LEN_DIR_NAME + sizeof(int) + pResult->ret_value);
			CLIENT_UCX::RegisterBuf_RW_Local_Remote((void*)pDir, sizeof(MYDIR) + LEN_DIR_NAME + sizeof(int) + pResult->ret_value, &mr_loc_buf);
			pDir->offset = 0;	// starting from the first entry
			pDir->size_of_Data = pResult->ret_value;
			// pClient_qp[idx_fs]->IB_Get((void*)((char*)pDir+offsetof(MYDIR_REAL, nEntries)), mr_loc_buf->lkey, (void*)(pResult_Ext->addr + sizeof(int)), pResult_Ext->rkey, pResult->ret_value);
			ucp_rkey_h pResult_Ext_rkey;
			ucs_status_t status = ucp_ep_rkey_unpack(pClient_ucx[idx_fs]->server_ep, pResult_Ext->rkey_buffer, &pResult_Ext_rkey);
			pClient_ucx[idx_fs]->UCX_Get((void*)((char*)pDir+offsetof(MYDIR_REAL, nEntries)), (void*)(pResult_Ext->addr + sizeof(int)), pResult_Ext_rkey, pResult->ret_value);
			ucx_loc_buff[0] = 1;
			// pClient_qp[idx_fs]->IB_Put(ucx_loc_buff, mr_loc->lkey, (void*)(pResult_Ext->addr), pResult_Ext->rkey, 1);	// Let the server know transfer is done. 
			pClient_ucx[idx_fs]->UCX_Put(ucx_loc_buff, (void*)(pResult_Ext->addr), pResult_Ext_rkey, 1);
			// ibv_dereg_mr(mr_loc_buf);
			ucp_mem_unmap(CLIENT_UCX::ucp_main_context, mr_loc_buf);
			ucp_rkey_destroy(pResult_Ext_rkey);
		}

		return (DIR*)pDir;
	}

	return real_opendir(szDirName);
}
extern "C" DIR * __opendir(const char *__name) __attribute__ ( (alias ("opendir")) );

__thread dirent one_dirent;

//extern "C" void Request_Dir_Entries_List(DIR *__dirp, long int offset, long int nLen)
//{
//}

extern "C" dirent *readdir(DIR *__dirp)
{
	int *pNumEntry, *pEntryOffsetList, *pIdxMin, *pIdxMax, offset, *pIdx_fs;
	char *pEntryNameBuff, *pDirName;
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	RW_FUNC_RETURN_EXT *pResult_Ext;
	int ret, idx_fs=0;
	// struct ibv_mr *mr_loc_buf=NULL;
	ucp_mem_h mr_loc_buf = NULL;

	if(real_readdir==NULL)	{
		real_readdir = (org_readdir)dlsym(RTLD_NEXT, "readdir");
	}

	if( Inited == 0 ) {       // init() not finished yet
		return real_readdir(__dirp);
	}
	
	if(__dirp == NULL)	{
		printf("__dirp == NULL in readdir().\nQuit\n");
		return NULL;
	}
	else	{
		if(__dirp->fd < FD_DIR_BASE)	return real_readdir(__dirp);

		pDirName = (char*)__dirp + sizeof(MYDIR);
		pIdx_fs = (int*)( (char*)__dirp + offsetof(MYDIR_REAL, idx_fs) );
		pNumEntry = (int*)( (char*)__dirp + offsetof(MYDIR_REAL, nEntries) );
		pIdxMin = (int*)( (char*)__dirp + offsetof(MYDIR_REAL, IdxMin) );
		pIdxMax = (int*)( (char*)__dirp + offsetof(MYDIR_REAL, IdxMax) );

		pEntryOffsetList = (int*)( (char*)__dirp + offsetof(MYDIR_REAL, EntryOffsetList) );
		pEntryNameBuff = (char*)__dirp + offsetof(MYDIR_REAL, EntryOffsetList) + sizeof(int)*( (*pIdxMax) - (*pIdxMin) + 1);

		if(__dirp->offset == 0)	{
				strcpy(one_dirent.d_name, ".");
				__dirp->offset ++;
//				one_dirent.d_ino = count_readdir;
				one_dirent.d_type = DIR_ENT_TYPE_DIR;
				return &one_dirent;
		}
		else if(__dirp->offset == 1)	{
				strcpy(one_dirent.d_name, "..");
				__dirp->offset ++;
//				one_dirent.d_ino = count_readdir;
				one_dirent.d_type = DIR_ENT_TYPE_DIR;
				return &one_dirent;
		}
		else if(__dirp->offset < (__dirp->size + 2) )	{
			offset = __dirp->offset - 2;
			if( (offset >= (*pIdxMin) ) && (offset <= (*pIdxMax)) )	{
				offset -= (*pIdxMin);
				strcpy(one_dirent.d_name, pEntryNameBuff+pEntryOffsetList[offset] + 1);	// skip the d_type stored at the very beginning
				one_dirent.d_type = *((char*)(pEntryNameBuff+pEntryOffsetList[offset]));
				__dirp->offset ++;
				return &one_dirent;
			}
			else	{
				pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
				pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
				pResult_Ext = (RW_FUNC_RETURN_EXT *)ucx_rem_buff;
				pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 

				// pIO_Cmd->rkey = mr_rem->rkey;
				// pIO_Cmd->rem_buff = mr_rem->addr;
				CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
				pIO_Cmd->rem_buff = ucx_rem_buff;
				strcpy(pIO_Cmd->szName, pDirName);
				pIO_Cmd->offset = (*pIdxMax) + 1;
				pIO_Cmd->nLen = ((MYDIR*)__dirp)->size_of_Data;	// size of my buffer. NumEntries, IdxMin, IdxMax, then data follow.  
				pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_READ_DIR_ENTRIES;
				Send_IO_Request(idx_fs);

				if(pResult->ret_value < 0)	{	// error
					errno = pResult->myerrno;
					return NULL;
				}

				if(pResult->nDataSize < IO_RESULT_BUFFER_SIZE)	{
					pNumEntry = (int*)( (char*)pResult + sizeof(RW_FUNC_RETURN)-sizeof(int) );
					memcpy((char*)__dirp+offsetof(MYDIR_REAL, nEntries), (char*)pNumEntry, pResult->ret_value);	// Get the new entries data
				}
				else	{
					// mr_loc_buf = CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote((void*)__dirp, sizeof(MYDIR) + LEN_DIR_NAME + sizeof(int) + pResult->ret_value);
					CLIENT_UCX::RegisterBuf_RW_Local_Remote((void*)__dirp, sizeof(MYDIR) + LEN_DIR_NAME + sizeof(int) + pResult->ret_value, &mr_loc_buf);
					ucp_rkey_h pResult_Ext_rkey;
					ucs_status_t status = ucp_ep_rkey_unpack(pClient_ucx[idx_fs]->server_ep, pResult_Ext->rkey_buffer, &pResult_Ext_rkey);
					// pClient_qp[idx_fs]->IB_Get((void*)((char*)__dirp+offsetof(MYDIR_REAL, nEntries)), mr_loc_buf->lkey, (void*)(pResult_Ext->addr + sizeof(int)), pResult_Ext->rkey, pResult->ret_value);
					pClient_ucx[idx_fs]->UCX_Get((void*)((char*)__dirp+offsetof(MYDIR_REAL, nEntries)), (void*)(pResult_Ext->addr + sizeof(int)), pResult_Ext_rkey, pResult->ret_value);
					ucx_loc_buff[0] = 1;
					// pClient_qp[idx_fs]->IB_Put(ucx_loc_buff, mr_loc->lkey, (void*)(pResult_Ext->addr), pResult_Ext->rkey, 1);
					pClient_ucx[idx_fs]->UCX_Put(ucx_loc_buff, (void*)(pResult_Ext->addr), pResult_Ext_rkey, 1);
					// ibv_dereg_mr(mr_loc_buf);
					ucp_mem_unmap(CLIENT_UCX::ucp_main_context, mr_loc_buf);
					ucp_rkey_destroy(pResult_Ext_rkey);
				}
				pEntryNameBuff = (char*)__dirp + offsetof(MYDIR_REAL, EntryOffsetList) + sizeof(int)*( (*pIdxMax) - (*pIdxMin) + 1);

				if( (offset >= (*pIdxMin) ) && (offset <= (*pIdxMax)) )	{
					offset -= (*pIdxMin);
					strcpy(one_dirent.d_name, pEntryNameBuff+pEntryOffsetList[offset] + 1);	// skip the d_type stored at the very beginning
					one_dirent.d_type = *((char*)(pEntryNameBuff+pEntryOffsetList[offset]));
					__dirp->offset ++;
					return &one_dirent;
				}
				else	{
					printf("DBG> Something wrong in readdir()!\n");
					return NULL;
				}
			}
		}
		else	{
			return NULL;
		}
	}
}
extern "C" dirent * __readdir(DIR *__dirp) __attribute__ ( (alias ("readdir")) );
extern "C" dirent * readdir64(DIR *__dirp) __attribute__ ( (alias ("readdir")) );

extern "C" int closedir(DIR *__dirp)
{
	int idx, fd;

	if(real_closedir==NULL)	{
		real_closedir = (org_closedir)dlsym(RTLD_NEXT, "closedir");
	}

	if( Inited == 0 ) {       // init() not finished yet
		return real_closedir(__dirp);
	}
	
	if(__dirp == NULL)	{
		printf("__dirp == NULL in closedir().\nQuit\n");
		return (-1);
	}
	else	{
		if(__dirp->fd >= FD_DIR_BASE)	{
//			if(DirStatus[fd].Init_by_open == 0)	{	// allocated memory in opendir
//				if(DirStatus[fd].pData) free(DirStatus[fd].pData);
//				DirStatus[fd].pData = 0;	// recycle id
//			}
			free(__dirp);
			return 0;
		}
		else	{
			return real_closedir(__dirp);
		}
	}
}
extern "C" int __closedir(DIR *__dirp) __attribute__ ( (alias ("closedir")) );

extern "C" int access(const char *pathname, int mode)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, idx_fs=0;
	char szFullPath[MAX_FILE_NAME_LEN];

	if(real_access==NULL)	{
		real_access = (org_access)dlsym(RTLD_NEXT, "access");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_access(pathname, mode);
	}

	Standardlize_Path(pathname, szFullPath);
	
	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{	// always allowed at this time. Will call stat() and check permission later!!!!!!!!
		if(ucx_loc_buff == NULL)	Allocate_ucx_loc_rem_buff();

		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		Gather_FileName_Info(szFullPath, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), NULL, &idx_fs);
//		idx_fs = pIO_Cmd->file_hash % pFileServerList->nFSServer;	// the index of which file server holding this file		

		// Setup_QP_if_Needed(idx_fs);
		Setup_UCX_if_Needed(idx_fs);
//		if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
//			pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
//			pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
//			fetch_and_add(&nQp, 1);	// atomically add the counter
//		}
		
		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		// pIO_Cmd->rkey = mr_rem->rkey;
		// pIO_Cmd->rem_buff = mr_rem->addr;
		CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
		pIO_Cmd->rem_buff = ucx_rem_buff;
		strcpy(pIO_Cmd->szName, szFullPath);
//		pIO_Cmd->nTokenNeeded = 0;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_STAT;
		Send_IO_Request(idx_fs);
		
		ret = (pResult->ret_value) & 0xFFFFFFFF;
		if(ret < 0)	{
			errno = pResult->myerrno;
			return ret;
		}
		else	return 0;
	}
	else	{
		return real_access(pathname, mode);
	}
}

extern "C" int faccessat(int dirfd, const char *pathname, int mode, int flag)
{
	char szFullPath[MAX_FILE_NAME_LEN];

	if(real_faccessat==NULL)	{
		real_faccessat = (org_faccessat)dlsym(RTLD_NEXT, "faccessat");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_faccessat(dirfd, pathname, mode, flag);
	}

	if(dirfd >= FD_DIR_BASE)	{
		sprintf(szFullPath, "%s/%s", DirList[dirfd - FD_DIR_BASE].szPath, pathname);
		return access(szFullPath, mode);
	}
	else if(dirfd == AT_FDCWD)	{
		if(strncmp(pathname, MYFS_ROOT_DIR, 5) == 0)	{
			return access(pathname, mode);
		}
		else	{
			return real_faccessat(dirfd, pathname, mode, flag);
		}
	}
	else	return real_faccessat(dirfd, pathname, mode, flag);
}

extern "C" int fallocate(int fd, int mode, off_t offset, off_t len)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, fd_real, fd_Directed, idx_fs;

	if(real_fallocate==NULL)	{
		real_fallocate = (org_fallocate)dlsym(RTLD_NEXT, "fallocate64");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_fallocate(fd, mode, offset, len);
	}
	else	{
		fd_Directed = Get_Fd_Redirected(fd);
		if(fd_Directed < FD_FILE_BASE)	{
			return real_fallocate(fd, mode, offset, len);
		}
		else	{
			if( FileList[fd_Directed - FD_FILE_BASE].MaxDataRange < (offset + len) )	{
				FileList[fd_Directed - FD_FILE_BASE].MaxDataRange = offset + len;
			}
			return 0;	// a relaxed implementation
/*
			printf("ERROR> fallocate() is under construction.\n");
			errno = ENOSYS;
			return (-1);

			fd_real = GetFileFD(fd_Directed);
			idx_fs = GetFileFSIdx(fd_Directed);
			
			pIO_Cmd = (IO_CMD_MSG *)loc_buff;
			pResult = (RW_FUNC_RETURN *)rem_buff;
			pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
			
			pIO_Cmd->fd = fd_real;
			pIO_Cmd->rkey = mr_rem->rkey;
			pIO_Cmd->rem_buff = mr_rem->addr;
			pIO_Cmd->mode = mode;
			pIO_Cmd->offset = offset;
			pIO_Cmd->nLen = len;
//			pIO_Cmd->nTokenNeeded = 0;
			pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_FILE_ALLOCATE;	// NEED server side implementation!!!!!!
			Send_IO_Request(idx_fs);
			
			ret = (pResult->ret_value) & 0xFFFFFFFF;
			if(ret < 0)	errno = pResult->myerrno;
			return ret;
*/
		}
	}
}

extern "C" int chdir(const char *pathname)
{
	int idx_fs, ret;
	char szFullPath[MAX_FILE_NAME_LEN];
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	unsigned long long fn_hash;
	struct stat StatDir;
	
	if(real_chdir == NULL)  {
		real_chdir = (org_chdir)dlsym(RTLD_NEXT, "chdir");
	}
	if( Inited == 0 ) {       // init() not finished yet
		return real_chdir(pathname);
	}

	Standardlize_Path(pathname, szFullPath);
	
	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{
		ret = my_xstat(1, szFullPath, &StatDir);
		if(ret < 0)	{	// error
			return ret;	// errno is set already. 
		}
		else	{	// success in xstat()
			if ( (StatDir.st_mode & S_IFMT) == S_IFDIR	)	{	// It is a directory
				// We need to add the code to check user, group permissions later!!!!!!!!!!!!!!!!!!!!!!!!
				strcpy(szCurDir, szFullPath);
				return ret;
			}
			else	{
				errno = ENOTDIR;
				return (-1);
			}
		}		
	}

	ret = real_chdir(pathname);
	Update_CWD();
	
	return ret;
}

extern "C" int fchdir(int fd)
{
	int ret;
	
	if(real_fchdir == NULL) {
		real_fchdir = (org_fchdir)dlsym(RTLD_NEXT, "fchdir");
	}
	if( Inited == 0 ) {       // init() not finished yet
		return real_fchdir(fd);
	}

	if(fd >= FD_DIR_BASE)	{
		printf("ERROR> Under construction in fchdir().\n");
//		strcpy(szCurDir, DirStatus[fd - FD_DIR_BASE].szPath);	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		return (-1);
	}
	else	{
		ret = real_fchdir(fd);
		Update_CWD();
	}
	
	return ret;
}

extern "C" int fchmod(int fd, mode_t mode)
{
	int fd_Directed;
	if(real_fchmod==NULL)	{
		real_fchmod = (org_fchmod)dlsym(RTLD_NEXT, "fchmod");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_fchmod(fd, mode);
	}
	else	{
		fd_Directed = Get_Fd_Redirected(fd);
		if(fd_Directed >= FD_FILE_BASE)	{	// Always success at this moment. Need change later!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			return 0;
		}
		else	{
			return real_fchmod(fd, mode);
		}
	}
}

extern "C" int fchmodat(int dirfd, const char *pathname, mode_t mode, int flag)
{
	char *p_szName, szFullPath[MAX_FILE_NAME_LEN], szPath[MAX_FILE_NAME_LEN], szFdPath[MAX_FILE_NAME_LEN], *pSubStr;
	
	if(real_fchmodat==NULL)	{
		real_fchmodat = (org_fchmodat)dlsym(RTLD_NEXT, "fchmodat");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_fchmodat(dirfd, pathname, mode, flag);
	}
	else	{
		if(pathname[0] == '/')	{	// absolute path
			p_szName = (char*)pathname;
		}
		else	{	// relative path
			p_szName = szFullPath;
			if(dirfd == AT_FDCWD)	{	// CWD
				sprintf(p_szName, "%s/%s", szCurDir, pathname);
			}
			else if(dirfd >= FD_DIR_BASE)	{
//				sprintf(p_szName, "%s/%s", DirStatus[dirfd-FAKE_RW_DIR_FD].szPath, pathname);
			}
			else	{
				sprintf(szFdPath, "/proc/%d/fd/%d", pid, dirfd);
				readlink(szFdPath, szPath, MAX_FILE_NAME_LEN-1);
				sprintf(p_szName, "%s/%s", szPath, pathname);
			}

		}
		
		if(strstr(p_szName, MYFS_ROOT_DIR))	{	// always success at this moement. Need revise later!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			return 0;
		}
		else	{
			return real_fchmodat(dirfd, p_szName, mode, flag);
		}
	}
}

extern "C" int fcntl(int fd, int cmd,...)
{
	int fd_save, fd_Directed, param, OrgFunc=1, *pBuffSize, fd_dup2ed_Dest=-1, Next_Dirfd, Next_fd, nDataSize;
	va_list arg;

	if(real_fcntl == NULL)  {
		real_fcntl = (org_fcntl)dlsym(RTLD_NEXT, "fcntl");
	}
	fd_Directed = Get_Fd_Redirected(fd);
	fd_save = fd_Directed;

//	if(Inited == 0) {       // init() not finished yet
//		if(two_args)	return real_fcntl(fd, cmd);
//		else	return real_fcntl(fd, cmd, param);
//	}
	
	switch(cmd)     {
	case F_DUPFD:
	case F_DUPFD_CLOEXEC:
	case F_GETFD:
	case F_SETFD:
	case F_SETFL:
	case F_GETFL:
	case F_SETOWN:
	case F_SETSIG:
	case F_SETLEASE:
	case F_NOTIFY:
	case F_SETPIPE_SZ:
	case F_ADD_SEALS:
		va_start (arg, cmd);
		param = va_arg (arg, int);
		va_end (arg);

      if(Inited == 0) {       // init() not finished yet
              return real_fcntl(fd, cmd, param);
      }

		if(cmd == F_GETFL)	{
			if(fd_Directed >= FD_DIR_BASE)	{
				return DirList[fd_Directed - FD_DIR_BASE].OpenFlag;
			}
			else if(fd_Directed >= FD_FILE_BASE)	{
				return FileList[fd_Directed - FD_FILE_BASE].OpenFlag;
			}
			else	return real_fcntl(fd_Directed, cmd);
		}

		fd_dup2ed_Dest = Query_Fd_Forward_Dest(fd_Directed);
		if(fd_dup2ed_Dest >= FD_FILE_BASE)	{
			if( cmd == F_SETFD )	{
				return 0;
			}
			else if( cmd == F_GETFL )	{
				return FileList[fd_dup2ed_Dest - FD_FILE_BASE].OpenFlag;
			}
		}

		if(fd_Directed >= FD_DIR_BASE)	{
			fd_Directed -= FD_DIR_BASE;
			OrgFunc=0;
		}
		else if(fd_Directed >= FD_FILE_BASE)	{
			fd_Directed -= FD_FILE_BASE;
			OrgFunc=0;
		}

		if( (cmd == F_DUPFD) || (cmd == F_DUPFD_CLOEXEC) )	{
			if(fd_save >= FD_DIR_BASE)	{
				if(fd_save == DUMMY_FD_DIR)	{
					printf("ERROR> Unexpected fd == DUMMY_FD_DIR in fcntl(fd, F_DUPFD / F_DUPFD_CLOEXEC)\n");
					return (-1);
				}

				nDataSize = ((MYDIR*)(DirList[fd_Directed].pDir))->size_of_Data;
				MYDIR *pDir = (MYDIR *)malloc(sizeof(MYDIR) + nDataSize);
				assert(pDir != NULL);
				memcpy(pDir, DirList[fd_Directed].pDir, sizeof(MYDIR) + nDataSize);

				Next_Dirfd = Find_Next_Available_dirfd();
				memcpy(&(DirList[Next_Dirfd]), &(DirList[fd_Directed]), sizeof(DIRSTATUS));
				DirList[Next_Dirfd].pDir = (DIR*)pDir;
				
				return (Next_Dirfd + FD_DIR_BASE);
			}
			else if(fd_save >= FD_FILE_BASE)	{
				Next_fd = Find_Next_Available_fd();
				memcpy(&(FileList[Next_fd]), &(FileList[fd_Directed]), sizeof(FILESTATUS));
				return (Next_fd + FD_FILE_BASE);
			}
		}
		else if( (cmd == F_GETFD) || (cmd == F_SETFD) )	{
			if(OrgFunc == 0)	{	// Do nothing
				return 0;
			}
		}
//		else if(cmd == F_GETFL)	{
//		}
		return real_fcntl(fd, cmd, param);
	case F_SETLK:
	case F_SETLKW:
	case F_GETLK:
	case F_OFD_SETLK:
	case F_OFD_SETLKW:
	case F_OFD_GETLK:
	case F_GETOWN_EX:
	case F_SETOWN_EX:
		va_start (arg, cmd);
		param = va_arg (arg, int);
		va_end (arg);

      if(Inited == 0) {       // init() not finished yet
              return real_fcntl(fd, cmd, param);
      }

		return real_fcntl(fd, cmd, param);
	default:
		return real_fcntl(fd, cmd);
	}

	return real_fcntl(fd, cmd);
}
extern "C" int __fcntl(int fd, int cmd,...) __attribute__ ( (alias ("fcntl")) );


int Query_Fd_Forward_Dest(int fd_src)
{
	int i;

	for(i=0; i<MAX_FD_DUP2ED; i++)	{
		if(fd_src == Fd_Dup2_List[i].fd_src)	{
			return Fd_Dup2_List[i].fd_dest;
		}
	}
	return -1;
}

int Query_Fd_Forward_Src(int fd_dest, int *Closed)
{
	int i;

	for(i=0; i<MAX_FD_DUP2ED; i++)	{
		if(fd_dest == Fd_Dup2_List[i].fd_dest)	{
			*Closed = Fd_Dup2_List[i].Closed;
			return Fd_Dup2_List[i].fd_src;
		}
	}
	return -1;
}

void Close_Duped_All_Fd(void)
{
	int i;

	for(i=0; i<MAX_FD_DUP2ED; i++)	{
		if( (Fd_Dup2_List[i].fd_src >= 0) && (Fd_Dup2_List[i].Closed == 0) )	{
			Fd_Dup2_List[i].Closed = 1;
			Fd_Dup2_List[i].fd_src = -1;
			close(Fd_Dup2_List[i].fd_dest);
		}
	}
}

extern "C" DIR *fdopendir(int fd)
{
	int fd_save=fd;
	DIR *p_Dir;

	if(real_fdopendir==NULL)  {
		real_fdopendir = (org_fdopendir)dlsym(RTLD_NEXT, "fdopendir");
	}
	if( Inited == 0 ) {       // init() not finished yet
		return real_fdopendir(fd);
	}

	if(fd < FD_DIR_BASE)	return real_fdopendir(fd);	// normal file
	else if(fd == DUMMY_FD_DIR)	{
		printf("ERROR> Unexpected. fd == DUMMY_FD_DIR in fdopendir().\n");
		return NULL;
	}
	else	{	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		p_Dir = DirList[fd - FD_DIR_BASE].pDir;
		p_Dir->offset = 0;	// starting from the first entry
		return p_Dir;
	}

	return real_fdopendir(fd);
}

extern "C" int fsync(int fd)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, fd_real, fd_Directed, idx_fs;

	if(real_fsync==NULL)	{
		real_fsync = (org_fsync)dlsym(RTLD_NEXT, "fsync");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_fsync(fd);
	}
	fd_Directed = Get_Fd_Redirected(fd);

	if(fd_Directed < FD_FILE_BASE)	{
		return real_fsync(fd_Directed);
	}
	else	{
		return 0;	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

		fd_real = GetFileFD(fd_Directed);
		idx_fs = GetFileFSIdx(fd_Directed);
		
		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		pIO_Cmd->fd = fd_real;
		// pIO_Cmd->rkey = mr_rem->rkey;
		// pIO_Cmd->rem_buff = mr_rem->addr;
		CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
		pIO_Cmd->rem_buff = ucx_rem_buff;
//		pIO_Cmd->nTokenNeeded = 0;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_FSYNC;	// NEED server side implementation!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		Send_IO_Request(idx_fs);
		
		ret = (pResult->ret_value) & 0xFFFFFFFF;
		if(ret < 0)	errno = pResult->myerrno;
		return ret;
	}
}

extern "C" int ftruncate(int fd, off_t length)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, fd_real, fd_Directed, idx_fs;

	if(real_ftruncate==NULL)	{
		real_ftruncate = (org_ftruncate)dlsym(RTLD_NEXT, "ftruncate");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_ftruncate(fd, length);
	}
	else	{
		fd_Directed = Get_Fd_Redirected(fd);
		if(fd_Directed < FD_FILE_BASE)	{
			return real_ftruncate(fd_Directed, length);
		}
		else	{
			fd_real = GetFileFD(fd_Directed);
			idx_fs = GetFileFSIdx(fd_Directed);
			
			pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
			pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
			pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
			
			pIO_Cmd->fd = fd_real;
			// pIO_Cmd->rkey = mr_rem->rkey;
			// pIO_Cmd->rem_buff = mr_rem->addr;
			CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
			pIO_Cmd->rem_buff = ucx_rem_buff;
			pIO_Cmd->nLen = length;
//			pIO_Cmd->nTokenNeeded = 0;
			pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_FTRUNCATE;	// NEED server side implementation!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Finished. Need test!!!!!
			Send_IO_Request(idx_fs);
			
			ret = (pResult->ret_value) & 0xFFFFFFFF;
			if(ret < 0)	errno = pResult->myerrno;
			return ret;
		}
	}
}
extern "C" int ftruncate64(int fd, off_t length) __attribute__ ( (alias ("ftruncate")) );

extern "C" int truncate(const char *path, off_t length)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, idx_fs=0;
	char szFullPath[MAX_FILE_NAME_LEN];

	if(real_truncate==NULL)	{
		real_truncate = (org_truncate)dlsym(RTLD_NEXT, "truncate64");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_truncate(path, length);
	}

	Standardlize_Path(path, szFullPath);

	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{
		if(ucx_loc_buff == NULL)	Allocate_ucx_loc_rem_buff();

		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		Gather_FileName_Info(szFullPath, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), &(pIO_Cmd->parent_dir_hash), &idx_fs);
//		idx_fs = pIO_Cmd->file_hash % pFileServerList->nFSServer;	// the index of which file server holding this file		
		
		// Setup_QP_if_Needed(idx_fs);
		Setup_UCX_if_Needed(idx_fs);
//		if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
//			pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
//			pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
//			fetch_and_add(&nQp, 1);	// atomically add the counter
//		}

		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		// pIO_Cmd->rkey = mr_rem->rkey;
		// pIO_Cmd->rem_buff = mr_rem->addr;
		CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
		pIO_Cmd->rem_buff = ucx_rem_buff;
		strcpy(pIO_Cmd->szName, szFullPath);
		pIO_Cmd->nLen = length;
//		pIO_Cmd->nTokenNeeded = 0;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_TRUNCATE;	// NEED work on server side!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  Finished. Need test!!!!!
		Send_IO_Request(idx_fs);
		
		ret = (pResult->ret_value) & 0xFFFFFFFF;
		if(ret < 0)	{
			errno = pResult->myerrno;
			return ret;
		}
	}

	return real_truncate(path, length);
}
extern "C" int truncate64(const char *path, off_t length) __attribute__ ( (alias ("truncate")) );

extern "C" int futimens(int fd, const struct timespec times[2])
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, fd_real, fd_Directed, idx_fs;
	long int *pDest, *pSrc;
	struct timespec t_spec;
	// sizeof(struct timespec) = 16. Copy the data to &offset. 

	if(real_futimens==NULL)	{
		real_futimens = (org_futimens)dlsym(RTLD_NEXT, "futimens");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_futimens(fd, times);
	}
	else	{
		fd_Directed = Get_Fd_Redirected(fd);
		if(fd_Directed < FD_FILE_BASE)	{
			return real_futimens(fd_Directed, times);
		}
		else	{
			fd_real = GetFileFD(fd_Directed);
			idx_fs = GetFileFSIdx(fd_Directed);
			
			pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
			pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
			pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 

			pIO_Cmd->fd = fd_real;
			// pIO_Cmd->rkey = mr_rem->rkey;
			// pIO_Cmd->rem_buff = mr_rem->addr;
			CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
			pIO_Cmd->rem_buff = ucx_rem_buff;

			pDest = (long int *)(&(pIO_Cmd->nLen_FileName));
			pSrc = (long int *)times;

			if(pSrc == NULL)	{	// set current time stamp
				clock_gettime(CLOCK_REALTIME, &t_spec);
				pSrc = (long int *)(&t_spec);
				pDest[0] = pSrc[0];
				pDest[1] = pSrc[1];
				pDest[2] = pSrc[0];
				pDest[3] = pSrc[1];
			}
			else	{
				pDest[0] = pSrc[0];
				pDest[1] = pSrc[1];
				pDest[2] = pSrc[2];
				pDest[3] = pSrc[3];
			}

//			pIO_Cmd->nTokenNeeded = 0;
			pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_FUTIMENS;	// NEED server side implementation!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			Send_IO_Request(idx_fs);
			
			ret = (pResult->ret_value) & 0xFFFFFFFF;
			if(ret < 0)	errno = pResult->myerrno;
			return ret;
		}
	}
}

extern "C" int utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flag)	// NOT finished !!!!!!
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, fd_real, idx_fs;
	long int *pDest, *pSrc;
	struct timespec t_spec;	// sizeof(struct timespec) = 16. Copy the data to &offset. 
	char szFullPath[MAX_FILE_NAME_LEN], szTmpPath[MAX_FILE_NAME_LEN];

	if(real_utimensat==NULL)	{
		real_utimensat = (org_utimensat)dlsym(RTLD_NEXT, "utimensat");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_utimensat(dirfd, pathname, times, flag);
	}

	if(dirfd >= FD_DIR_BASE)	{
		sprintf(szTmpPath, "%s/%s", DirList[dirfd - FD_DIR_BASE].szPath, pathname);
		Standardlize_Path(szTmpPath, szFullPath);
//		return xstat(ver, szFullPath, stat_buf);
	}
	else if(dirfd == AT_FDCWD)	{
		if(strncmp(pathname, MYFS_ROOT_DIR, 5) == 0)	{
			Standardlize_Path(pathname, szFullPath);
//			return xstat(ver, filename, stat_buf);
		}
		else	{
			return real_utimensat(dirfd, pathname, times, flag);
		}
	}
	else	return real_utimensat(dirfd, pathname, times, flag);
	
	if(ucx_loc_buff == NULL)	Allocate_ucx_loc_rem_buff();
	
	pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
	Gather_FileName_Info(szFullPath, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), NULL, &idx_fs);
//	idx_fs = pIO_Cmd->file_hash % pFileServerList->nFSServer;	// the index of which file server holding this file		
	
	// Setup_QP_if_Needed(idx_fs);
	Setup_UCX_if_Needed(idx_fs);
//	if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
//		pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
//		pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
//		fetch_and_add(&nQp, 1);	// atomically add the counter
//	}
	
	pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
	pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
	
	// pIO_Cmd->rkey = mr_rem->rkey;
	// pIO_Cmd->rem_buff = mr_rem->addr;
	CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
	pIO_Cmd->rem_buff = ucx_rem_buff;
	strcpy(pIO_Cmd->szName, szFullPath);
	
	pDest = (long int *)(&(pIO_Cmd->nLen_FileName));	// use this space to store times[2]
	pSrc = (long int *)times;
	if(pSrc == NULL)	{	// set current time stamp
		clock_gettime(CLOCK_REALTIME, &t_spec);
		pSrc = (long int *)(&t_spec);
		pDest[0] = pSrc[0];
		pDest[1] = pSrc[1];
		pDest[2] = pSrc[0];
		pDest[3] = pSrc[1];
	}
	else	{
		pDest[0] = pSrc[0];
		pDest[1] = pSrc[1];
		pDest[2] = pSrc[2];
		pDest[3] = pSrc[3];
	}
	
//	pIO_Cmd->nTokenNeeded = 0;
	pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_UTIMES;	// NEED server side implementation!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	Send_IO_Request(idx_fs);
	
	ret = (pResult->ret_value) & 0xFFFFFFFF;
	if(ret < 0)	errno = pResult->myerrno;
	return ret;
}

extern "C" int utimes(const char *pathname, const struct timeval times[2])
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, idx_fs=0;
	char szFullPath[MAX_FILE_NAME_LEN];
	long int *pDest, *pSrc;
	struct timespec t_spec;

	if(real_utimes==NULL)	{
		real_utimes = (org_utimes)dlsym(RTLD_NEXT, "utimes");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_utimes(pathname, times);
	}

	Standardlize_Path(pathname, szFullPath);
	
	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{
		if(ucx_loc_buff == NULL)	Allocate_ucx_loc_rem_buff();

		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		Gather_FileName_Info(szFullPath, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), NULL, &idx_fs);
//		idx_fs = pIO_Cmd->file_hash % pFileServerList->nFSServer;	// the index of which file server holding this file		
		
		// Setup_QP_if_Needed(idx_fs);
		Setup_UCX_if_Needed(idx_fs);
//		if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
//			pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
//			pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
//			fetch_and_add(&nQp, 1);	// atomically add the counter
//		}

		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		// pIO_Cmd->rkey = mr_rem->rkey;
		// pIO_Cmd->rem_buff = mr_rem->addr;
		CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
		pIO_Cmd->rem_buff = ucx_rem_buff;
		strcpy(pIO_Cmd->szName, szFullPath);
		
		pDest = (long int *)(&(pIO_Cmd->nLen_FileName));
		pSrc = (long int *)times;
		if(pSrc == NULL)	{	// set current time stamp
			clock_gettime(CLOCK_REALTIME, &t_spec);
			pSrc = (long int *)(&t_spec);
			pDest[0] = pSrc[0];
			pDest[1] = pSrc[1] * 1000;	// convert from us to ns
			pDest[2] = pSrc[0];
			pDest[3] = pSrc[1] * 1000;	// convert from us to ns
		}
		else	{
			pDest[0] = pSrc[0];
			pDest[1] = pSrc[1] * 1000;	// convert from us to ns
			pDest[2] = pSrc[2];
			pDest[3] = pSrc[3] * 1000;	// convert from us to ns
		}
		
//		pIO_Cmd->nTokenNeeded = 0;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_UTIMES;	// NEED work on server side!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		Send_IO_Request(idx_fs);
		
		ret = (pResult->ret_value) & 0xFFFFFFFF;
		if(ret < 0)	{
			errno = pResult->myerrno;
			return ret;
		}
		
		return ret;
	}
	return real_utimes(pathname, times);
}

extern "C" int utime(const char *pathname, const struct utimbuf *times)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, idx_fs=0;
	char szFullPath[MAX_FILE_NAME_LEN];
	long int *pDest, *pSrc;
	struct timespec t_spec;

	if(real_utime==NULL)	{
		real_utime = (org_utime)dlsym(RTLD_NEXT, "utime");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_utime(pathname, times);
	}

	Standardlize_Path(pathname, szFullPath);
	
	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{
		if(ucx_loc_buff == NULL)	Allocate_ucx_loc_rem_buff();

		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		Gather_FileName_Info(szFullPath, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), NULL, &idx_fs);
//		idx_fs = pIO_Cmd->file_hash % pFileServerList->nFSServer;	// the index of which file server holding this file		

		// Setup_QP_if_Needed(idx_fs);
		Setup_UCX_if_Needed(idx_fs);
//		if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
//			pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
//			pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
//			fetch_and_add(&nQp, 1);	// atomically add the counter
//		}

		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		// pIO_Cmd->rkey = mr_rem->rkey;
		// pIO_Cmd->rem_buff = mr_rem->addr;
		CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
		pIO_Cmd->rem_buff = ucx_rem_buff;
		strcpy(pIO_Cmd->szName, szFullPath);
		
		pDest = (long int *)(&(pIO_Cmd->nLen_FileName));
		pSrc = (long int *)times;
		if(pSrc == NULL)	{	// set current time stamp
			clock_gettime(CLOCK_REALTIME, &t_spec);
			pSrc = (long int *)(&t_spec);
			pDest[0] = pSrc[0];
			pDest[1] = 0;
			pDest[2] = pSrc[0];
			pDest[3] = 0;
		}
		else	{
			pDest[0] = pSrc[0];
			pDest[1] = 0;
			pDest[2] = pSrc[1];
			pDest[3] = 0;
		}
		
//		pIO_Cmd->nTokenNeeded = 0;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_UTIMES;	// NEED work on server side!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		Send_IO_Request(idx_fs);
		
		ret = (pResult->ret_value) & 0xFFFFFFFF;
		if(ret < 0)	{
			errno = pResult->myerrno;
			return ret;
		}
		
		return ret;
	}
	return real_utime(pathname, times);
}

extern "C" int unlinkat(int dirfd, const char *pathname, int flags)	// unlink or rmdir
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, idx_fs=0;
	char szFullPath[MAX_FILE_NAME_LEN];

	if(real_unlinkat==NULL)	{
		real_unlinkat = (org_unlinkat)dlsym(RTLD_NEXT, "unlinkat");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_unlinkat(dirfd, pathname, flags);
	}

	if(dirfd >= FD_DIR_BASE)	{
		sprintf(szFullPath, "%s/%s", DirList[dirfd - FD_DIR_BASE].szPath, pathname);
		return my_unlink(szFullPath);
	}
	else if(dirfd == AT_FDCWD)	{
		if(strncmp(pathname, MYFS_ROOT_DIR, 5) == 0)	{
			return my_unlink(pathname);
		}
		else	{
			return real_unlinkat(dirfd, pathname, flags);
		}
	}
	else	return real_unlinkat(dirfd, pathname, flags);

/*
	if(strncmp(p_szName, MYFS_ROOT_DIR, 5) == 0)	{
		if(loc_buff == NULL)	Allocate_loc_rem_buff();

		pIO_Cmd = (IO_CMD_MSG *)loc_buff;
		Gather_FileName_Info(p_szName, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), &(pIO_Cmd->parent_dir_hash), &idx_fs);
//		idx_fs = pIO_Cmd->file_hash % pFileServerList->nFSServer;	// the index of which file server holding this file		
		
		if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
			pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
			pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
			fetch_and_add(&nQp, 1);	// atomically add the counter
		}

		pResult = (RW_FUNC_RETURN *)rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		strcpy(pIO_Cmd->szName, p_szName);
		pIO_Cmd->rem_buff = pClient_qp[idx_fs]->mr_rem->addr;
		pIO_Cmd->rkey = pClient_qp[idx_fs]->mr_rem->rkey;
		
		pIO_Cmd->nTokenNeeded = 0;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_REMOVE_FILE;	// NEED work on server side!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		Send_IO_Request(idx_fs);
		
		ret = (pResult->ret_value) & 0xFFFFFFFF;
		if(ret < 0)	{
			errno = pResult->myerrno;
			return ret;
		}
	}

	return real_unlinkat(dirfd, pathname, flags);
*/

}

extern "C" int isatty(int fd)
{
	int fd_Directed;

	if(real_isatty==NULL)	{
		real_isatty = (org_isatty)dlsym(RTLD_NEXT, "isatty");
		assert(real_isatty != NULL);
	}
	if(Inited == 0) {       // init() not finished yet
		return real_isatty(fd);
	}

	fd_Directed = Get_Fd_Redirected(fd);
	if(fd_Directed >= FD_FILE_BASE)	{
		return 0;	// non-terminal
	}
	else	{
		return real_isatty(fd);
	}
}
extern "C" int __isatty(int fd) __attribute__ ( (alias ("isatty")) );


extern "C" int mkdir(const char *pathname, mode_t mode)
{
	char szFullPath[MAX_FILE_NAME_LEN];
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	unsigned long long fn_hash;
	int ret, idx_fs;

	if(real_mkdir==NULL)	{
		real_mkdir = (org_mkdir)dlsym(RTLD_NEXT, "mkdir");
		assert(real_mkdir != NULL);
	}
	if(Inited == 0) {       // init() not finished yet
		return real_mkdir(pathname, mode);
	}

	Standardlize_Path(pathname, szFullPath);
	
	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{
		if(ucx_loc_buff == NULL)	Allocate_ucx_loc_rem_buff();

		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;

		Gather_FileName_Info(szFullPath, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), &(pIO_Cmd->parent_dir_hash), &idx_fs);	// create a new file
		if( (pIO_Cmd->nLen_FileName - pIO_Cmd->nLen_Parent_Dir_Name - 1) >= MAX_ENTRY_NAME_LEN)	{
			printf("ERROR> The length of entry name >= MAX_ENTRY_NAME_LEN (%d).\n%s\nQuit\n", MAX_ENTRY_NAME_LEN, szFullPath);
			exit(1);
		}
//		idx_fs = pIO_Cmd->file_hash % pFileServerList->nFSServer;	// the index of which file server holding this file
		
		// Setup_QP_if_Needed(idx_fs);
		Setup_UCX_if_Needed(idx_fs);
//		if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
//			pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
//			pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
//			fetch_and_add(&nQp, 1);	// atomically add the counter
//		}

//		printf("DBG> mkdir(%s) idx_fs = %d\n", szFullPath, idx_fs);
		
		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		// pIO_Cmd->rkey = mr_rem->rkey;
		// pIO_Cmd->rem_buff = mr_rem->addr;
		CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
		pIO_Cmd->rem_buff = ucx_rem_buff;
		strcpy(pIO_Cmd->szName, szFullPath);		
		pIO_Cmd->mode = mode;
//		pIO_Cmd->nTokenNeeded = 0;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_MKDIR;	// NEED work on server side!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		Send_IO_Request(idx_fs);
		
		ret = (pResult->ret_value) & 0xFFFFFFFF;
		if(ret < 0)	{
			errno = pResult->myerrno;
			return ret;
		}
		return ret;
	}
	return real_mkdir(pathname, mode);
}

extern "C" int mkdirat(int dirfd, const char *pathname, mode_t mode)
{
	char szFullPath[MAX_FILE_NAME_LEN];

	if(real_mkdirat==NULL)	{
		real_mkdirat = (org_mkdirat)dlsym(RTLD_NEXT, "mkdirat");
		assert(real_mkdirat != NULL);
	}

	if(Inited == 0) {       // init() not finished yet
		return real_mkdirat(dirfd, pathname, mode);
	}

	if(dirfd >= FD_DIR_BASE)	{
		sprintf(szFullPath, "%s/%s", DirList[dirfd - FD_DIR_BASE].szPath, pathname);
		return mkdir(szFullPath, mode);
	}
	else if(dirfd == AT_FDCWD)	{
		if(strncmp(pathname, MYFS_ROOT_DIR, 5) == 0)	{
			return mkdir(pathname, mode);
		}
		else	{
			return real_mkdirat(dirfd, pathname, mode);
		}
	}
	else	return real_mkdirat(dirfd, pathname, mode);
}

/*
inline void Wait_For_IO_Request_Result(int Tag_Magic)
{
	RW_FUNC_RETURN *pResult = (RW_FUNC_RETURN *)rem_buff;
	int *pTag_End, Tag_Ini;

	while(1)	{
		if(pResult->nDataSize > 0)	break;	// waiting for the size of result. 
	}
	Tag_Ini = pResult->Tag_Ini;
	pTag_End =  (int*)((char*)rem_buff + pResult->nDataSize - 4);

	while(1)	{
		if( ( Tag_Ini ^ (*pTag_End) ) == Tag_Magic )	break;	// waiting for the ending tag. 
	}
}
*/

inline int Wait_For_IO_Request_Result(ucp_worker_h data_worker, int Tag_Magic)
{
	RW_FUNC_RETURN *pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
	int *pTag_End;
	struct timeval tm1, tm2;	// tm1.tv_sec
	long int t1_ms, t2_ms;
	char szHostName[128];

	gettimeofday(&tm1, NULL);
	t1_ms = (tm1.tv_sec * 1000) + (tm1.tv_usec / 1000);
	while(1)	{
		ucp_worker_progress(data_worker);
		if(pResult->nDataSize > 0)	break;	// waiting for the size of result. 

		gettimeofday(&tm2, NULL);
		t2_ms = (tm2.tv_sec * 1000) + (tm2.tv_usec / 1000);
		if( (t2_ms - t1_ms) > UCX_WAIT_RESULT_TIMEOUT_MS )	{
			gethostname(szHostName, 63);
			printf("DBG> Timeout[nDataSize]. pid = %d on %s\n", getpid(), szHostName);
			fflush(stdout);
			sleep(1);
			return 1;	// time out
		}
	}
//	Tag_Ini = pResult->Tag_Ini;
//	if( pResult->nDataSize > IO_RESULT_BUFFER_SIZE )	{
//		pTag_End =  (int*)((char*)rem_buff + sizeof(RW_FUNC_RETURN_EXT) - sizeof(int) );
//	}
//	else	{
//		pTag_End =  (int*)((char*)rem_buff + pResult->nDataSize - 4);
//	}

	while(1)	{
		ucp_worker_progress(data_worker);
	    if( pResult->nDataSize > IO_RESULT_BUFFER_SIZE )        {
                pTag_End =  (int*)((char*)ucx_rem_buff + sizeof(RW_FUNC_RETURN_EXT) - sizeof(int) );
        }
	    else    {
                pTag_End =  (int*)((char*)ucx_rem_buff + pResult->nDataSize - 4);
	    }

		if( ( (pResult->Tag_Ini) ^ (*pTag_End) ) == Tag_Magic )	break;	// waiting for the ending tag. 

		gettimeofday(&tm2, NULL);
		t2_ms = (tm2.tv_sec * 1000) + (tm2.tv_usec / 1000);
		if( (t2_ms - t1_ms) > UCX_WAIT_RESULT_TIMEOUT_MS )	{
			gethostname(szHostName, 63);
			printf("DBG> Timeout[endTage]. pid = %d on %s\n", getpid(), szHostName);
                        fflush(stdout);
                        sleep(300);

			return 1;	// time out
		}
	}
	return 0;
}

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
extern "C" int openat(int dirfd, const char *pathname, int oflags, ...)
{
	int mode = 0, two_args=1, fd, idx_fs;
	char *p_szDirEntryName=NULL;
	char szFullPath[MAX_FILE_NAME_LEN];
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	unsigned long long fn_hash;

	if (oflags & O_CREAT)	{
		va_list arg;
		va_start (arg, oflags);
		mode = va_arg (arg, int);
		va_end (arg);
		two_args=0;
	}

	if(real_openat==NULL)	{
		real_openat = (org_openat)dlsym(RTLD_NEXT, "openat");
		assert(real_openat != NULL);
	}
	if(Inited == 0) {       // init() not finished yet
		if(two_args)    {
			return real_openat(dirfd, pathname, oflags);
		}
		else    {
			return real_openat(dirfd, pathname, oflags, mode);
		}
	}

	if( dirfd >= FD_DIR_BASE )	{
		sprintf(szFullPath, "%s/%s", DirList[dirfd - FD_DIR_BASE].szPath, pathname);

		if(two_args)    {
			return my_open(szFullPath, oflags);
		}
		else	{
			return my_open(szFullPath, oflags, mode);
		}
	}
	else if( dirfd == AT_FDCWD )	{
		if(strncmp(pathname, MYFS_ROOT_DIR, 5) == 0)	{
			if(two_args)    {
				return my_open(pathname, oflags);
			}
			else	{
				return my_open(pathname, oflags, mode);
			}
		}
		else	{
			if(two_args)    {
				return real_openat(dirfd, pathname, oflags);
			}
			else    {
				return real_openat(dirfd, pathname, oflags, mode);
			}
		}
	}
	else	{
		if(two_args)    {
			return real_openat(dirfd, pathname, oflags);
		}
		else    {
			return real_openat(dirfd, pathname, oflags, mode);
		}
	}
}
extern "C" int openaT64(int dirfd, const char *pathname, int flags, ...) __attribute__ ( (alias ("openat")) );

extern "C" int	__openat_2 (int dirfd, const char *pathname, int oflags)
{
	int mode = 0, two_args=1, fd, idx_fs;
	char *p_szDirEntryName=NULL;
	char szFullPath[MAX_FILE_NAME_LEN];
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	unsigned long long fn_hash;

	if(real_openat_2==NULL)	{
		real_openat_2 = (org_openat_2)dlsym(RTLD_NEXT, "__openat_2");
		assert(real_openat_2 != NULL);
	}
	if(Inited == 0) {       // init() not finished yet
		return real_openat_2(dirfd, pathname, oflags);
	}

	if( dirfd >= FD_DIR_BASE )	{
		sprintf(szFullPath, "%s/%s", DirList[dirfd - FD_DIR_BASE].szPath, pathname);
		return my_open(szFullPath, oflags);
	}
	else if( dirfd == AT_FDCWD )	{
		if(strncmp(pathname, MYFS_ROOT_DIR, 5) == 0)	{
			return my_open(pathname, oflags);
		}
		else	{
			return real_openat_2(dirfd, pathname, oflags);
		}
	}
	else	{
		return real_openat_2(dirfd, pathname, oflags);
	}
}

extern "C" int posix_fadvise(int fd, off_t offset, off_t len, int advice)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, fd_real, fd_Directed, idx_fs;

	if(real_posix_fadvise==NULL)	{
		real_posix_fadvise = (org_posix_fadvise)dlsym(RTLD_NEXT, "posix_fadvise");
	}

	fd_Directed = Get_Fd_Redirected(fd);
	if(fd_Directed >= FD_FILE_BASE)	{	// Do nothing!!!
		return 0;
	}

	return real_posix_fadvise(fd, offset, len, advice);
}
extern "C" int posix_fadvise64(int fd, off_t offset, off_t len, int advice) __attribute__ ( (alias ("posix_fadvise")) );

extern "C" int posix_fallocate(int fd, off_t offset, off_t len)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, fd_real, fd_Directed, idx_fs;

	if(real_posix_fallocate==NULL)	{
		real_posix_fallocate = (org_posix_fallocate)dlsym(RTLD_NEXT, "posix_fallocate64");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_posix_fallocate(fd, offset, len);
	}
	else	{
		fd_Directed = Get_Fd_Redirected(fd);

		if(fd_Directed < FD_FILE_BASE)	{
			return real_posix_fallocate(fd, offset, len);
		}
		else	{
			if( FileList[fd_Directed - FD_FILE_BASE].MaxDataRange < (offset + len) )	{
				FileList[fd_Directed - FD_FILE_BASE].MaxDataRange = offset + len;
			}
			return 0;	// a relaxed implementation

/*
			fd_real = GetFileFD(fd_Directed);
			idx_fs = GetFileFSIdx(fd_Directed);
			
			pIO_Cmd = (IO_CMD_MSG *)loc_buff;
			pResult = (RW_FUNC_RETURN *)rem_buff;
			pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
			
			pIO_Cmd->fd = fd_real;
			pIO_Cmd->rkey = mr_rem->rkey;
			pIO_Cmd->rem_buff = mr_rem->addr;
			pIO_Cmd->offset = offset;
			pIO_Cmd->nLen = len;
//			pIO_Cmd->nTokenNeeded = 0;
			pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_FILE_ALLOCATE;	// NEED server side implementation!!!!!!
			Send_IO_Request(idx_fs);
			
			ret = (pResult->ret_value) & 0xFFFFFFFF;
			if(ret < 0)	errno = pResult->myerrno;
			return ret;
*/			
		}
	}
}

extern "C" int rmdir(const char *path)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, idx_fs=0;
	char szFullPath[MAX_FILE_NAME_LEN];

	if(real_rmdir==NULL)	{
		real_rmdir = (org_rmdir)dlsym(RTLD_NEXT, "rmdir");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_rmdir(path);
	}

	Standardlize_Path(path, szFullPath);
//	printf("DBG> rmdir(%s)\n", szFullPath);

	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{
		if(ucx_loc_buff == NULL)	Allocate_ucx_loc_rem_buff();

		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		Gather_FileName_Info(szFullPath, &(pIO_Cmd->nLen_FileName), &(pIO_Cmd->nLen_Parent_Dir_Name), &(pIO_Cmd->file_hash), &(pIO_Cmd->parent_dir_hash), &idx_fs);
//		idx_fs = pIO_Cmd->file_hash % pFileServerList->nFSServer;	// the index of which file server holding this file		
		// Setup_QP_if_Needed(idx_fs);
		Setup_UCX_if_Needed(idx_fs);
//		if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
//			pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
//			pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
//			fetch_and_add(&nQp, 1);	// atomically add the counter
//		}
		
//		printf("DBG> rmdir(%s) idx_fs = %d\n", szFullPath, idx_fs);

		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
		
		// pIO_Cmd->rkey = mr_rem->rkey;
		// pIO_Cmd->rem_buff = mr_rem->addr;
		CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
		pIO_Cmd->rem_buff = ucx_rem_buff;
		strcpy(pIO_Cmd->szName, szFullPath);
//		pIO_Cmd->nTokenNeeded = 0;
		pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_REMOVE_DIR;	// NEED work on server side!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		Send_IO_Request(idx_fs);
		
		ret = (pResult->ret_value) & 0xFFFFFFFF;
		if(ret < 0)	{
			errno = pResult->myerrno;
			return ret;
		}
		else	return ret;
	}

	return real_rmdir(path);
}

extern "C" void Print_Mem(void)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int idx_fs;

	if(ucx_loc_buff == NULL)	Allocate_ucx_loc_rem_buff();
	
	pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
	idx_fs = 0;	// the index of which file server holding this file		
	// Setup_QP_if_Needed(idx_fs);
	Setup_UCX_if_Needed(idx_fs);
//	if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
//		pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
//		pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
//		fetch_and_add(&nQp, 1);	// atomically add the counter
//	}
	
	pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
	pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
	
	pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_PRINT_MEM;	// NEED work on server side!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

	// send the IO request first
	pIO_Cmd->tag_magic = rand();
	// pClient_qp[idx_fs]->IB_Put(ucx_loc_buff, mr_loc->lkey, (void*)(pClient_qp[idx_fs]->remote_addr_IO_CMD + sizeof(IO_CMD_MSG)*pClient_qp[idx_fs]->Idx_fs), pClient_qp[idx_fs]->pal_remote_mem.key, IO_Msg_Size_op);
	pClient_ucx[idx_fs]->UCX_Put(ucx_loc_buff, (void*)(pClient_ucx[idx_fs]->remote_addr_IO_CMD + sizeof(IO_CMD_MSG)*pClient_ucx[idx_fs]->Idx_fs), pClient_ucx[idx_fs]->pal_remote_mem.rkey, IO_Msg_Size_op);
	// send a msg to notify that a new IO quest is coming.
	ucx_loc_buff[0] = TAG_NEW_REQUEST;
	// pClient_qp[idx_fs]->IB_Put(ucx_loc_buff, mr_loc->lkey, (void*)(pClient_qp[idx_fs]->remote_addr_new_msg + sizeof(char)*pClient_qp[idx_fs]->Idx_fs), pClient_qp[idx_fs]->pal_remote_mem.key, 1);
	pClient_ucx[idx_fs]->UCX_Put(ucx_loc_buff, (void*)(pClient_ucx[idx_fs]->remote_addr_new_msg + sizeof(char)*pClient_ucx[idx_fs]->Idx_fs), pClient_ucx[idx_fs]->pal_remote_mem.rkey, 1);
}


extern "C" int statfs(const char *pathname, struct statfs *buf)
{
	char szFullPath[MAX_FILE_NAME_LEN];
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, idx_fs=0;
	FS_STAT fs_Stat_Sum, *pFS_Stat;

	if(real_statfs==NULL)	{
		real_statfs = (org_statfs)dlsym(RTLD_NEXT, "statfs");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_statfs(pathname, buf);
	}

	Standardlize_Path(pathname, szFullPath);

	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{
		if(ucx_loc_buff == NULL)	Allocate_ucx_loc_rem_buff();

		memcpy(buf, &fs_stat_shm, sizeof(struct statfs));
		fs_Stat_Sum.fs_nblocks = 0;
		fs_Stat_Sum.fs_bfreeblocks = 0;
		fs_Stat_Sum.fs_ninode = 0;
		fs_Stat_Sum.fs_nfreeinode = 0;

		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		for(idx_fs=0; idx_fs<UCXFileServerListLocal.nFSServer; idx_fs++)	{
			// Setup_QP_if_Needed(idx_fs);
			Setup_UCX_if_Needed(idx_fs);
//			if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
//				pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
//				pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
//				fetch_and_add(&nQp, 1);	// atomically add the counter
//			}
			pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
			// pIO_Cmd->rkey = mr_rem->rkey;
			// pIO_Cmd->rem_buff = mr_rem->addr;
			CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
			pIO_Cmd->rem_buff = ucx_rem_buff;
			pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_STAT_FS;
			Send_IO_Request(idx_fs);
			pFS_Stat = (FS_STAT *)( (char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int) );

			fs_Stat_Sum.fs_nblocks += pFS_Stat->fs_nblocks;
			fs_Stat_Sum.fs_bfreeblocks += pFS_Stat->fs_bfreeblocks;
			fs_Stat_Sum.fs_ninode += pFS_Stat->fs_ninode;
			fs_Stat_Sum.fs_nfreeinode += pFS_Stat->fs_nfreeinode;
		}
		fs_Stat_Sum.f_namelen = pFS_Stat->f_namelen;

		buf->f_type = BBTHEMIS_SUPER_MAGIC;
		buf->f_blocks = fs_Stat_Sum.fs_nblocks;
		buf->f_bfree = fs_Stat_Sum.fs_bfreeblocks;
		buf->f_bavail = fs_Stat_Sum.fs_bfreeblocks;
		buf->f_files = fs_Stat_Sum.fs_ninode;
		buf->f_ffree = fs_Stat_Sum.fs_nfreeinode;
		buf->f_namelen = pFS_Stat->f_namelen;

		return 0;
	}

	return real_statfs(pathname, buf);
}

extern "C" int staTfs64(const char *pathname, struct statfs *buf) __attribute__ ( (alias ("statfs")) );
extern "C" int __statfs(const char *pathname, struct statfs *buf) __attribute__ ( (alias ("statfs")) );

extern "C" int statvfs(const char *pathname, struct statvfs *buf)
{
	char szFullPath[MAX_FILE_NAME_LEN];
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, idx_fs=0;
	FS_STAT fs_Stat_Sum, *pFS_Stat;

	if(real_statvfs==NULL)	{
		real_statvfs = (org_statvfs)dlsym(RTLD_NEXT, "statvfs");
	}

	if(Inited == 0) {       // init() not finished yet
		return real_statvfs(pathname, buf);
	}

	Standardlize_Path(pathname, szFullPath);

	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{
		if(ucx_loc_buff == NULL)	Allocate_ucx_loc_rem_buff();

		memcpy(buf, &vfs_stat_shm, sizeof(struct statvfs));
		
		fs_Stat_Sum.fs_nblocks = 0;
		fs_Stat_Sum.fs_bfreeblocks = 0;
		fs_Stat_Sum.fs_ninode = 0;
		fs_Stat_Sum.fs_nfreeinode = 0;

		pIO_Cmd = (IO_CMD_MSG *)ucx_loc_buff;
		pResult = (RW_FUNC_RETURN *)ucx_rem_buff;
		for(idx_fs=0; idx_fs<UCXFileServerListLocal.nFSServer; idx_fs++)	{
			// Setup_QP_if_Needed(idx_fs);
			Setup_UCX_if_Needed(idx_fs);
//			if(pClient_qp[idx_fs] == NULL)	{	// Must be in a new thread. Need to establish a new QP. 
//				pClient_qp[idx_fs] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
//				pClient_qp[idx_fs]->Setup_QueuePair(idx_fs, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
//				fetch_and_add(&nQp, 1);	// atomically add the counter
//			}
			pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 
			// pIO_Cmd->rkey = mr_rem->rkey;
			// pIO_Cmd->rem_buff = mr_rem->addr;
			CLIENT_UCX::UCX_Pack_Rkey(ucx_mr_rem, pIO_Cmd->rkey_buffer);
			pIO_Cmd->rem_buff = ucx_rem_buff;
			pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_STAT_FS;
			Send_IO_Request(idx_fs);
			pFS_Stat = (FS_STAT *)( (char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int) );

			fs_Stat_Sum.fs_nblocks += pFS_Stat->fs_nblocks;
			fs_Stat_Sum.fs_bfreeblocks += pFS_Stat->fs_bfreeblocks;
			fs_Stat_Sum.fs_ninode += pFS_Stat->fs_ninode;
			fs_Stat_Sum.fs_nfreeinode += pFS_Stat->fs_nfreeinode;
		}
		fs_Stat_Sum.f_namelen = pFS_Stat->f_namelen;

		buf->f_blocks = fs_Stat_Sum.fs_nblocks;
		buf->f_bfree = fs_Stat_Sum.fs_bfreeblocks;
		buf->f_bavail = fs_Stat_Sum.fs_bfreeblocks;
		buf->f_files = fs_Stat_Sum.fs_ninode;
		buf->f_ffree = fs_Stat_Sum.fs_nfreeinode;
		buf->f_favail = fs_Stat_Sum.fs_nfreeinode;
		buf->f_namemax = pFS_Stat->f_namelen;

		return 0;
	}
	
	return real_statvfs(pathname, buf);
}
extern "C" int staTvfs64(const char *pathname, struct statvfs *buf) __attribute__ ( (alias ("statvfs")) );


extern "C" char * realpath(const char *pathname, char *resolved_path)
{
	char szFullPath[MAX_FILE_NAME_LEN];

	if(real_realpath==NULL)	{
		real_realpath = (org_realpath)dlvsym(RTLD_NEXT, "realpath", "GLIBC_2.3");
		assert(real_realpath != NULL);
	}

	if(Inited == 0) {       // init() not finished yet
		return real_realpath(pathname, resolved_path);
	}

	Standardlize_Path(pathname, szFullPath);

	if(strncmp(szFullPath, MYFS_ROOT_DIR, 5) == 0)	{
		if(resolved_path == NULL)	resolved_path = (char*)malloc(strlen(szFullPath) + 1);
		assert(resolved_path != NULL);
		strcpy(resolved_path, szFullPath);
		return resolved_path;
	}

	return real_realpath(pathname, resolved_path);

}
/*
extern "C" int MPI_Init(int *argc, char ***argv)
{
        if(real_MPI_Init==NULL)     {
                real_MPI_Init = (org_MPI_Init)dlsym(RTLD_NEXT, "MPI_Init");
                assert(real_MPI_Init != NULL);
        }

	int ret;
	ret = real_MPI_Init(argc, argv);
	struct timeval t;
	gettimeofday(&t, 0);
	t_MPI_Init = t.tv_sec + 0.000001 * t.tv_usec;
}

extern "C" int MPI_Finalize(void)
{
	if(real_MPI_Finalize==NULL)	{
		real_MPI_Finalize = (org_MPI_Finalize)dlsym(RTLD_NEXT, "MPI_Finalize");
		assert(real_MPI_Finalize != NULL);
	}

//	printf("DBG> In MPI_Finalize().\n");
	// You can push all cached IO requests to server here!!!

	struct timeval t;
	gettimeofday(&t, 0);
	t_MPI_Finalize = t.tv_sec + 0.000001 * t.tv_usec;
	if(mpi_rank == 0)	{
		char szMsg[128];
		sprintf(szMsg, "DBG> Time = %6.2lf\n", t_MPI_Finalize - t_MPI_Init);
		write(STDERR_FILENO, szMsg, strlen(szMsg));
		fsync(STDERR_FILENO);
	}
	return real_MPI_Finalize();
}
*/
extern "C" int flock(int fd, int operation)
{
	if(real_flock==NULL)	{
		real_flock = (org_flock)dlsym(RTLD_NEXT, "flock");
		assert(real_flock != NULL);
	}

	if(fd >= FD_FILE_BASE)	return 0;
	else	return real_flock(fd, operation);
}
/*
int test_open_client(char szFileName[])
{
	int fd, idx_fs=0;
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;

	pIO_Cmd = (IO_CMD_MSG *)loc_buff;
	pResult = (RW_FUNC_RETURN *)rem_buff;
	pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 

	strcpy(pIO_Cmd->szName, szFileName);
	pIO_Cmd->rem_buff = pClient_qp[0]->mr_rem->addr;
	pIO_Cmd->rkey = pClient_qp[0]->mr_rem->rkey;

	pIO_Cmd->flag = O_RDONLY;
	pIO_Cmd->nTokenNeeded = 0;
	pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_OPEN;
	Send_IO_Request(idx_fs);

	fd = (pResult->ret_value) & 0xFFFFFFFF;
	if(fd < 0)	{
		errno = pResult->myerrno;
	}
	return fd;
}

int test_open_new_file_client(char szNewFileName[])
{
	int fd, idx_fs=0;
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;

	pIO_Cmd = (IO_CMD_MSG *)loc_buff;
	pResult = (RW_FUNC_RETURN *)rem_buff;
	pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 

	strcpy(pIO_Cmd->szName, szNewFileName);
	pIO_Cmd->rem_buff = pClient_qp[0]->mr_rem->addr;
	pIO_Cmd->rkey = pClient_qp[0]->mr_rem->rkey;

	pIO_Cmd->flag = O_CREAT | O_WRONLY;
	pIO_Cmd->mode = S_IRUSR | S_IWUSR;
	pIO_Cmd->nTokenNeeded = 0;
	pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_OPEN;
	Send_IO_Request(idx_fs);

	fd = (pResult->ret_value) & 0xFFFFFFFF;
	if(fd < 0)	{
		errno = pResult->myerrno;
	}
	return fd;
}

size_t test_read_client(int fd, char *buf, size_t size)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_loc_buf=NULL;
	size_t nBytesRead;
	int idx_fs=0;

	pIO_Cmd = (IO_CMD_MSG *)loc_buff;
	pResult = (RW_FUNC_RETURN *)rem_buff;

	if(size > DATA_COPY_THRESHOLD_SIZE)	{
		mr_loc_buf = CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote(buf, size);
		pIO_Cmd->rem_buff = mr_loc_buf->addr;
		pIO_Cmd->rkey = mr_loc_buf->rkey;
	}
	else	{
		pIO_Cmd->rem_buff = pClient_qp[0]->mr_rem->addr;
		pIO_Cmd->rkey = pClient_qp[0]->mr_rem->rkey;
	}

	pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 

	pIO_Cmd->offset = -1;
	pIO_Cmd->nLen = size;
	pIO_Cmd->fd = fd;
	pIO_Cmd->nTokenNeeded = 0;
	pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_READ;

	Send_IO_Request(idx_fs);

	nBytesRead = pResult->ret_value;
	if(nBytesRead < 0)	{
		errno = pResult->myerrno;
	}

	if(mr_loc_buf)	ibv_dereg_mr(mr_loc_buf);	// data were written to buf already
	else	memcpy(buf, (char*)pResult+sizeof(RW_FUNC_RETURN)-sizeof(int), nBytesRead);

	return nBytesRead;
}

size_t test_write_client(int fd, char *buf, size_t size)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_loc_buf=NULL;
	size_t nBytesWritten;
	int idx_fs=0;

	pIO_Cmd = (IO_CMD_MSG *)loc_buff;
	pResult = (RW_FUNC_RETURN *)rem_buff;

	if(size > DATA_COPY_THRESHOLD_SIZE)	{
		mr_loc_buf = CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote(buf, size);
		pIO_Cmd->rem_buff = mr_loc_buf->addr;
		pIO_Cmd->rkey = mr_loc_buf->rkey;
	}
	else	{
		pIO_Cmd->rem_buff = pClient_qp[0]->mr_rem->addr;
		pIO_Cmd->rkey = pClient_qp[0]->mr_rem->rkey;

		memcpy((char*)pResult+sizeof(RW_FUNC_RETURN), buf, size);
	}
	pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 

	pIO_Cmd->offset = -1;
	pIO_Cmd->nLen = size;
	pIO_Cmd->fd = fd;
	pIO_Cmd->nTokenNeeded = 0;
	pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_WRITE;

	Send_IO_Request(idx_fs);

	nBytesWritten = pResult->ret_value;
	if(nBytesWritten < 0)	{
		errno = pResult->myerrno;
	}

	if(mr_loc_buf)	ibv_dereg_mr(mr_loc_buf);	// data were written to buf already

	return nBytesWritten;
}


int test_close_client(int fd)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int idx_fs=0;

	pIO_Cmd = (IO_CMD_MSG *)loc_buff;
	pResult = (RW_FUNC_RETURN *)rem_buff;
	pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 

//	strcpy(pIO_Cmd->szName, szFileName);
	pIO_Cmd->rem_buff = pClient_qp[0]->mr_rem->addr;
	pIO_Cmd->rkey = pClient_qp[0]->mr_rem->rkey;

	pIO_Cmd->fd = fd;
	pIO_Cmd->nTokenNeeded = 0;
	pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_CLOSE;
	Send_IO_Request(idx_fs);

	fd = (pResult->ret_value) & 0xFFFFFFFF;
	if(fd < 0)	{
		errno = pResult->myerrno;
	}
	return fd;
}

int test_stat_client(char szName[])
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	struct stat *pStatFile;
	int ret, idx_fs=0;

	pIO_Cmd = (IO_CMD_MSG *)loc_buff;
	pResult = (RW_FUNC_RETURN *)rem_buff;
	pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 

	strcpy(pIO_Cmd->szName, szName);
	pIO_Cmd->rem_buff = pClient_qp[0]->mr_rem->addr;
	pIO_Cmd->rkey = pClient_qp[0]->mr_rem->rkey;

	pIO_Cmd->nTokenNeeded = 0;
	pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_STAT;
	Send_IO_Request(idx_fs);

	ret = (pResult->ret_value) & 0xFFFFFFFF;
	if(ret < 0)	{
		errno = pResult->myerrno;
		return ret;
	}
	pStatFile = (struct stat *)((char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int));

	return ret;
}

off_t test_seek_client(int fd, off_t offset, int whence)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	int ret, idx_fs=0;

	pIO_Cmd = (IO_CMD_MSG *)loc_buff;
	pResult = (RW_FUNC_RETURN *)rem_buff;
	pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 

	pIO_Cmd->rem_buff = pClient_qp[0]->mr_rem->addr;
	pIO_Cmd->offset = offset;
	pIO_Cmd->rkey = pClient_qp[0]->mr_rem->rkey;
	pIO_Cmd->fd = fd;
	pIO_Cmd->flag = whence;

	pIO_Cmd->nTokenNeeded = 0;
	pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_SEEK;
	Send_IO_Request(idx_fs);

	if(ret < 0)	errno = pResult->myerrno;

	return pResult->ret_value;
}

int test_opendir_client(void)
{
	IO_CMD_MSG *pIO_Cmd;
	RW_FUNC_RETURN *pResult;
	char szDirName[]="/myfs/0";
	int *pNumEntry, *pEntryOffsetList, i, idx_fs=0;
	char *pEntryNameBuff;

	pIO_Cmd = (IO_CMD_MSG *)loc_buff;
	pResult = (RW_FUNC_RETURN *)rem_buff;
	pResult->nDataSize = 0;	// init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data. 

	strcpy(pIO_Cmd->szName, szDirName);
	pIO_Cmd->rem_buff = pClient_qp[0]->mr_rem->addr;
	pIO_Cmd->rkey = pClient_qp[0]->mr_rem->rkey;

//	pIO_Cmd->fd = fd;
	pIO_Cmd->nTokenNeeded = 0;
	pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_OPENDIR;
	Send_IO_Request(idx_fs);

	pNumEntry = (int*)( (char*)pResult + sizeof(RW_FUNC_RETURN)-sizeof(int) );
	pEntryOffsetList = (int*)( (char*)pResult + sizeof(RW_FUNC_RETURN) );
	pEntryNameBuff = (char*)pResult + sizeof(RW_FUNC_RETURN) + sizeof(int)*(*pNumEntry);

	printf("There are %d entries under %s.\n", *pNumEntry, szDirName);
	for(i=0; i<(*pNumEntry); i++)	{
		printf(" %d %s\n", i, pEntryNameBuff+pEntryOffsetList[i] + 1);
	}

//	fd = (pResult->ret_value) & 0xFFFFFFFF;
//	if(fd < 0)	{
//		errno = pResult->myerrno;
//	}

	return 0;
}
*/

void Update_CWD(void)
{
	char *szDir=NULL;
	
	szDir = get_current_dir_name();
	
	if(szDir == NULL)       {
		printf("Fail to get CWD with get_current_dir_name().\nQuit\n");
		exit(1);
	}
	else    {
		strcpy(szCurDir, szDir);
		free(szDir);
	}
}

extern "C" char *getcwd(char *buf, size_t size)
{
        if(real_getcwd == NULL) {
                real_getcwd = (org_getcwd)dlsym(RTLD_NEXT, "getcwd");
        }

        if(Inited == 0) {       // init() not finished yet
                return real_getcwd(buf, size);
        }

        if(szCurDir[0] != '/')  {       // Not initlized yet
                printf("DBG> szCurDir = %s\n", szCurDir);
                Update_CWD();
                printf("DBG> szCurDir = %s\n", szCurDir);
        }

        if( strncmp(szCurDir, MYFS_ROOT_DIR, 5) != 0)   {
                return real_getcwd(buf, size);
        }

        if(buf == NULL) {
                char *szPath=NULL;
                szPath = (char*)malloc(strlen(szCurDir) + 256);
                if(szPath == NULL)      {
                        printf("Fail to allocate memory for szPath in getcwd().\nQuit\n");
                        exit(1);
                }
                strcpy(szPath, szCurDir);
                return szPath;
        }
        else    {
                strcpy(buf, szCurDir);
                return buf;
        }

}

static void Find_Func_Addr(char szPathLib[], char szFunc_List[][MAX_LEN_FUNC_NAME], long int func_addr[], long int func_len[], long int img_base_addr, int nhook)
{
	int fd, i, j, k, count=0;
	struct stat file_stat;
	void *map_start;
	Elf64_Sym *symtab;
	Elf64_Ehdr *header;
	Elf64_Shdr *sections;
	int strtab_offset=0;
	void *pSymbBase=NULL;
	int nSym=0, nSymTotal=0, SymRecSize=0, SymOffset, RecAddr;
	char *szSymName;

	
	stat(szPathLib, &file_stat);
	
	fd = open(szPathLib, O_RDONLY);
	if(fd == -1)    {
		printf("Fail to open file %s\nQuit\n", szPathLib);
		exit(1);
	}
	
	map_start = mmap(0, file_stat.st_size, PROT_READ, MAP_SHARED, fd, 0);
	if((long int)map_start == -1)   {
		printf("Fail to mmap file %s\nQuit\n", szPathLib);
		exit(1);
	}
	header = (Elf64_Ehdr *) map_start;
	
	sections = (Elf64_Shdr *)((char *)map_start + header->e_shoff);
	
	for (i = 0; i < header->e_shnum; i++)	{
		if ( (sections[i].sh_type == SHT_DYNSYM) || (sections[i].sh_type == SHT_SYMTAB) ) {
			pSymbBase = (void*)(sections[i].sh_offset + (char*)map_start);
			SymRecSize = sections[i].sh_entsize;
			nSym = sections[i].sh_size / sections[i].sh_entsize;
			
			for (j = i-1; j < i+2; j++)   {	// tricky here!!!
				if ( (sections[j].sh_type == SHT_STRTAB) ) {
					strtab_offset = (int)(sections[j].sh_offset);
				}
			}
			
			for(j=0; j<nSym; j++) {
				RecAddr = SymRecSize*j;
				SymOffset = *( (int *)( (char*)pSymbBase + RecAddr ) ) & 0xFFFFFFFF;
				szSymName = (char *)( (char*)map_start + strtab_offset + SymOffset );
//				Type = *((int *)(pSymbBase + RecAddr + 4)) & 0xf;

				for(k=0; k<nhook; k++)	{
					if( strcmp(szSymName, szFunc_List[k])==0 )      { 
						func_addr[k] = ( (long int)( *((int *)((char*)pSymbBase + RecAddr + 8)) ) & 0xFFFFFFFF ) + img_base_addr;
//						func_len[k] = *((int *)(pSymbBase + RecAddr + 16));
						func_len[k] = (*((int *)((char*)pSymbBase + RecAddr + 16))) & 0xFFFFFFFF;
//						count++;
					}
				}
				
//				printf("%-20s addr %x \n", szSymName, *((int *)(pSymbBase + RecAddr + 8)));
				nSymTotal++;
			}
		}
	}
	munmap(map_start, file_stat.st_size);
	close(fd);

/*
	printf("In lib %s. %p\n", szPathLib, img_base_addr);
	for(j=0; j<nhook; j++)	{
		printf("Found %-16s at %llx with length %llx\n", szFunc_List[j], func_addr[j]-img_base_addr, func_len[j]);
	}
	printf("\n\n");
*/
}


static void Init_udis86(void)
{
	ud_init(&ud_obj);
	ud_set_mode(&ud_obj, 64);     // 32 or 64
	ud_set_syntax(&ud_obj, UD_SYN_INTEL); // intel syntax
}

int input_hook_x(ud_t* u)
{
	if(ud_idx < Max_Bytes_Disassemble)        {
		ud_idx++;
		return (int)(szOp[ud_idx-1] & 0xFF);
	}
	else    {
		return UD_EOI;
	}
}

void Remove_Dot_Dot(char szPath[])
{
	char *p_Offset_2Dots, *p_Back, *pTmp, *pMax, *pNewStr;
	int i, nLen, nNonZero=0;
	
//	printf("%s\n", szPath);
	nLen = strlen(szPath);

	p_Offset_2Dots = strstr(szPath, "..");
	if( p_Offset_2Dots == (szPath+1) )	{
		printf("Must be something wrong in path: %s\n", szPath);
		return;
	}

	while(p_Offset_2Dots > 0)	{
		pMax = p_Offset_2Dots + 2;
		for(p_Back=p_Offset_2Dots-2; p_Back>= szPath; p_Back--)	{
			if(*p_Back == '/')	{
				for(pTmp=p_Back; pTmp<pMax; pTmp++)	{
					*pTmp = 0;
				}
				break;
			}
		}
		p_Offset_2Dots = strstr(p_Offset_2Dots + 2, "..");
		if(p_Offset_2Dots == NULL)	{
			break;
		}
	}

	pNewStr = szPath;
	for(i=0; i<nLen; i++)	{
		if(szPath[i])	{
			pNewStr[nNonZero] = szPath[i];
			nNonZero++;
		}
	}
	pNewStr[nNonZero] = 0;

//	printf("%s\n", szPath);
}

void Remove_Dot(char szPath[])
{
	char *p_Offset_Dots, *p_Offset_Slash, *pNewStr;
	int i, nLen, nNonZero=0;
	
//	printf("%s\n", szPath);
	nLen = strlen(szPath);

	p_Offset_Dots = strstr(szPath, "/./");

	while(p_Offset_Dots > 0)	{
		p_Offset_Dots[0] = 0;
		p_Offset_Dots[1] = 0;
		p_Offset_Dots = strstr(p_Offset_Dots + 2, "/./");
		if(p_Offset_Dots == NULL)	{
			break;
		}
	}

	// replace "//" with "/"
	p_Offset_Slash = strstr(szPath, "//");
	while(p_Offset_Slash > 0)	{
		p_Offset_Slash[0] = 0;
		p_Offset_Slash = strstr(p_Offset_Slash + 1, "//");
		if(p_Offset_Slash == NULL)	{
			break;
		}
	}

	pNewStr = szPath;
	for(i=0; i<nLen; i++)	{
		if(szPath[i])	{
			pNewStr[nNonZero] = szPath[i];
			nNonZero++;
		}
	}
	pNewStr[nNonZero] = 0;
	for(i=nNonZero-1; i>=0; i--)	{	// remove "/" at the end of path!!!
		if(pNewStr[i] == '/')	{
			pNewStr[i] = 0;
		}
		else	break;
	}

//	printf("%s\n", szPath);
}


static void Setup_Trampoline(void)
{
	int i, j, jMax, ReadItem, *p_int, WithJmp[NHOOK];
	int nInstruction, RIP_Offset, Jmp_Offset;
	int OffsetList[MAX_INSTUMENTS];
	char szHexCode[MAX_INSTUMENTS][32], szInstruction[MAX_INSTUMENTS][64];
	char *pSubStr=NULL, *pOpOrgEntry;
	long int OrgEntryCode;
	void *pbaseOrg;
	size_t MemSize_Modify;

	Max_Bytes_Disassemble = 24;
	for(i=0; i<NHOOK; i++)	{	// disassemble the orginal functions
		WithJmp[i]=-1;

		pTrampoline[i].pOrgFunc = (void *)(org_func_addr[i]);
		pTrampoline[i].Offset_RIP_Var = NULL_RIP_VAR_OFFSET;
		pTrampoline[i].nBytesCopied = 0;

		ud_obj.pc = 0;
		ud_set_input_hook(&ud_obj, input_hook_x);
		
		nInstruction = 0;
		ud_idx = 0;
		szOp = (char *)(org_func_addr[i]);

//		printf("Disassemble %s\n", szOrgFunc_List[i]);
		while (ud_disassemble(&ud_obj)) {
			OffsetList[nInstruction] = ud_insn_off(&ud_obj);
			if(OffsetList[nInstruction] >= MAX_JMP_LEN)	{	// size of jmp instruction
				pTrampoline[i].nBytesCopied = OffsetList[nInstruction];
				if( (nInstruction > 0) && (strncmp(szHexCode[nInstruction-1], "e9", 2)==0) )	{
					if(strlen(szHexCode[nInstruction-1]) == 10)	{	// found a jmp instruction here!!!
						if(nInstruction >= 2)	{
							WithJmp[i] = strlen(szHexCode[nInstruction-2])/2 + 1;
						}
					}
				}
				break;
			}
//			printf("%16x ", OffsetList[nInstruction]);        // instruction offset
			
			strcpy(szHexCode[nInstruction], ud_insn_hex(&ud_obj));
			strcpy(szInstruction[nInstruction], ud_insn_asm(&ud_obj));
//			printf("%-16.16s %-24s", szHexCode[nInstruction], szInstruction[nInstruction]);   // binary code, assembly code
//			printf("\n");
			pSubStr = strstr(szInstruction[nInstruction], "[rip+");
			if(pSubStr)	{
				ReadItem = sscanf(pSubStr+5, "%x]", &RIP_Offset);
				if(ReadItem == 1)	{
					pTrampoline[i].Offset_RIP_Var = RIP_Offset;
//					printf("RIP_Offset = %x\n", RIP_Offset);
				}
			}
			nInstruction++;
		}
//		printf("To copy %d bytes.\n\n", nByteToCopied);
	}

	for(i=0; i<NHOOK; i++)	{
		memcpy(pTrampoline[i].bounce, szOp_Bounce, BOUNCE_CODE_LEN);
		*((long int *)(pTrampoline[i].bounce + OFFSET_HOOK_FUNC)) = func_addr[i];
	}


	for(i=0; i<NHOOK; i++)	{
		memcpy(pTrampoline[i].trampoline, pTrampoline[i].pOrgFunc, pTrampoline[i].nBytesCopied);
		pTrampoline[i].trampoline[pTrampoline[i].nBytesCopied] = 0xE9;	// jmp
//		Jmp_Offset = (int) ( ( (long int)(pTrampoline[i].pOrgFunc) + pTrampoline[i].nBytesCopied - ( (long int)(pTrampoline[i].trampoline) +  pTrampoline[i].nBytesCopied + 5) )  & 0xFFFFFFFF);
		Jmp_Offset = (int) ( ( (long int)(pTrampoline[i].pOrgFunc) - ( (long int)(pTrampoline[i].trampoline) + 5) )  & 0xFFFFFFFF);
		*((int*)(pTrampoline[i].trampoline + pTrampoline[i].nBytesCopied + 1)) = Jmp_Offset;

		if(pTrampoline[i].Offset_RIP_Var != NULL_RIP_VAR_OFFSET)	{
			jMax = pTrampoline[i].nBytesCopied - 4;
			for(j=1; j<jMax; j++)	{
				p_int = (int *)(pTrampoline[i].trampoline + j);
				if( *p_int == pTrampoline[i].Offset_RIP_Var )	{
					*p_int += ( (int)( ( (long int)(pTrampoline[i].pOrgFunc) - (long int)(pTrampoline[i].trampoline) )  ) );	// correct relative offset of PIC var
				}
			}
		}
		if(WithJmp[i] > 0)	{
//			printf("pTrampoline[i].nBytesCopied = %d WithJmp = %d\n", pTrampoline[i].nBytesCopied, WithJmp[i]);
			p_int = (int *)(pTrampoline[i].trampoline + WithJmp[i]);
//			printf("Before: %x\n", *p_int);
			*p_int += ( (int)( ( (long int)(pTrampoline[i].pOrgFunc) - (long int)(pTrampoline[i].trampoline) )  ) );
//			printf("After: %x\n", *p_int);
//			printf("Fixed jmp instruction offset in function: %s\n", szOrgFunc_List[i]);
		}
	}
	
	// set up function pointers for original functions

	real_open = (org_open)(pTrampoline[0].trampoline);
	real_close_nocancel = (org_close_nocancel)(pTrampoline[1].trampoline);
	real_read = (org_read)(pTrampoline[2].trampoline);
	real_write = (org_write)(pTrampoline[3].trampoline);
	real_lseek = (org_lseek)(pTrampoline[4].trampoline);
	real_unlink = (org_unlink)(pTrampoline[5].trampoline);
	real_fxstat = (org_fxstat)(pTrampoline[6].trampoline);
	real_xstat = (org_xstat)(pTrampoline[7].trampoline);
	
	//start to patch orginal function
	for(i=0; i<NHOOK; i++)	{
		pbaseOrg = (void *)( (long int)(pTrampoline[i].pOrgFunc) & filter );	// fast mod

		MemSize_Modify = Set_Block_Size((void *)(pTrampoline[i].pOrgFunc));
		if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_WRITE | PROT_EXEC) != 0)	{
			printf("Error in executing mprotect(). %s\n", szFunc_List[i]);
			exit(1);
		}

		OrgEntryCode = *((long int*)(pTrampoline[i].pOrgFunc));
		pTrampoline[i].OrgCode = OrgEntryCode;
		pOpOrgEntry = (char *)(&OrgEntryCode);
		pOpOrgEntry[0] = 0xE9;
		*((int *)(pOpOrgEntry+1)) = (int)( (long int)(pTrampoline[i].bounce) - (long int)(pTrampoline[i].pOrgFunc) - 5 );
		*( (long int *)(pTrampoline[i].pOrgFunc) ) = OrgEntryCode;

		if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_EXEC) != 0)	{
			printf("Error in executing mprotect(). %s\n", szFunc_List[i]);
			exit(1);
		}
	}


	// patch functions in libpthread if applicable
	if(func_addr_in_lib_pthread[0])	{
		for(i=0; i<NHOOK_IN_PTHREAD; i++)	{
			pbaseOrg = (void *)( func_addr_in_lib_pthread[i] & filter );	// fast mod
			
			MemSize_Modify = Set_Block_Size((void *)(func_addr_in_lib_pthread[i]));
//			printf("pbaseOrg = %p  MemSize_Modify = %llx ", pbaseOrg, (long int)MemSize_Modify);
			if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_WRITE | PROT_EXEC) != 0)	{
				printf("Error in executing mprotect(). %s in libpthread\n", szFunc_List[i]);
				perror("mprotect");
				exit(1);
			}
			
			OrgEntryCode = *((long int*)(func_addr_in_lib_pthread[i]));
			org_code_saved_lib_pthread[i] = OrgEntryCode;
			pOpOrgEntry = (char *)(&OrgEntryCode);
			pOpOrgEntry[0] = 0xE9;
			*((int *)(pOpOrgEntry+1)) = (int)( (long int)(pTrampoline[i].bounce) - func_addr_in_lib_pthread[i] - 5 );
			*( (long int *)(func_addr_in_lib_pthread[i]) ) = OrgEntryCode;
			
			if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_EXEC) != 0)	{
				printf("Error in executing mprotect(). %s in libpthread\n", szFunc_List[i]);
				exit(1);
			}
		}
	}

	
}

void* Query_Available_Memory_BLock(void *p)
{
	int i, iSeg;
	uint64_t pCheck=(uint64_t)p;
	void *p_Alloc;

	iSeg = -1;
	for(i=0; i<nSeg; i++)	{
		if( (pCheck >= Addr_Min[i]) && (pCheck <= Addr_Max[i]) )	{
			iSeg = i;
			break;
		}
	}

	if(iSeg < 0)	{
		printf("p_Query = %p\n", p);
		printf("Something wrong! The address you queried is not inside any module!\nQuit\n");
		sleep(60);
		exit(1);
	}
	p_Alloc = (void*)(Addr_Max[iSeg]);

	if( iSeg < (nSeg - 1) )	{
		if( (Addr_Min[iSeg+1] - Addr_Max[iSeg]) < MIN_MEM_SIZE)	{
			printf("Only %llx bytes available.\nQuit\n", Addr_Min[iSeg+1] - Addr_Max[iSeg]);
			exit(1);
		}
	}

//	printf("p_Alloc = %p  Distance = %ld\n", p_Alloc, Addr_Max[iSeg] - pCheck);

	return p_Alloc;
}

// 2aaaaaad1000-2aaaaaae8000 r--p 00000000 127:8fee 450370427156430895      /scratch1/00410/huang/pmem/myfs/shared_qp/ver2.2/client_so/wrapper.so

void Init_FS_Client();

#define MAX_MAP_SIZE	(524288)
void Get_Module_Maps(void)
{
	FILE *fIn;
	char szName[64], szBuf[MAX_MAP_SIZE], szLibName[256];
	int i, iPos, ReadItem, Idx;
	long int FileSize;
	uint64_t addr_B, addr_E, addr_my_code;
	
	addr_my_code = (uint64_t)Init_FS_Client;
	
	sprintf(szName, "/proc/%d/maps", pid);
	fIn = fopen(szName, "rb");	// non-seekable file. fread is needed!!!
	if(fIn == NULL)	{
		printf("Fail to open file: %s\nQuit\n", szName);
		exit(1);
	}
	
	FileSize = fread(szBuf, 1, MAX_MAP_SIZE, fIn);	// fread can read complete file. read() does not most of time!!!
	fclose(fIn);
	
	szBuf[FileSize] = 0;
	
	iPos = 0;	// start from the beginging
	
	while(iPos >= 0)	{
		ReadItem = sscanf(szBuf+iPos, "%lx-%lx", &addr_B, &addr_E);
		if(ReadItem == 2)	{
			Addr_Min[nSeg] = addr_B;
			Addr_Max[nSeg] = addr_E;
			if(nSeg > 1)	{	// merge contacted blocks !!!
				if(Addr_Min[nSeg] == Addr_Max[nSeg-1])	{
					Addr_Max[nSeg-1] = Addr_Max[nSeg];
					nSeg--;
				}
			}
			if( (addr_my_code >=addr_B) && (addr_my_code <=addr_E) && (img_mylib_base == 0) )	{
				ReadItem = sscanf(szBuf+iPos+72, "%s", szLibName);
				if(ReadItem == 1)	{
					if(strlen(szLibName) > 3)	{
						strcpy(szPathMyLib, szLibName);
//						img_mylib_base = (unsigned long int)addr_B - 0x17000;
//						img_mylib_base = (unsigned long int)addr_B - 0x36000;
						img_mylib_base = (unsigned long int)addr_B;
//						printf("My code is in %s. (%p, %p)\n", szPathMyLib, (void*)addr_B, (void*)addr_E);
					}
				}
			}
			nSeg++;
		}
		iPos = Get_Position_of_Next_Line(szBuf, iPos + 38, FileSize);	// find the next line
	}
}


static int Get_Position_of_Next_Line(char szBuff[], int iPos, int nBuffSize)
{
	int i=iPos;
	
	while(i < nBuffSize)	{
		if(szBuff[i] == 0xA)	{	// A new line
			i++;
			return ( (i>=nBuffSize) ? (-1) : (i) );
		}
		else	{
			i++;
		}
	}
	return (-1);
}

inline static size_t Set_Block_Size(void *pAddr)
{
	unsigned long int res, pOrg;

	pOrg = (unsigned long int)pAddr;
	res = pOrg % page_size;
	if( (res + 5) > page_size )	{	// close to the boundary of two memory pages 
		return (size_t)(page_size*2);
	}
	else	{
		return (size_t)(page_size);
	}
}

inline void Make_Sure_FD_List_Inited(void)
{
	int i;

	if(Inited_fd_List == 0)	{
		for(i=0; i<MAX_OPENED_FILE; i++)	{
			FileList[i].fd = -1;
		}
		for(i=0; i<MAX_OPENED_DIR; i++)	{
			DirList[i].fd = -1;
		}
		Next_Av_fd=0;
		Last_fd=-1;
		Next_Av_dirfd=0;
		Last_dirfd=-1;

		nFd = nDirfd = 0;

		Inited_fd_List = 1;
	}
}

int Find_Next_Available_fd(void)
{
	int i, idx = -1, Done=0;

	Make_Sure_FD_List_Inited();

	if(Next_Av_fd < 0)	{
		return Next_Av_fd;
	}
	idx = Next_Av_fd;
	if(Next_Av_fd > Last_fd)	{
		Last_fd = Next_Av_fd;
	}
	Next_Av_fd = -1;

	for(i=idx+1; i<MAX_OPENED_FILE; i++)	{
		if(FileList[i].fd < 0)	{	// available
			Next_Av_fd = i;	// update Next_Av_fd
			Done = 1;
			break;
		}
	}
	if(Next_Av_fd < 0)	{
		printf("WARNING> All space for FileList are used.\n");
	}

	nFd++;
	return idx;
}

int Find_Next_Available_dirfd(void)
{
	int i, idx = -1, Done=0;

	Make_Sure_FD_List_Inited();

	if(Next_Av_dirfd < 0)	{
		return Next_Av_dirfd;
	}
	idx = Next_Av_dirfd;
	if(Next_Av_dirfd > Last_dirfd)	{
		Last_dirfd = Next_Av_dirfd;
	}
	Next_Av_dirfd = -1;

	for(i=idx+1; i<MAX_OPENED_DIR; i++)	{
		if(DirList[i].fd < 0)	{	// available
			Next_Av_dirfd = i;	// update Next_Av_fd
			Done = 1;
			break;
		}
	}
	if(Next_Av_dirfd < 0)	{
		printf("WARNING> All space for DirList are used.\n");
	}

	nDirfd++;
	return idx;
}

void Free_fd(int idx)
{
	int i;

	FileList[idx].fd = -1;

	if(idx < Next_Av_fd)	{
		Next_Av_fd = idx;
	}

	if(idx == Last_fd)	{
		for(i=idx-1; i>=0; i--)	{
			if(FileList[i].fd >= 0)	{
				Last_fd = i;
				break;
			}
		}
	}

	nFd--;
}

void Free_dirfd(int idx)
{
	int i;

	DirList[idx].fd = -1;

	if(idx < Next_Av_dirfd)	{
		Next_Av_dirfd = idx;
	}

	if(idx == Last_dirfd)	{
		for(i=idx-1; i>=0; i--)	{
			if(DirList[i].fd >= 0)	{
				Last_dirfd = i;
				break;
			}
		}
	}

	nDirfd--;
}

__attribute__((constructor)) void Init_FS_Client()
{

	int i;
	char szUid[16], *szEnvPWD, *szMPIRank;
	void *p_patch_allocated=NULL;
//	char szHostName[128];
//	if(pthread_mutex_init(&global_lock, NULL) != 0) {
//        perror("pthread_mutex_init");
//        exit(1);
//	}

//	return;

	ibv_fork_init();

	szMPIRank = getenv("PMI_RANK");
	if(szMPIRank == NULL)    {
		mpi_rank = 0;
	}
	else    {
		mpi_rank = atoi(szMPIRank);
	}

//	gethostname(szHostName, 63);
//	printf("DBG> Host %s pid = %d\n", szHostName, getpid());
//	fflush(stdout);

	Get_Exe_Name(szExeName);
	if( (strstr(szExeName, "ssh")) || (strstr(szExeName, "pmi_proxy")) )	{
		return;
	}
	Init_UCX_Client();
	// Init_Client();
	

//	if(p_sigaction==NULL)	p_sigaction = (org_sigaction)dlsym(RTLD_NEXT,"sigaction");

	// Setup_Signal_QueuePair();
	

	// Establish the first QP
//	memset(pClient_qp, 0, sizeof(CLIENT_QUEUEPAIR *)*MAX_FS_SERVER);
//	if( (strstr(szExeName, "ior")) || (strstr(szExeName, "mdtest")) || (strstr(szExeName, "wrf.exe")) )	{
/*
	CLIENT_QUEUEPAIR *pQP;
		for(i=0; i<pFileServerList->nFSServer; i++)	{	// set up the QP for the main thread now
////		idx_qp = nQp;
////		pClient_qp[i] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
////		pClient_qp[i]->Setup_QueuePair(i, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
			pQP = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));
//			Allocate_loc_rem_buff();
			pQP->Setup_QueuePair(i, (char*)loc_buff, IO_RESULT_BUFFER_SIZE + 4096, (char*)rem_buff, IO_RESULT_BUFFER_SIZE + 4096);	// !!!!!!!!!!!!!! idx_server need to be changed!!!!
//		printf("DBG> %d, %p %p\n", pQP, pClient_qp[i]);
			nQp++;
		}
*/
//	}
	for(i=0; i<NHOOK_IN_PTHREAD; i++)	{
		func_addr_in_lib_pthread[i] = 0;
	}
	for(i=0; i<MAX_FD_DUP2ED; i++)	{
		Fd_Dup2_List[i].fd_src = -1;
		Fd_Dup2_List[i].fd_dest = -1;
	}
	pid = getpid();

	szEnvPWD = getenv("PWD");
	if(szEnvPWD == NULL)	{
		printf("Waring: Fail to call getenv(\"PWD\")\n");
		Update_CWD();
	}
	else	{
		strcpy(szCurDir, szEnvPWD);	// The initial working directory
	}

	page_size = sysconf(_SC_PAGESIZE);
	filter = ~(page_size - 1);

	dl_iterate_phdr(callback, NULL);	// get library full path. ~44 us

	uid = getuid();

	Get_Module_Maps();
	Find_Func_Addr(szPathLibc, szOrgFunc_List, org_func_addr, org_func_len, img_libc_base, NHOOK);	// get addresses and function length for libc functions.
	Find_Func_Addr(szPathLibpthread, szOrgFunc_List, func_addr_in_lib_pthread, func_len_in_lib_pthread, img_libpthread_base, NHOOK_IN_PTHREAD);	// get addresses and function length for libc functions.
	Find_Func_Addr(szPathMyLib, szFunc_List, func_addr, func_len, img_mylib_base, NHOOK);	// get addresses and function length for my new functions.

	long int delta = (long int)my_open - func_addr[0];
	for(i=0; i<NHOOK; i++)	{
		func_addr[i] += delta;
	}

//	Query_Original_Func_Address();	// get addresses of org functions
	p_patch = Query_Available_Memory_BLock((void*)(org_func_addr[0]));
	p_patch_allocated = mmap(p_patch, MIN_MEM_SIZE, PROT_READ | PROT_WRITE | PROT_EXEC, MAP_FIXED | MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if(p_patch_allocated == MAP_FAILED) {
		printf("Fail to allocate code block at %p with mmap().\nQuit\n", p_patch);
		exit(1);
	}
	else if(p_patch_allocated != p_patch) {
		printf("Allocated at %p. Desired at %p\n", p_patch_allocated, p_patch);
	}

	pTrampoline = (TRAMPOLINE *)p_patch;

//	memset(FileStatus, 0, sizeof(FILESTATUS)*MAX_OPENED_FILE);
//	memset(DirStatus, 0, sizeof(DIRSTATUS)*MAX_OPENED_DIR);

	Init_udis86();
	Setup_Trampoline();

	statfs("/dev/shm", &fs_stat_shm);
	statvfs("/dev/shm", &vfs_stat_shm);
	
	Inited = 1;
	
	//start	to check stdin, stdout, stderr
	char szFileName[512];
	ssize_t nNameLen;
//	szStdin[0] = 0;
//	szStdout[0] = 0;
//	szStderr[0] = 0;

	int pid, idx_io_redirect;
	unsigned long long fn_hash;
	pid = getpid();
	idx_io_redirect = pHT_IO_Redirect_UCX->DictSearch(pid, &elt_list_IO_Redirect_UCX, &ht_table_IO_Redirect_UCX, &fn_hash);
	if(idx_io_redirect>=0)	{	// IO redirect was recorded. 
		if(pIO_Redirect_List_UCX[idx_io_redirect].fNameStdin[0])	{	// a valid record for stdin
			fd_stdin = my_open(pIO_Redirect_List_UCX[idx_io_redirect].fNameStdin, O_RDONLY);
			assert(fd_stdin >= FD_FILE_BASE);
//			printf("INFO> stdin is redirected to %s\n", pIO_Redirect_List_UCX[idx_io_redirect].fNameStdin);
		}
		if(pIO_Redirect_List_UCX[idx_io_redirect].fNameStdout[0])	{	// a valid record for stdout
			fd_stdout = my_open(pIO_Redirect_List_UCX[idx_io_redirect].fNameStdout, O_WRONLY | O_CREAT | O_TRUNC);
			assert(fd_stdout >= FD_FILE_BASE);
//			printf("INFO> stdout is redirected to %s\n", pIO_Redirect_List_UCX[idx_io_redirect].fNameStdout);
		}
		if(pIO_Redirect_List_UCX[idx_io_redirect].fNameStderr[0])	{	// a valid record for stderr
			fd_stderr = my_open(pIO_Redirect_List_UCX[idx_io_redirect].fNameStderr, O_WRONLY | O_CREAT | O_TRUNC);
			assert(fd_stderr >= FD_FILE_BASE);
//			printf("INFO> stderr is redirected to %s\n", pIO_Redirect_List_UCX[idx_io_redirect].fNameStderr);
		}
	}
	//end	to check stdin, stdout, stderr
}

__attribute__((destructor)) void Finalize_Client()
{
	int i, idx_io_redirect, pid;
	IO_CMD_MSG *pIO_Cmd=NULL;
	unsigned long long int fn_hash;

	if(fd_stdin > 0)	close(fd_stdin);
	if(fd_stdout > 0)	close(fd_stdout);
	if(fd_stderr > 0)	close(fd_stderr);
	if( (fd_stdin > 0) || (fd_stdout > 0) || (fd_stderr > 0) )	{
		pid = getpid();
		idx_io_redirect = pHT_IO_Redirect_UCX->DictSearch(pid, &elt_list_IO_Redirect_UCX, &ht_table_IO_Redirect_UCX, &fn_hash);
		assert(idx_io_redirect >= 0);
		pIO_Redirect_List_UCX[idx_io_redirect].fNameStdin[0] = 0;
		pIO_Redirect_List_UCX[idx_io_redirect].fNameStdout[0] = 0;
		pIO_Redirect_List_UCX[idx_io_redirect].fNameStderr[0] = 0;
		pthread_mutex_lock(&(pUCXFileServerList->lock_IO_Redirect));
		pHT_IO_Redirect_UCX->DictDelete(pid, &elt_list_IO_Redirect_UCX, &ht_table_IO_Redirect_UCX);
		pthread_mutex_unlock(&(pUCXFileServerList->lock_IO_Redirect));
		fd_stdin = fd_stdout = fd_stderr = -1;
	}

	pthread_mutex_lock(&ht_ucx_lock);
	
	if(pHT_ucx == NULL)	return;

	for(i=0; i<MAX_UCX_PER_PROCESS; i++)	{
		if(pClient_ucx_List[i])	{
			if(pClient_ucx_List[i]->ucp_worker)	{	// to put a msg to let server close this associated QP
				pIO_Cmd = (IO_CMD_MSG *)(pClient_ucx_List[i]->mr_loc_thread_addr);
				assert(pIO_Cmd != NULL);
//				local_mr = CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote((void*)pIO_Cmd, sizeof(IO_CMD_MSG));
				
				pIO_Cmd->rem_buff = 0;	// NO need for writing back
				// pIO_Cmd->rkey = 0;
				pIO_Cmd->op = IO_OP_MAGIC | RF_RW_OP_DISCONNECT;
//				Send_IO_Request(idx_fs);
				// send the IO request first
//				pClient_qp_List[i]->IB_Put((void*)pIO_Cmd, pClient_qp_List[i]->mr_loc->lkey, (void*)(pClient_qp_List[i]->remote_addr_IO_CMD + sizeof(IO_CMD_MSG)*pClient_qp_List[i]->Idx_fs), pClient_qp_List[i]->pal_remote_mem.key, sizeof(IO_CMD_MSG));
				// pClient_qp_List[i]->IB_Put((void*)pIO_Cmd, pClient_qp_List[i]->mr_loc_thread->lkey, (void*)(pClient_qp_List[i]->remote_addr_IO_CMD), pClient_qp_List[i]->pal_remote_mem.key, sizeof(IO_CMD_MSG));
				pClient_ucx_List[i]->UCX_Put((void*)pIO_Cmd, (void*)(pClient_ucx_List[i]->remote_addr_IO_CMD), pClient_ucx_List[i]->pal_remote_mem.rkey, sizeof(IO_CMD_MSG));
				// send a msg to notify that a new IO quest is coming.
				*((unsigned char *)pIO_Cmd) = TAG_NEW_REQUEST;
//				pClient_qp_List[i]->IB_Put((void*)pIO_Cmd, pClient_qp_List[i]->mr_loc->lkey, (void*)(pClient_qp_List[i]->remote_addr_new_msg + sizeof(char)*pClient_qp_List[i]->Idx_fs), pClient_qp_List[i]->pal_remote_mem.key, 1);
				// pClient_qp_List[i]->IB_Put((void*)pIO_Cmd, pClient_qp_List[i]->mr_loc_thread->lkey, (void*)(pClient_qp_List[i]->remote_addr_new_msg), pClient_qp_List[i]->pal_remote_mem.key, 1);
				pClient_ucx_List[i]->UCX_Put((void*)pIO_Cmd, (void*)(pClient_ucx_List[i]->remote_addr_new_msg), pClient_ucx_List[i]->pal_remote_mem.rkey, 1);
//				ibv_dereg_mr(local_mr);
			}
		}
	}


	for(i=0; i<MAX_UCX_PER_PROCESS; i++)	{	// destroy client side QP
		if(pClient_ucx_List[i])	{
			if(pClient_ucx_List[i]->ucp_worker)	{
				pClient_ucx_List[i]->CloseUCPDataWorker();
				pHT_ucx->DictDelete(pClient_ucx_List[i]->tid, &elt_list_ucx, &ht_table_ucx);

//				pthread_mutex_lock(&(pFileServerList->FS_List[pClient_qp_List[i]->Idx_fs].fs_qp_lock));
//				pFileServerList->FS_List[pClient_qp_List[i]->Idx_fs].nQP --;
//				pthread_mutex_unlock(&(pFileServerList->FS_List[pClient_qp_List[i]->Idx_fs].fs_qp_lock));
			}
			free(pClient_ucx_List[i]);
			if(ucx_loc_buff)	{
//				free(loc_buff);
				free(ucx_rem_buff);
				ucx_loc_buff = ucx_rem_buff = NULL;
			}
			pClient_ucx_List[i] = NULL;
		}
	}
// 	if(CLIENT_QUEUEPAIR::pd_)	{
// 		ibv_dealloc_pd(CLIENT_QUEUEPAIR::pd_);
// 		ibv_close_device(CLIENT_QUEUEPAIR::context_);
// //		ibv_free_device_list(dev_list_);
// 		CLIENT_QUEUEPAIR::pd_ = NULL;
// 	}

	free(pHT_ucx);
	pHT_ucx = NULL;
	pthread_mutex_unlock(&ht_ucx_lock);
	
	pthread_mutex_destroy(&ht_ucx_lock);

/*
	if(CLIENT_QUEUEPAIR::Done_IB_PD_Init)	{
		pthread_mutex_lock(&process_lock);
		if (CLIENT_QUEUEPAIR::pd_ != NULL) ibv_dealloc_pd(CLIENT_QUEUEPAIR::pd_);
		if (CLIENT_QUEUEPAIR::context_ != NULL)	ibv_close_device(CLIENT_QUEUEPAIR::context_);
		if (CLIENT_QUEUEPAIR::dev_list_ != NULL)	ibv_free_device_list(CLIENT_QUEUEPAIR::dev_list_);
		CLIENT_QUEUEPAIR::Done_IB_PD_Init = 0;
		pthread_mutex_unlock(&process_lock);
	}
*/
	pthread_mutex_destroy(&ucx_process_lock);
}
