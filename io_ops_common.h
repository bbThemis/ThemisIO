#ifndef __IO_QUEUE_COMMON
#define __IO_QUEUE_COMMON

#define MYFS_ROOT_DIR	"/myfs"

#define DIR_ENT_TYPE_DIR	(4)
#define DIR_ENT_TYPE_FILE	(8)

#define FD_FILE_BASE		(0x20000000)
#define FD_DIR_BASE			(0x40000000)
#define DUMMY_FD_DIR		(0x50000000)

#define MAX_ENTRY_NAME_LEN    (40)

#define MAX_NUM_DIR_ENTRY_PER_REQUEST	(1024*1024)
#define MAX_SIZE_DIR_ENTRY_BUFF_PER_REQUEST	(40*MAX_NUM_DIR_ENTRY_PER_REQUEST)	// the max length of each entry is 40 bytes right now. 

#define	BBTHEMIS_SUPER_MAGIC	0x5566
// max allowed 512 file servers
//#define SetFSIdx(fd,idx_fs)     ( fd = ( fd | (idx_fs << 20 ) ) )
//#define GetFileFSIdx(fd)            ( ( (fd-FD_FILE_BASE) & 0x1FF00000) >> 20  )
//#define GetDirFSIdx(fd)            ( ( (fd-FD_DIR_BASE) & 0x1FF00000) >> 20  )
//#define GetFileFD(fd)               ( (fd-FD_FILE_BASE) & 0xFFFFF )
//#define GetDirFD(fd)               ( (fd-FD_DIR_BASE) & 0xFFFFF )

#define GetFileFD(file_descriptor)               ( FileList[file_descriptor-FD_FILE_BASE].fd )
#define GetFileFSIdx(file_descriptor)            ( FileList[file_descriptor-FD_FILE_BASE].idx_fs )

//#define GetDirFD(fd)               ( DirList[fd-FD_DIR_BASE].fd )
//#define GetDirFSIdx(fd)            ( DirList[fd-FD_DIR_BASE].idx_fs )

//#define MAX_DEFERRED_SEEK_OP	(3)
#define DEFAULT_FILE_STAT_BLOCKSIZE   (1048576)
//#define DEFAULT_FILE_STAT_BLOCKSIZE	(4194304)

#define MAX_SIZE_MR_BLOCK     (8*1024*1024)
//#define MAX_SIZE_MR_BLOCK	(512*1024*1024)
//#define MAX_SIZE_MR_BLOCK	(1024*1024*1024)
//#define IO_RESULT_BUFFER_SIZE		(1024*1024)
#define IO_RESULT_BUFFER_SIZE		(2048*1024)
#define FILE_STRIPE_SIZE			(IO_RESULT_BUFFER_SIZE)
#define FILE_STRIPE_SIZE_M1		(FILE_STRIPE_SIZE - 1)
#define SHIFT_FOR_DIV_FILE_STRIPE_SIZE	(21)	// 2^SHIFT_FOR_DIV_FILE_STRIPE_SIZE == IO_RESULT_BUFFER_SIZE

//#define DATA_COPY_THRESHOLD_SIZE	(8*1024)	// 328 KB. (Maybe need to be measured and tuned later. Done tuning.)
#define DATA_COPY_THRESHOLD_SIZE	(328*1024)	// 328 KB. (Maybe need to be measured and tuned later. Done tuning.)
//#define DATA_COPY_THRESHOLD_SIZE	(1200*1024)	// 328 KB. (Maybe need to be measured and tuned later. Done tuning.)
// Copy data if size is smaller than this threshold. RDMA if large data need to be transfered. Register destination block and pass the receiving buffer address. 

#define TAG_NEW_REQUEST	(0x80)

#define IO_OP_MAGIC		(0x78563400)
#define RF_RO_OP_OPEN	(0x0)	// read only file.
#define RF_RO_OP_CLOSE	(0x1)	// read only file.
#define RF_RO_OP_DONE	(0x2)	// read only file. Nothing to do.

#define RF_RW_OP_OPEN	(0x11)
#define RF_RW_OP_READ	(0x12)	// read  at position
#define RF_RW_OP_WRITE	(0x13)	// write at position
#define RF_RW_OP_CLOSE	(0x14)
#define RF_RW_OP_STAT	(0x15)	// get stat info
#define RF_RW_OP_LSTAT	(0x16)	// get stat info
#define RF_RW_OP_REMOVE_FILE	(0x17)	// remove, unlink
#define RF_RW_OP_REMOVE_DIR		(0x18)	// VERY TRICKY!!!!!!! 
#define RF_RW_OP_MKDIR	(0x19)	// make a directory
#define RF_RW_OP_OPENDIR	(0x1A)	// open a directory.
#define RF_RW_OP_ADDENTRY_PARENT_DIR	(0x1B)	// add one entry into the items under a directory. "ls" need this. 
#define RF_RW_OP_REMOVEENTRY_PARENT_DIR	(0x1C)	// add one entry into the items under a directory. "ls" need this. 
#define RF_RW_OP_DIR_EXIST	(0x1D)	// Check a dir exist or not
#define RF_RW_OP_SEEK	(0x1E)	// lseek()
#define RF_RW_OP_FSTAT	(0x1F)
//#define RF_RW_OP_UPDATE_IDX_PARENT_DIR_ENTRY_LIST	(0x20)
#define RF_RW_OP_FILE_ALLOCATE	(0x21)
#define RF_RW_OP_POSIX_FILE_ALLOCATE	(0x22)
#define RF_RW_OP_TRUNCATE	(0x23)
#define RF_RW_OP_FTRUNCATE	(0x24)
#define RF_RW_OP_FSYNC		(0x25)
#define RF_RW_OP_POSIX_FADVISE	(0x26)
#define RF_RW_OP_FACCESSAT	(0x27)
#define RF_RW_OP_PREAD	(0x28)
#define RF_RW_OP_PWRITE	(0x29)
#define RF_RW_OP_FUTIMENS	(0x2A)
#define RF_RW_OP_UTIMES		(0x2B)
#define RF_RW_OP_FREE_STRIPE_DATA	(0x2C)
#define RF_RW_OP_STAT_FS	(0x2D)
#define RF_RW_OP_READ_DIR_ENTRIES	(0x2E)
//#define RF_RW_OP_UTIMES		(0x2B)
#define RF_RW_OP_HELLO			(0x4E)
#define RF_RW_OP_PRINT_MEM		(0x4F)
#define RF_RW_OP_DISCONNECT		(0x50)

/*
typedef struct	{
//	int newmsgflag;
	char szName[152];	// May need to be increased later!
//	char szName[160];	// May need to be increased later!
	void *rem_buff;// the buffer for remote memory access. For return code, errno and results sent back!
//	void *write_buff;	// only used for read() and pread() 
	long int offset;	// the offset of file
	unsigned long int nLen;		// the number of bytes to read/write
	unsigned long long file_hash;	// file hash calculated on each client to save the cpu on file server
	unsigned long long parent_dir_hash;
	unsigned int rkey;				// remote key for RDMA
	int nLen_FileName;			// the length of szName
	int nLen_Parent_Dir_Name;	// the length of parent dir
	int idx;			// index in the list of openned remote files
	int fd;				// fd valid only on the host holding the file
	int flag;			// open() needs flag
	int mode;			// open() needs mode when creating files
	int idx_qp;			// index of queue pair
	int nTokenNeeded;	// the number of token needed to finish this operation
	int tid;			// the index of IO worker that is handling this request
	int idx_JobRec;		// the index of job record. It is determined by jobid which is known from QP. 
	unsigned long int T_Queued;	// time in us when this OP was queued. 
	int tag_magic;
	int op;		// operation tag. Containing a magic tag at the end!
}IO_CMD_MSG, *PIO_CMD_MSG;
*/

typedef struct	{
	int fd;				// fd valid only on the host holding the file
	unsigned int rkey;				// remote key for RDMA
	void *rem_buff;// the buffer for remote memory access. For return code, errno and results sent back!
	char szName[160];	// May need to be increased later!

	int nLen_FileName;			// the length of szName
	int nLen_Parent_Dir_Name;	// the length of parent dir

	int flag;			// open() needs flag
	int mode;			// open() needs mode when creating files

	long int offset;	// the offset of file
	unsigned long int nLen;		// the number of bytes to read/write
	
	int tag_magic;
	int op;		// operation tag. Containing a magic tag at the end! // Offset 212
	
	unsigned long long file_hash;	// file hash calculated on each client to save the cpu on file server
	unsigned long long parent_dir_hash;

	// -----------------------------------------------------------------------------
	// Unued
//	int idx;			// index in the list of openned remote files
//	int pad;

	// The data set by server
	int idx_qp;			// index of queue pair
	int tid;			// the index of IO worker that is handling this request
	int idx_JobRec;		// the index of job record. It is determined by jobid which is known from QP. 
	int nTokenNeeded;	// the number of token needed to finish this operation
	unsigned long int T_Queued;	// time in us when this OP was queued. 
}IO_CMD_MSG, *PIO_CMD_MSG;

typedef struct {
	long int ret_value;	// return value of function call
	int myerrno;	// errno is saved in case it is needed. 
	int Tag_Ini;	// Tag_End and Tag_Ini need to be consistent. Tag_Ini xor const = Tag_End 
	int nDataSize;	// the number of bytes of this data buffer
	int pad[2];
	int Tag_End;		// END		tag. Check this tag to make sure data transfer is DONE! Disabled here since a length undetermined buffer before this variable. 
}RW_FUNC_RETURN, *PRW_FUNC_RETURN;

typedef struct {
	long int ret_value;	// return value of function call
	int myerrno;	// errno is saved in case it is needed. 
	int Tag_Ini;	// Tag_End and Tag_Ini need to be consistent. Tag_Ini xor const = Tag_End 
	int nDataSize;	// the number of bytes of this data buffer
	int pad[2];
	struct ibv_mr *mr_tmp;
	long int addr;
	int rkey, nEntry;
	int Tag_End;		// END		tag. Check this tag to make sure data transfer is DONE! Disabled here since a length undetermined buffer before this variable. 
}RW_FUNC_RETURN_EXT, *PRW_FUNC_RETURN_EXT;

typedef struct {
	int idx_Parent_Dir;		// the index of parent dir in hash table
//	int IdxEntry_in_Dir;	// the index of entry in the parent dir entry list
}PARENTDIR_FUNC_RETURN, *PPARENTDIR_FUNC_RETURN;

typedef	struct {
	off_t MaxDataRange;	// the largest offset in write(). 
	struct timespec modification_time;	// most recent modification time. 16 bytes. 
}META_DATA_ON_CLOSE, *PMETA_DATA_ON_CLOSE;

typedef	struct {
	size_t fs_nblocks, fs_bfreeblocks;	// the total number of data blocks, the total number of free data blocks
	size_t fs_ninode, fs_nfreeinode;	// the total number of inode, the total number of free inode
	int f_namelen;
}FS_STAT, *PFS_STAT;

/*
typedef struct	{
	long int offset;
	int whence;
	int pad;
}SEEK_OP_PARAM;
*/

#endif

