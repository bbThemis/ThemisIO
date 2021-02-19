#ifndef __IO_QUEUE_COMMON
#define __IO_QUEUE_COMMON

#define DIR_ENT_TYPE_DIR	(4)
#define DIR_ENT_TYPE_FILE	(8)

#define FD_FILE_BASE		(100000000)
#define FD_DIR_BASE			(200000000)

#define IO_RESULT_BUFFER_SIZE		(1024*1024)	// 328 KB. (Maybe need to be measured and tuned later. Done tuning.)
#define DATA_COPY_THRESHOLD_SIZE	(328*1024)	// 328 KB. (Maybe need to be measured and tuned later. Done tuning.)
// Copy data if size is smaller than this threshold. RDMA if large data need to be transfered. Register destination block and pass the receiving buffer address. 

#define TAG_NEW_REQUEST	(0x80)
//#define TAG_RESULT_MAGIC	(0x20212021)

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
#define RF_RW_OP_UPDATE_IDX_PARENT_DIR_ENTRY_LIST	(0x20)
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

typedef struct	{
	char szName[160];	// May need to be increased later!
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
	int tid;
	int tag_magic;
	int idx_UserData;
	int op;		// operation tag. Containing a magic tag at the end!
}IO_CMD_MSG, *PIO_CMD_MSG;

typedef struct {
	long int ret_value;	// return value of function call
	int myerrno;	// errno is saved in case it is needed. 
	int Tag_Ini;	// Tag_End and Tag_Ini need to be consistent. Tag_Ini xor const = Tag_End 
	int nDataSize;	// the number of bytes of this data buffer
	int Tag_End;		// END		tag. Check this tag to make sure data transfer is DONE! Disabled here since a length undetermined buffer before this variable. 
}RW_FUNC_RETURN, *PRW_FUNC_RETURN;


#endif

