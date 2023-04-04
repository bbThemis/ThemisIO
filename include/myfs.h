#ifndef __MYFS_H__
#define __MYFS_H__

#include <sys/types.h>


#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <uct/api/uct.h>


#include "io_ops_common.h"
#include "dict.h"

typedef unsigned long int ULongInt;
typedef unsigned int UInt;

#define RATIO_DIRENTRY_HASHTABLE_EXPAND	(0.9)
#define RATIO_DIRENTRY_HASHTABLE_SHRINK	(0.35)

#define MAX_NUM_FILE_OP_LOCK	(1024)
#define MAX_NUM_FILE_OP_LOCK_M1	(MAX_NUM_FILE_OP_LOCK-1)

#define NUM_DIRCT_PT	(8)
#define DEFAULT_NUM_EXTRA_PT	(16)

//#define _NPAGES			(2*8192*1024L)
#define _NPAGES                       (8*8192*1024L)

#define MAX_NUM_LARGE_FILE	(0x400000UL)	// per node

#define MAX_NUM_FILE	(0x2000000UL)	// per node
#define MAX_NUM_DIR		(0x800000UL)	// per node

#define MAX_FD_ACTIVE	(1024*1024*128)
#define INVALID_FILE_IDX	(-1)
#define RESERVED_FILE_IDX	(-2)

//#define MAX_NUM_FILE	(10000000UL)	// per node
//#define MAX_NUM_DIR		(500000UL)	// per node
#define MAX_NUM_BLOCKS_TO_FREE	(4096)
#define MAX_NUM_BLOCKS_TO_FREE_M1	(MAX_NUM_BLOCKS_TO_FREE-1)

#define DEFAULT_FULL_FILE_NAME_LEN	(164)
#define DEFAULT_FULL_FILE_NAME_LEN_M1	(DEFAULT_FULL_FILE_NAME_LEN-1)
//#define DEFAULT_FULL_FILE_NAME_LEN	(176)
#define DEFAULT_MAX_ENTRY_PER_DIR	(1024)

#define MAX_LEN_EXTRA_POINTERS_BUFF	(2048*1024*1024L)
//#define MAX_LEN_DIR_ENTRY_BUFF	(128*1024*1024)
//#define MAX_LEN_DIR_ENTRY_OFFSET_BUFF	(160*1024*1024)
#define MAX_LEN_DIR_ENTRY_LIST_BUFF	(56*1024*1024)
#define MAX_LEN_LONG_FILE_NAME_BUFF	(16*1024*1024)
#define MAX_LEN_PER_DIR_ENTRY_BUFF	(2*1024*1024)
//#define MAX_LEN_RETURN_BUFF			(2*1024*1024)
#define MAX_LEN_OPEN_DIR_BUFF		(384*1024*1024)

#define MAX_NUM_RETURN_BUFF			(512)
#define SIZE_FOR_NEW_MSG	(64)

//#define MAX_LEN_DIR_ENTRY_HASHTABLE_BUFF	(16*4096*1024*1024L)	// NEED for large number of files in mdtest!!!
#define MAX_LEN_DIR_ENTRY_HASHTABLE_BUFF      (6*4096*1024*1024L)


typedef struct	{
	ULongInt AddressofData;	// the address of data blocks
	ULongInt FileOffset, DataBlockSize;
//	ULongInt OffsetofData;	// the relative address of data blocks
}DirectPointer, *PDirectPointer;

typedef struct	{
	int idx_file;	// idx_file - index in hash table. idx_bock - index of the current offset located in which pointer block. 
//	int idx_file, idx_block;	// idx_file - index in hash table. idx_bock - index of the current offset located in which pointer block. 
//	unsigned long int Offset;	// the offset of reading/writing
}ACTIVEFILE, *PACTIVEFILE;

typedef struct {
	int nLenName;			// 
	int nLenParentDirName;		// Needed when query parent dir status
	int idx_Server_Parent_Dir;	// index of the server that holds parent dir.
	int idx_Parent_Dir;		// the index of parent dir in hash table
//	int IdxEntry_in_Dir;	// the index of entry in the parent dir entry list
	int idx_dir_ht;			// the index in directory hash table. -1 means a regular file. >= 0 gives the index in dir hash table
	int nOpen;				// refernce counter. 
	int ToRemove;			// A flag for deleting file. Can be removed only after the reference number decreases to zero. 
	int nLinks;

	int nExtraPointer;
	int nMaxExtraPointer;	// the max number of extra pointers
	off_t MaxOffset, MaxDataRange;
	DirectPointer *pExtraData;	// the extra data blocks list

	char *pszFullName;	// full name in case of DEFAULT_FULL_FILE_NAME_LEN is not long enough to hold the file name
	char szFileName[DEFAULT_FULL_FILE_NAME_LEN];	// real file name. Full path. This should be stored in the hash table key. 

	// start struct stat
	ULongInt	st_dev;		// 8. ID of device containing file. st_dev = 2304
	ULongInt	st_ino;		// 8. Inode number
	ULongInt	st_nlink;	// 8. 1 for regular file without a hard link. 
	UInt		st_mode;    // 4. File type and mode
	UInt		st_uid;     // 4. User ID of owner
	UInt		st_gid;     // 4. Group ID of owner
	UInt		__pad0;     // 4. Always zero. 

	ULongInt	st_rdev;    // 8. Device ID (if special file). Always zero. 
	ULongInt	st_size;    // 8. Total size, in bytes
	ULongInt	st_blksize; // 8. Block size for filesystem I/O. 4096 bytes. 4194304 on lustre. It is important to set this to get better performance!!!
	ULongInt	st_blocks;  // 8. Number of 512B blocks allocated. Depends on file size. 
	struct timespec st_atim;  // 16. Time of last access
	struct timespec st_mtim;  // 16. Time of last modification
	struct timespec st_ctim;  // 16. Time of last status change
	ULongInt	__unused[3];  // 8*3. Always zero. 
//	struct stat stat;	// length 144 bytes
}META_INFO, *PMETA_INFO;	// local meta info of rw files

typedef struct {
//	UInt		nEntries;	// 4. number of entries under this directory
	int			nEntries;	// 4. number of entries under this directory
	UInt		nMaxEntry;	// 4. the max number of entries current buffer can hold.
//	ULongInt	nOffset_To_EntryNameOffsetList;		// the offset relative of p_DirEntryNameOffsetBuff
//	UInt		nLenAllEntries;
	int			nLenAllEntries;
	int			nEntryTriggerExpand;	// larger than or equal to this number triggers hash table expand. Will implement shrink later!
	int			nEntryTriggerShrink;	// larger than or equal to this number triggers hash table expand. Will implement shrink later!
	CHASHTABLE_DirEntry *p_Hash_DirEntry=NULL;
	struct elt_CharEntry *elt_list_DirEntry=NULL;
	int				*ht_table_DirEntry=NULL;

	char szDirName[DEFAULT_FULL_FILE_NAME_LEN];	// real file name. Full path. This should be stored in the hash table key. 
	char *pszFullName;	// full name in case of DEFAULT_FULL_FILE_NAME_LEN is not long enough to hold the file name
	char szPad[40];
}DIR_META_INFO, *PDIR_META_INFO;	// local meta info of rw files

typedef struct {
	int nExtraPointer;
	int nMaxExtraPointer;	// the max number of extra pointers
	size_t MaxOffset, MaxDataRange;
	DirectPointer *pExtraData;	// the extra data blocks list
}STRIPE_DATA_INFO, *PSTRIPE_DATA_INFO;

void Init_Memory(void);
int Query_Parent_Dir(const char szDirName[], int *nLenParentDirName, int *nLenFileName);
int my_mkdir(const char szDirName[], int mode, int uid, int gid);
int my_openfile(size_t DataReturn[], const char szFileName[], int oflags, ...);
int openfile_by_index(int idx_file, int bAppend);
int my_close(int fd, META_DATA_ON_CLOSE *pMetaData_OnClose);
//size_t my_read(int fd, void *buf, size_t count, off_t offset);
//size_t my_read_RDMA(int fd, int idx_qp, void *loc_buf, unsigned int lkey, void *rem_buf, unsigned int rkey, size_t count, off_t offset);
//size_t my_write(int fd, const void *buf, size_t count, off_t offset);
//size_t my_write_RDMA(int fd, int idx_qp, void *loc_buf, unsigned int lkey, void *rem_buf, unsigned int rkey, size_t count, off_t offset);

size_t my_write_stripe(int fd, const char *szFileName, int server_shift, const void *buf, size_t count, off_t offset);
size_t my_write_stripe_RDMA(int fd, const char *szFileName, int server_shift, int idx_ucx, void *loc_buf, void *rem_buf, ucp_rkey_h rkey, size_t count, off_t offset);

size_t my_read_stripe(int fd, const char *szFileName, int server_shift, void *buf, size_t count, off_t offset);
size_t my_read_stripe_RDMA(int fd, const char *szFileName, int server_shift, int idx_ucx, void *loc_buf, void *rem_buf, ucp_rkey_h rkey, size_t count, off_t offset);

int Find_First_Available_FD(void);
int Truncate_File(int file_idx, size_t size);
off_t my_lseek(int fd, off_t offset, int whence);
void Determine_Index_StorageBlock_for_Offset(int fd, off_t offset);
int Query_Index_StorageBlock_with_Offset(int idx_file, off_t offset);
int Query_Index_StorageBlock_with_Offset_Stripe(STRIPE_DATA_INFO *pStripeData, off_t offset);
int my_unlink(char szFileName[], size_t *nFileSize);
int my_rmdir(const char szDirName[]);
int my_ls(char szDirName[]);	// list entries under a directory
int my_AddEntryInfo(int my_file_idx, int dir_idx);
void my_RemoveEntryInfo(int my_file_idx);
int my_fdopendir(int fd, void *loc_buf);
int my_opendir_by_index(int dir_idx, void *loc_buf, long int offset, long int nBuffSize);
int my_opendir(char szDirName[], void *loc_buf);
int my_chmod(char szFileName[], int mode);
int my_setuserinfo(char szFileName[]);
void my_statfs(FS_STAT *pFS_Stat);

int my_AddEntryInfo_Remote_Request(char *szFullName, int nLenParentDirName, int EntryType, int *pIdx_Parent_Dir);
void my_RemoveEntryInfo_Remote_Request(char szEntryName_ToRemove[], int idx_Parent_Dir, int nLenParentDirName);
//void my_UpdateEntryIndex_in_ParentDir(char szPath[], int NewIdxEntry_in_Dir);

void Query_Other_Server(int idx_server);
void Test_File_System_Local(void);
static void Readin_All_Dir(void);
static void Readin_All_File(void);
ssize_t read_all(int fd, void *buf, size_t count);

void Request_Free_Stripe_Data(int idx_server, char szFileName[]);
void my_Free_Stripe_Data_Ext_Server(char szFileName[]);

#endif
