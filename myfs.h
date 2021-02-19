#ifndef __MYFS_H__
#define __MYFS_H__

#include <sys/types.h>
#include "io_ops_common.h"

typedef unsigned long int ULongInt;
typedef unsigned int UInt;

#define NUM_DIRCT_PT	(8)
#define DEFAULT_NUM_EXTRA_PT	(16)
#define _NPAGES			(8192*128)
#define MAX_NUM_FILE	(0x80000UL)	// per node
#define MAX_NUM_DIR		(0x8000UL)	// per node

#define MAX_FD_ACTIVE	(1024*1024)
#define INVALID_FILE_IDX	(-1)

//#define MAX_NUM_FILE	(10000000UL)	// per node
//#define MAX_NUM_DIR		(500000UL)	// per node
#define MAX_NUM_BLOCKS_TO_FREE	(4096)
#define MAX_NUM_BLOCKS_TO_FREE_M1	(MAX_NUM_BLOCKS_TO_FREE-1)

#define DEFAULT_FULL_FILE_NAME_LEN	(164)
#define DEFAULT_FULL_FILE_NAME_LEN_M1	(DEFAULT_FULL_FILE_NAME_LEN-1)
//#define DEFAULT_FULL_FILE_NAME_LEN	(176)
#define DEFAULT_MAX_ENTRY_PER_DIR	(16)

#define MAX_LEN_EXTRA_POINTERS_BUFF	(16*1024*1024)
#define MAX_LEN_DIR_ENTRY_BUFF	(32*1024*1024)
#define MAX_LEN_DIR_ENTRY_OFFSET_BUFF	(8*1024*1024)
#define MAX_LEN_DIR_ENTRY_LIST_BUFF	(8*1024*1024)
#define MAX_LEN_LONG_FILE_NAME_BUFF	(16*1024*1024)
#define MAX_LEN_PER_DIR_ENTRY_BUFF	(1024*1024)

typedef struct	{
	ULongInt AddressofData;	// the address of data blocks
	ULongInt FileOffset, DataBlockSize;
//	ULongInt OffsetofData;	// the relative address of data blocks
}DirectPointer, *PDirectPointer;

typedef struct	{
	int idx_file, idx_block;	// idx_file - index in hash table. idx_bock - index of the current offset located in which pointer block. 
	unsigned long int Offset;	// the offset of reading/writing
}ACTIVEFILE, *PACTIVEFILE;


typedef struct {
	int nLenName;			// 
	int nLenParentDirName;		// Needed when query parent dir status
	int idx_Parent_Dir;		// the index of parent dir in hash table
	int IdxEntry_in_Dir;	// the index of entry in the parent dir entry list
	int idx_dir_ht;			// the index in directory hash table. -1 means a regular file. >= 0 gives the index in dir hash table
	int nOpen;				// refernce counter. 
	int ToRemove;			// A flag for deleting file. Can be removed only after the reference number decreases to zero. 
	int nLinks;
	int nDirectPointer;
	int nExtraPointer;
	int nMaxExtraPointer;	// the max number of extra pointers
	int iPad[2];
	ULongInt nSizeAllocated;	// the allocated space
	DirectPointer DiretData[NUM_DIRCT_PT];	// list of direct data pointers
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
	ULongInt	st_blksize; // 8. Block size for filesystem I/O. 4096 bytes. 
	ULongInt	st_blocks;  // 8. Number of 512B blocks allocated. Depends on file size. 
	struct timespec st_atim;  // 16. Time of last access
	struct timespec st_mtim;  // 16. Time of last modification
	struct timespec st_ctim;  // 16. Time of last status change
	ULongInt	__unused[3];  // 8*3. Always zero. 
//	struct stat stat;	// length 144 bytes
//	char szPad[24];
}META_INFO, *PMETA_INFO;	// local meta info of rw files

typedef struct {
	UInt		nEntries;	// 4. number of entries under this directory
	UInt		nMaxEntry;	// 4. the max number of entries current buffer can hold.
	ULongInt	nOffset_To_EntryNameOffsetList;		// the offset relative of p_DirEntryNameOffsetBuff
	int			nLenAllEntries;
	char szDirName[DEFAULT_FULL_FILE_NAME_LEN];	// real file name. Full path. This should be stored in the hash table key. 
	char *pszFullName;	// full name in case of DEFAULT_FULL_FILE_NAME_LEN is not long enough to hold the file name
//	char szPad[56];
}DIR_META_INFO, *PDIR_META_INFO;	// local meta info of rw files

void Init_Memory(void);
int Query_Parent_Dir(char szDirName[], int *nLenParentDirName, int *nLenFileName);
int my_mkdir(char szDirName[]);
int my_openfile(char szFileName[], int oflags, ...);
int openfile_by_index(int idx_file, int bAppend);
int my_close(int fd);
size_t my_read(int fd, void *buf, size_t count);
size_t my_write(int fd, const void *buf, size_t count);
int Find_First_Available_FD(void);
int Truncate_File(int file_idx);
off_t my_lseek(int fd, off_t offset, int whence);
void Determine_Index_StorageBlock_for_Offset(int fd, off_t offset);
int my_unlink(char szFileName[]);
int my_rmdir(char szDirName[]);
int my_ls(char szDirName[]);	// list entries under a directory
int my_AddEntryInfo(int my_file_idx, int dir_idx);
void my_RemoveEntryInfo(int my_file_idx);

void Test_File_System_Local(void);
static void Readin_All_Dir(void);
static void Readin_All_File(void);
ssize_t read_all(int fd, void *buf, size_t count);

#endif
