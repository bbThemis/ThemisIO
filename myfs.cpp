#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/time.h>
#include <errno.h>
#include <stdarg.h>
#include <bits/stat.h>

#include "myfs.h"
#include "qp.h"
#include "buddy.h"
#include "xxhash.h"
//#include "queue.h"
#include "dict.h"
#include "utility.h"
#include "ncx_slab.h"

extern long int nSizeReg;

//#define MAX_DIR_FD	(65536)
//#define MAX_DIR_FD_M1	((MAX_DIR_FD) - 1)
extern SERVER_QUEUEPAIR Server_qp;

static int my_uid, my_gid;

int nFile=0, nDir=0;	// number of file and dir on this server
ULongInt FSSize, HashTableFileSize, FileMetaDataSize, HashTableDirSize, DirMetaDataSize, AllocatorSize, DataAreaSize;
// hash table for files, dirs;
int fd_shm;
char szNameShm[]="myfs_shm";
char szFSRoot[64]=MYFS_ROOT_DIR;

pthread_mutex_t create_new_lock[MAX_NUM_FILE_OP_LOCK];
pthread_mutex_t unlink_lock[MAX_NUM_FILE_OP_LOCK];
pthread_mutex_t file_lock[MAX_NUM_FILE_OP_LOCK];
pthread_mutex_t dir_entry_lock[MAX_NUM_FILE_OP_LOCK];
pthread_mutex_t fd_lock;
pthread_mutex_t ht_lock;	// modify hashtable lock

ncx_slab_pool_t *sp_DirEntryName=NULL, *sp_DirEntryNameOffset=NULL, *sp_LongFileNameBuff=NULL;
ncx_slab_pool_t *sp_ExtraPointers=NULL;
ncx_slab_pool_t *sp_DirEntryList=NULL;
static char *p_DirEntryNameOffsetBuff=NULL;
static char *p_DirEntryNameBuff=NULL;

void *pMyfs=NULL;

CMEM_ALLOCATOR *pMem_Allocator=NULL;

CHASHTABLE_CHAR *p_Hash_File=NULL;
struct elt_Char *elt_list_file=NULL;
int *ht_table_file=NULL;

CHASHTABLE_CHAR *p_Hash_Dir=NULL;
struct elt_Char *elt_list_dir=NULL;
int *ht_table_dir=NULL;

META_INFO *pMetaData=NULL;
DIR_META_INFO *pDirMetaData=NULL;

extern int mpi_rank, nFSServer, nNUMAPerNode;	// rank and size of MPI, number of numa nodes per compute node

ACTIVEFILE __attribute__((aligned(16))) fd_List[MAX_FD_ACTIVE];
int nActiveFd=0, First_Av_Fd=0, IdxLastFd=-1;

void Init_Memory(void)
{
	ULongInt Offset;
	int i;
	char szNameShm_Full[128];

	sprintf(szNameShm_Full, "%s_%d", szNameShm, mpi_rank%nNUMAPerNode);
	fd_shm = shm_open(szNameShm_Full, O_RDWR | O_CREAT, 0600);
	if(fd_shm == -1)    {	// failed to create
		printf("Error to create %s\nQuit\n", szNameShm_Full);
		exit(1);
	}

	HashTableFileSize = CHASHTABLE_CHAR::GetStorageSize(MAX_NUM_FILE);
	HashTableDirSize = CHASHTABLE_CHAR::GetStorageSize(MAX_NUM_DIR);
	FileMetaDataSize = sizeof(META_INFO)*MAX_NUM_FILE;
	DirMetaDataSize = sizeof(DIR_META_INFO)*MAX_NUM_DIR;
	AllocatorSize = _NPAGES * sizeof(struct page);
	DataAreaSize = _NPAGES * BUDDY_PAGE_SIZE;

//	FSSize = HashTableFileSize + HashTableDirSize + FileMetaDataSize + DirMetaDataSize + sizeof(CMEM_ALLOCATOR) + AllocatorSize + DataAreaSize;
	FSSize = HashTableFileSize + HashTableDirSize + FileMetaDataSize + DirMetaDataSize + BUDDY_PAGE_SIZE + AllocatorSize + DataAreaSize;

	if (ftruncate(fd_shm, FSSize) != 0) {
		perror("ftruncate for fd_shm");
	}
	pMyfs = mmap(NULL, FSSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd_shm, 0);
	if(pMyfs == MAP_FAILED)	{
		perror("mmap for pMyfs");
	}
	Offset = 0;

	pMem_Allocator = (CMEM_ALLOCATOR *)pMyfs;
//	Offset += sizeof(CMEM_ALLOCATOR);
	Offset += BUDDY_PAGE_SIZE;
	pMem_Allocator->Mem_Allocator_Init(_NPAGES, (void*)((ULongInt)pMyfs + Offset), (void*)((ULongInt)pMyfs + Offset + AllocatorSize));
//	printf("%lx %lx \n", (ULongInt)pMyfs + Offset, (ULongInt)pMyfs + Offset + AllocatorSize);
	Offset += AllocatorSize;
	Offset += DataAreaSize;

	p_Hash_File = (CHASHTABLE_CHAR *)((ULongInt)pMyfs + Offset);
	p_Hash_File->DictCreate(MAX_NUM_FILE, &elt_list_file, &ht_table_file);	// init hash table
//	printf("%lx \n", (ULongInt)p_Hash_File);
	Offset += HashTableFileSize;

	pMetaData = (META_INFO *)((ULongInt)pMyfs + Offset);
//	printf("%lx \n", (ULongInt)pMetaData);
	Offset += FileMetaDataSize;

	p_Hash_Dir = (CHASHTABLE_CHAR *)((ULongInt)pMyfs + Offset);
	p_Hash_Dir->DictCreate(MAX_NUM_DIR, &elt_list_dir, &ht_table_dir);	// init hash table
//	printf("%lx \n", (ULongInt)p_Hash_Dir);
	Offset += HashTableDirSize;

	pDirMetaData = (DIR_META_INFO *)((ULongInt)pMyfs + Offset);
//	printf("%lx \n", (ULongInt)pDirMetaData);
	Offset += DirMetaDataSize;

	sp_DirEntryName = ncx_slab_init(MAX_LEN_DIR_ENTRY_BUFF);
	p_DirEntryNameBuff = (char*)sp_DirEntryName;
	sp_DirEntryNameOffset = ncx_slab_init(MAX_LEN_DIR_ENTRY_OFFSET_BUFF);
	p_DirEntryNameOffsetBuff = (char*)sp_DirEntryNameOffset;
	sp_LongFileNameBuff = ncx_slab_init(MAX_LEN_LONG_FILE_NAME_BUFF);
	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	sp_ExtraPointers = ncx_slab_init(MAX_LEN_EXTRA_POINTERS_BUFF);
	sp_DirEntryList = ncx_slab_init(MAX_LEN_DIR_ENTRY_LIST_BUFF);

	// insert the record of the root directory! 
	my_mkdir(szFSRoot, S_IRWXU | S_IRWXG | S_IRWXO, my_uid, my_gid);
	my_chmod(szFSRoot, 17407);	// copy the mode of "/dev/shm"
	my_setuserinfo(szFSRoot);

	for(i=0; i<MAX_FD_ACTIVE; i++)	{
		fd_List[i].idx_file = INVALID_FILE_IDX;
	}

	for(i=0; i<MAX_NUM_FILE_OP_LOCK; i++)	{
		if(pthread_mutex_init(&(create_new_lock[i]), NULL) != 0) {
			printf("\n mutex create_new_lock init failed\n");
			exit(1);
		}
		if(pthread_mutex_init(&(unlink_lock[i]), NULL) != 0) {
			printf("\n mutex unlink_lock init failed\n");
			exit(1);
		}
		if(pthread_mutex_init(&(file_lock[i]), NULL) != 0) { 
			printf("\n mutex file_lock init failed\n"); 
			exit(1);
		}
		if(pthread_mutex_init(&(dir_entry_lock[i]), NULL) != 0) { 
			printf("\n mutex file_lock init failed\n"); 
			exit(1);
		}

	}
	if(pthread_mutex_init(&fd_lock, NULL) != 0) { 
		printf("\n mutex fd_lock init failed\n"); 
		exit(1);
	}
	if(pthread_mutex_init(&ht_lock, NULL) != 0) { 
		printf("\n mutex ht_lock init failed\n"); 
		exit(1);
	}
	printf("INFO> Finished Init_Memory().\n");

	my_uid = getuid();
	my_gid = getgid();
}

int Query_Parent_Dir(char szDirName[], int *nLenParentDirName, int *nLenFileName)
{
	int i, dir_idx;
	char szParentDir[512];
	unsigned long long fn_hash=0;

	if(strcmp(szFSRoot, szDirName)==0)	{
		*nLenFileName = strlen(szDirName);
		*nLenParentDirName = 0;	// root
		return 0;	// root dir
	}

	for(i=0; ; i++)	{
		if(szDirName[i]==0)	{
			*nLenFileName = i;
			break;
		}
	}
	for(; i>=1 ;i--)	{
		if( (szDirName[i] == '/') && (szDirName[i-1] != '/') )	{
			break;
		}
	}
	*nLenParentDirName = i;
	memcpy(szParentDir, szDirName, i);
	szParentDir[i] = 0;

	dir_idx = p_Hash_Dir->DictSearch(szParentDir, &elt_list_dir, &ht_table_dir, &fn_hash);
	return dir_idx;
}

int my_mkdir(char szDirName[], int mode, int uid, int gid)
{
	int dir_idx, file_idx, Parent_dir_idx, nLenParentDirName, nLenFileName;
	unsigned long long fn_hash=0;
	struct timespec t_spec;

//	printf("DBG> mkdir(%s)\n", szDirName);
	clock_gettime(CLOCK_REALTIME, &t_spec);
	fn_hash = XXH64(szDirName, strlen(szDirName), 0);
	pthread_mutex_lock(&(create_new_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
	dir_idx = p_Hash_Dir->DictSearch(szDirName, &elt_list_dir, &ht_table_dir, &fn_hash);
	if(dir_idx < 0)	{
		Parent_dir_idx = Query_Parent_Dir(szDirName, &nLenParentDirName, &nLenFileName);
		if( Parent_dir_idx >=0 )	{	// parent dir exists. 
			pthread_mutex_lock(&ht_lock);
			dir_idx = p_Hash_Dir->DictInsertAuto(szDirName, &elt_list_dir, &ht_table_dir);
			nDir++;
			pthread_mutex_unlock(&ht_lock);

			strcpy(pDirMetaData[dir_idx].szDirName, szDirName);

			pDirMetaData[dir_idx].nEntries = 0;	// init number of entries
			pDirMetaData[dir_idx].nMaxEntry = DEFAULT_MAX_ENTRY_PER_DIR;
			pDirMetaData[dir_idx].nOffset_To_EntryNameOffsetList = (long int)ncx_slab_alloc(sp_DirEntryNameOffset, pDirMetaData[dir_idx].nMaxEntry*sizeof(int)) - (long int)p_DirEntryNameOffsetBuff;
			pDirMetaData[dir_idx].nLenAllEntries = 0;

			// insert into file hash table too.
			pthread_mutex_lock(&ht_lock);
			file_idx = p_Hash_File->DictInsertAuto(szDirName, &elt_list_file, &ht_table_file);
			nFile++;
			pthread_mutex_unlock(&ht_lock);

			strcpy(pMetaData[file_idx].szFileName, szDirName);
			pMetaData[file_idx].st_dev = 0;	// const
			pMetaData[file_idx].st_ino = file_idx;
			pMetaData[file_idx].st_nlink = 2;	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			pMetaData[file_idx].idx_dir_ht = dir_idx;
			pMetaData[file_idx].nLenName = nLenFileName;
			pMetaData[file_idx].nLenParentDirName = nLenParentDirName;
			pMetaData[file_idx].st_mode = mode | __S_IFDIR;	// set the mode for a directory!!!!!!!
			pMetaData[file_idx].st_uid = uid;
			pMetaData[file_idx].st_gid = gid;
			pMetaData[file_idx].st_size = 0;
			pMetaData[file_idx].st_atim.tv_sec = t_spec.tv_sec;
			pMetaData[file_idx].st_atim.tv_nsec = t_spec.tv_nsec;
			pMetaData[file_idx].st_mtim.tv_sec = t_spec.tv_sec;
			pMetaData[file_idx].st_mtim.tv_nsec = t_spec.tv_nsec;
			pMetaData[file_idx].st_ctim.tv_sec = t_spec.tv_sec;
			pMetaData[file_idx].st_ctim.tv_nsec = t_spec.tv_nsec;

			if(nLenParentDirName)	{
				my_AddEntryInfo(file_idx, Parent_dir_idx);	// only for non-root directory
				pMetaData[file_idx].idx_Parent_Dir = Parent_dir_idx;
			}
			pthread_mutex_unlock(&(create_new_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));

			return 0;
		}
		else	{
			pthread_mutex_unlock(&(create_new_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
			errno = ENOENT;
			return (-1);
		}

	}
	else	{	// already exist!
		pthread_mutex_unlock(&(create_new_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		errno = EEXIST;
		return (-1);
	}
}

int my_openfile(char szFileName[], int oflags, ...)
{
	int file_idx, dir_idx, bFlagCreate=0, bFlagTrunc=0, bAppend=0, nLenParentDirName, nLenFileName;
	unsigned long long fn_hash=0;
	int mode = 0, two_args=1;
//	static struct timeval tm;
	struct timespec t_spec;

//	printf("DBG> my_openfile(%s)\n", szFileName);

	if (oflags & O_CREAT)	{
		va_list arg;
		va_start (arg, oflags);
		mode = va_arg (arg, int);
		va_end (arg);
		two_args=0;
		bFlagCreate = 1;
		if(oflags & O_TRUNC)	bFlagTrunc = 1;
	}
	if ( (oflags & O_APPEND) && ( (oflags & O_WRONLY) || (oflags & O_RDWR) ) ) bAppend=1;

//	file_idx = p_Hash_File->DictSearch(szFileName, &elt_list_file, &ht_table_file, &fn_hash);
	if(bFlagCreate)	{
		fn_hash = XXH64(szFileName, strlen(szFileName), 0);
		pthread_mutex_lock(&(create_new_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		file_idx = p_Hash_File->DictSearch(szFileName, &elt_list_file, &ht_table_file, &fn_hash);

		if(file_idx < 0)	{	// not existing. Create a new file
			dir_idx = Query_Parent_Dir(szFileName, &nLenParentDirName, &nLenFileName);
			if( dir_idx >= 0)	{	// parent dir exists.
				// insert into file hash table too.

				pthread_mutex_lock(&ht_lock);
				file_idx = p_Hash_File->DictInsertAuto(szFileName, &elt_list_file, &ht_table_file);
				nFile++;
				pthread_mutex_unlock(&ht_lock);

				pMetaData[file_idx].nLenName = nLenFileName;
				pMetaData[file_idx].nLenParentDirName = nLenParentDirName;
				if(nLenFileName >= DEFAULT_FULL_FILE_NAME_LEN)	{
					pMetaData[file_idx].pszFullName = (char*)ncx_slab_alloc(sp_LongFileNameBuff, nLenFileName + 1);
					assert(pMetaData[file_idx].pszFullName != NULL);
					strcpy(pMetaData[file_idx].pszFullName, szFileName);
					memcpy(pMetaData[file_idx].szFileName, szFileName, DEFAULT_FULL_FILE_NAME_LEN_M1);
					pMetaData[file_idx].szFileName[DEFAULT_FULL_FILE_NAME_LEN_M1] = 0;
				}
				else	{
					pMetaData[file_idx].pszFullName = NULL;
					strcpy(pMetaData[file_idx].szFileName, szFileName);
				}
//				gettimeofday(&tm, NULL);
				clock_gettime(CLOCK_REALTIME, &t_spec);
				pMetaData[file_idx].idx_dir_ht = -1;	// regular file
				pMetaData[file_idx].nOpen = 1;
//				pMetaData[file_idx].nDirectPointer = 0;
//				pMetaData[file_idx].nExtraPointer = 0;
//				pMetaData[file_idx].nMaxExtraPointer = 0;
//				pMetaData[file_idx].pExtraData = NULL;
				memset(&(pMetaData[file_idx].nDirectPointer), 0, sizeof(DirectPointer)*NUM_DIRCT_PT + sizeof(DirectPointer *) + 4*sizeof(int) + sizeof(ULongInt));
				pMetaData[file_idx].st_dev = 0;	// const
				pMetaData[file_idx].st_ino = file_idx;
				pMetaData[file_idx].st_nlink = 1;
				pMetaData[file_idx].st_mode = 0x8180;	// regular file and -rw-------
				pMetaData[file_idx].st_uid = 800193;	// user id
				pMetaData[file_idx].st_gid = 25276;	// group id
				pMetaData[file_idx].st_rdev = 0;	// const
				pMetaData[file_idx].st_size = 0;	// zero size for a newly created file
				pMetaData[file_idx].st_blksize = 4096;	// const
				pMetaData[file_idx].st_blocks = 0;
				pMetaData[file_idx].st_atim.tv_sec = t_spec.tv_sec;
				pMetaData[file_idx].st_atim.tv_nsec = t_spec.tv_nsec;
				pMetaData[file_idx].st_mtim.tv_sec = t_spec.tv_sec;
				pMetaData[file_idx].st_mtim.tv_nsec = t_spec.tv_nsec;
				pMetaData[file_idx].st_ctim.tv_sec = t_spec.tv_sec;
				pMetaData[file_idx].st_ctim.tv_nsec = t_spec.tv_nsec;
	
				my_AddEntryInfo(file_idx, dir_idx);

				pMetaData[file_idx].idx_Parent_Dir = dir_idx;

				pthread_mutex_unlock(&(create_new_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
				return openfile_by_index(file_idx, bAppend);
			}
			else	{
				pthread_mutex_unlock(&(create_new_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
				errno = ENOENT;
				return (-1);
			}
		}
		else	{	// opening existing file
			if(bFlagTrunc)	{
				Truncate_File(file_idx, 0);
			}
			pthread_mutex_unlock(&(create_new_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
			return openfile_by_index(file_idx, bAppend);
		}
	}
	else	{
		file_idx = p_Hash_File->DictSearch(szFileName, &elt_list_file, &ht_table_file, &fn_hash);
		if(file_idx < 0)	{	// does not exist
			errno = ENOENT;
			return (-1);
		}
		else	{
			return openfile_by_index(file_idx, bAppend);
		}
	}
}

int Find_First_Available_FD(void)
{
	int i, idx = -1, Done=0;

	pthread_mutex_lock(&fd_lock);
	if(First_Av_Fd < 0)	{
		pthread_mutex_unlock(&fd_lock);
		return First_Av_Fd;
	}
	fd_List[First_Av_Fd].idx_file = RESERVED_FILE_IDX;	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Important. 

	idx = First_Av_Fd;
	if(First_Av_Fd > IdxLastFd)	{
		IdxLastFd = First_Av_Fd;
	}
	First_Av_Fd = -1;

	for(i=idx+1; i<MAX_FD_ACTIVE; i++)	{
		if(fd_List[i].idx_file == INVALID_FILE_IDX)	{
			First_Av_Fd = i;	// update First_Av_Fd
			Done = 1;
			break;
		}
	}
	if(First_Av_Fd < 0)	{
		printf("WARNING> All space for fd_List are used.\n");
	}
	nActiveFd++;

	pthread_mutex_unlock(&fd_lock);

	return idx;
}

int openfile_by_index(int idx_file, int bAppend)
{
	int i, fd;
	size_t nBytesAllocated;

	fd = Find_First_Available_FD();
	fd_List[fd].idx_file = idx_file;
	if(bAppend)	{
		fd_List[fd].idx_block = (pMetaData[idx_file].st_size < pMetaData[idx_file].nSizeAllocated) ? (pMetaData[idx_file].nDirectPointer + pMetaData[idx_file].nExtraPointer - 1) : (pMetaData[idx_file].nDirectPointer + pMetaData[idx_file].nExtraPointer);
		fd_List[fd].Offset = pMetaData[idx_file].st_size;
	}
	else	{
		fd_List[fd].idx_block = 0;
		fd_List[fd].Offset = 0;
	}
//	fd_List[fd].idx_block = (pMetaData[idx_file].st_size < pMetaData[idx_file].nSizeAllocated) ? (pMetaData[idx_file].nDirectPointer + pMetaData[idx_file].nExtraPointer - 1) : (pMetaData[idx_file].nDirectPointer + pMetaData[idx_file].nExtraPointer);
//	fd_List[fd].Offset = bAppend ? (pMetaData[idx_file].st_size) : (0);

//	printf("DBG> open(%s) fd = %d idx = %d\n", pMetaData[idx_file].szFileName, fd, fd_List[fd].idx_file);
	return fd;
}

int my_close(int fd)
{
	int i, idx_file;
//	static struct timeval tm;
	struct timespec t_spec;

//	printf("DBG> closing(%d)\n", fd);
	clock_gettime(CLOCK_REALTIME, &t_spec);
//	gettimeofday(&tm, NULL);

	pthread_mutex_lock(&fd_lock);

	idx_file = fd_List[fd].idx_file;
//	printf("DBG> my_close(%d)\n", fd);
	if(idx_file < 0)	{
		printf("ERROR> Fatal error. idx_file = %d\n", idx_file);
	}
	assert(idx_file >= 0);
	
//	printf("DBG> Free %d fd.\n", fd);
	pMetaData[idx_file].st_atim.tv_sec = t_spec.tv_sec;
	pMetaData[idx_file].st_atim.tv_nsec = t_spec.tv_nsec;
//	pMetaData[idx_file].st_mtim.tv_sec = tm.tv_sec;	// need to track which fd is writing this file
//	pMetaData[idx_file].st_mtim.tv_nsec = tm.tv_usec*1000;


	pMetaData[idx_file].nOpen--;

//	fd_List[fd].idx_file = INVALID_FILE_IDX;

	if(fd < First_Av_Fd)	{
		First_Av_Fd = fd;
	}
	if(fd == IdxLastFd)	{	// Need to update IdxLastQueue
		IdxLastFd = -1;
		for(i=fd-1; i>=0; i--)	{
			if(fd_List[i].idx_file >= 0)	{
				IdxLastFd = i;
				break;
			}
		}
	}
	fd_List[fd].idx_file = INVALID_FILE_IDX;

	nActiveFd--;
	pthread_mutex_unlock(&fd_lock);

	return 0;
}

int Truncate_File(int file_idx, size_t size)
{
	long int nSize=0;
	int i, count=0, Done = 0, idx_block;
	DirectPointer *pDirectPointer, *pExtraPointer;
	void *Addr_List[MAX_NUM_BLOCKS_TO_FREE];
	META_INFO *pFileMetaInfo;

	pFileMetaInfo = &(pMetaData[file_idx]);

	if(pFileMetaInfo->st_size == 0)	return 0;

	pDirectPointer = pFileMetaInfo->DiretData;
	pExtraPointer = pFileMetaInfo->pExtraData;

	if(size == 0)	{
		for(i=0; i<pMetaData[file_idx].nDirectPointer; i++)	{
			if(pDirectPointer[i].AddressofData)	{
				Addr_List[count] = (void *)(pDirectPointer[i].AddressofData);
				count++;
			}
		}
		i = 0;

		while(Done == 0)	{
			for(; i<pMetaData[file_idx].nExtraPointer; i++)	{
				if(pExtraPointer[i].AddressofData)	{
					Addr_List[count] = (void *)(pExtraPointer[i].AddressofData);
					count++;
					if(count >= MAX_NUM_BLOCKS_TO_FREE_M1)	{
						break;
					}
				}
			}
			Addr_List[count] = NULL;
			pMem_Allocator->Mem_Batch_Free(Addr_List);	// free the memory pages of file content
			if(count == MAX_NUM_BLOCKS_TO_FREE_M1)	count = 0;	// again
			if(i>=pMetaData[file_idx].nExtraPointer)	Done = 1;
		}

		if(pMetaData[file_idx].nExtraPointer)	{
			ncx_slab_free(sp_ExtraPointers, pExtraPointer);
			pFileMetaInfo->nExtraPointer = 0;
			pFileMetaInfo->nMaxExtraPointer = 0;
			pFileMetaInfo->pExtraData = NULL;
		}

		pFileMetaInfo->nDirectPointer = 0;
		pFileMetaInfo->nSizeAllocated = 0;
		pFileMetaInfo->st_size = 0;
		pFileMetaInfo->st_blocks = 0;
	}
	else if(size > pFileMetaInfo->nSizeAllocated)	{	// Need to allocate a new block!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		void *pNewBuff=NULL;
		size_t nBytesJustAllocated;
		int nDirectPointer, nExtraPointer, nExtraPointerNewlyAllocated;
		DirectPointer *pExtraData_Org;

		pNewBuff = pMem_Allocator->Mem_Alloc(size - pFileMetaInfo->nSizeAllocated, &nBytesJustAllocated);
		assert( (pNewBuff != NULL) && (nBytesJustAllocated > 0) );
		nDirectPointer = pFileMetaInfo->nDirectPointer;

		if(nDirectPointer < NUM_DIRCT_PT)	{	// append this pointer
			if(pFileMetaInfo->st_size < (pFileMetaInfo->DiretData[nDirectPointer-1].FileOffset + pFileMetaInfo->DiretData[nDirectPointer-1].DataBlockSize) )	{
				memset((void*)( pFileMetaInfo->DiretData[nDirectPointer-1].AddressofData + (pFileMetaInfo->st_size - pFileMetaInfo->DiretData[nDirectPointer-1].FileOffset) ), 0, pFileMetaInfo->DiretData[nDirectPointer-1].FileOffset + pFileMetaInfo->DiretData[nDirectPointer-1].DataBlockSize - pFileMetaInfo->st_size);
			}

			pFileMetaInfo->DiretData[nDirectPointer].AddressofData = (ULongInt)pNewBuff;
			pFileMetaInfo->DiretData[nDirectPointer].FileOffset = (nDirectPointer > 0) ? (pFileMetaInfo->DiretData[nDirectPointer-1].FileOffset + pFileMetaInfo->DiretData[nDirectPointer-1].DataBlockSize) : (0L);
			pFileMetaInfo->DiretData[nDirectPointer].DataBlockSize = nBytesJustAllocated;
			memset((void*)(pFileMetaInfo->DiretData[nDirectPointer].AddressofData), 0, size-pFileMetaInfo->DiretData[nDirectPointer].FileOffset);
			pFileMetaInfo->nDirectPointer++;
		}
		else	{
			nExtraPointer = pFileMetaInfo->nExtraPointer;
			if(pFileMetaInfo->nMaxExtraPointer <= nExtraPointer)	{	// Need to reallocate the storage for pExtraData[]
				pExtraData_Org = pFileMetaInfo->pExtraData;
				nExtraPointerNewlyAllocated = pFileMetaInfo->nMaxExtraPointer + max((pFileMetaInfo->nMaxExtraPointer)>>2, DEFAULT_NUM_EXTRA_PT);
				pFileMetaInfo->pExtraData = (DirectPointer *)ncx_slab_alloc(sp_ExtraPointers, nExtraPointerNewlyAllocated*sizeof(DirectPointer));
				if(nExtraPointer)	memcpy(pFileMetaInfo->pExtraData, pExtraData_Org, sizeof(DirectPointer)*nExtraPointer);
				if(pExtraData_Org)	ncx_slab_free(sp_ExtraPointers, pExtraData_Org);
				pFileMetaInfo->nMaxExtraPointer = nExtraPointerNewlyAllocated;
			}
			if(nExtraPointer == 0)	{
				if(pFileMetaInfo->st_size < (pFileMetaInfo->DiretData[NUM_DIRCT_PT-1].FileOffset + pFileMetaInfo->DiretData[NUM_DIRCT_PT-1].DataBlockSize) )	{
					memset((void*)( pFileMetaInfo->DiretData[NUM_DIRCT_PT-1].AddressofData + (pFileMetaInfo->st_size - pFileMetaInfo->DiretData[NUM_DIRCT_PT-1].FileOffset) ), 0, pFileMetaInfo->DiretData[NUM_DIRCT_PT-1].FileOffset + pFileMetaInfo->DiretData[NUM_DIRCT_PT-1].DataBlockSize - pFileMetaInfo->st_size);
				}
			}
			else	{
				if(pFileMetaInfo->st_size < (pFileMetaInfo->pExtraData[nExtraPointer-1].FileOffset + pFileMetaInfo->pExtraData[nExtraPointer-1].DataBlockSize) )	{
					memset((void*)( pFileMetaInfo->pExtraData[nExtraPointer-1].AddressofData + (pFileMetaInfo->st_size - pFileMetaInfo->pExtraData[nExtraPointer-1].FileOffset) ), 0, pFileMetaInfo->pExtraData[nExtraPointer-1].FileOffset + pFileMetaInfo->pExtraData[nExtraPointer-1].DataBlockSize - pFileMetaInfo->st_size);
				}
			}
			pFileMetaInfo->pExtraData[nExtraPointer].AddressofData = (ULongInt)pNewBuff;
			pFileMetaInfo->pExtraData[nExtraPointer].FileOffset = (nExtraPointer > 0) ? (pFileMetaInfo->pExtraData[nExtraPointer-1].FileOffset + pFileMetaInfo->pExtraData[nExtraPointer-1].DataBlockSize) : (pFileMetaInfo->DiretData[NUM_DIRCT_PT-1].FileOffset + pFileMetaInfo->DiretData[NUM_DIRCT_PT-1].DataBlockSize);
			pFileMetaInfo->pExtraData[nExtraPointer].DataBlockSize = nBytesJustAllocated;
			memset((void*)(pFileMetaInfo->pExtraData[nExtraPointer].AddressofData), 0, size-pFileMetaInfo->pExtraData[nExtraPointer].FileOffset);

			pFileMetaInfo->nExtraPointer++;
		}
		pFileMetaInfo->nSizeAllocated += nBytesJustAllocated;
		pFileMetaInfo->st_blocks = (pFileMetaInfo->nSizeAllocated) >> 9;	// the number of blocks of 512 bytes
		pFileMetaInfo->st_size = size;
	}
	else	{	// No need to allocate storae. May need to fill zero or free storage partially. 
		idx_block = Query_Index_StorageBlock_with_Offset(file_idx, size);
		if(idx_block < pFileMetaInfo->nDirectPointer)	{
			memset((void*)(pFileMetaInfo->DiretData[idx_block].AddressofData + (size - pFileMetaInfo->DiretData[idx_block].FileOffset) ), 0,  pFileMetaInfo->DiretData[idx_block].FileOffset + pFileMetaInfo->DiretData[idx_block].DataBlockSize - size );
			for(i=idx_block+1; i<pFileMetaInfo->nDirectPointer; i++)	{
				if(pDirectPointer[i].AddressofData)	{
					Addr_List[count] = (void *)(pDirectPointer[i].AddressofData);
					count++;
				}
			}

			i = 0;
			while(Done == 0)	{
				for(; i<pFileMetaInfo->nExtraPointer; i++)	{
					if(pExtraPointer[i].AddressofData)	{
						Addr_List[count] = (void *)(pExtraPointer[i].AddressofData);
						count++;
						if(count >= MAX_NUM_BLOCKS_TO_FREE_M1)	{
							break;
						}
					}
				}
				Addr_List[count] = NULL;
				pMem_Allocator->Mem_Batch_Free(Addr_List);	// free the memory pages of file content
				if(count == MAX_NUM_BLOCKS_TO_FREE_M1)	count = 0;	// again
				if(i >= pFileMetaInfo->nExtraPointer)	Done = 1;
			}

			if(pFileMetaInfo->nExtraPointer)	{
				ncx_slab_free(sp_ExtraPointers, pExtraPointer);
				pFileMetaInfo->nExtraPointer = 0;
				pFileMetaInfo->nMaxExtraPointer = 0;
				pFileMetaInfo->pExtraData = NULL;
			}

			pFileMetaInfo->nDirectPointer = idx_block + 1;
			pFileMetaInfo->nSizeAllocated = pFileMetaInfo->DiretData[idx_block].FileOffset + pFileMetaInfo->DiretData[idx_block].DataBlockSize;
			pFileMetaInfo->st_size = size;
			pFileMetaInfo->st_blocks = (pFileMetaInfo->nSizeAllocated) >> 9;
		}
		else	{	// in extra data region
			i = idx_block - NUM_DIRCT_PT + 1;
			memset((void*)(pFileMetaInfo->pExtraData[i-1].AddressofData + (size - pFileMetaInfo->pExtraData[i-1].FileOffset) ), 0,  pFileMetaInfo->pExtraData[i-1].FileOffset + pFileMetaInfo->pExtraData[i-1].DataBlockSize - size );
			while(Done == 0)	{
				for(; i<pFileMetaInfo->nExtraPointer; i++)	{
					if(pExtraPointer[i].AddressofData)	{
						Addr_List[count] = (void *)(pExtraPointer[i].AddressofData);
						count++;
						if(count >= MAX_NUM_BLOCKS_TO_FREE_M1)	{
							break;
						}
					}
				}
				Addr_List[count] = NULL;
				pMem_Allocator->Mem_Batch_Free(Addr_List);	// free the memory pages of file content
				if(count == MAX_NUM_BLOCKS_TO_FREE_M1)	count = 0;	// again
				if(i >= pFileMetaInfo->nExtraPointer)	Done = 1;
			}
			pFileMetaInfo->nExtraPointer = idx_block - NUM_DIRCT_PT + 1;
/*
			if(pFileMetaInfo->nExtraPointer)	{	// maybe need to shrink pExtraPointer[] !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				ncx_slab_free(sp_ExtraPointers, pExtraPointer);
				pFileMetaInfo->nExtraPointer = 0;
				pFileMetaInfo->nMaxExtraPointer = 0;
				pFileMetaInfo->pExtraData = NULL;
			}
			pFileMetaInfo->nDirectPointer = idx_block + 1;
*/
			pFileMetaInfo->nSizeAllocated = pFileMetaInfo->pExtraData[idx_block-NUM_DIRCT_PT].FileOffset + pFileMetaInfo->pExtraData[idx_block-NUM_DIRCT_PT].DataBlockSize;
			pFileMetaInfo->st_size = size;
			pFileMetaInfo->st_blocks = (pFileMetaInfo->nSizeAllocated) >> 9;
		}
	}

	return 0;
}

size_t my_write(int fd, const void *buf, size_t count, off_t offset)
{
	int Done = 0, i, nDirectPointer, nExtraPointer, nExtraPointerNewlyAllocated, idx_file;
	size_t nBytes_Written, nBytes_Written_OneTime, nOffsetMax, nAllocatedSize, nBytesJustAllocated, nPrevOffset, count_save;
	META_INFO *pFileMetaInfo;
	void *pNewBuff=NULL;
	DirectPointer *pExtraData_Org;
//	Determine_Index_StorageBlock_for_Offset(fd, fd_List[fd].Offset);

	count_save = count;
	idx_file = fd_List[fd].idx_file;
	pFileMetaInfo = &(pMetaData[idx_file]);

//	pthread_mutex_lock(&(file_lock[fd & MAX_NUM_FILE_OP_LOCK_M1]));
	pthread_mutex_lock(&(file_lock[idx_file & MAX_NUM_FILE_OP_LOCK_M1]));

	nAllocatedSize = pFileMetaInfo->nSizeAllocated;
	nOffsetMax = offset + count;
//	nOffsetMax = fd_List[fd].Offset + count;	// the tentative end of this file
	nDirectPointer = pFileMetaInfo->nDirectPointer;
	if(nOffsetMax > nAllocatedSize)	{	// need allocate new pages to accomodate new incoming data.
		pNewBuff = pMem_Allocator->Mem_Alloc(nOffsetMax - nAllocatedSize, &nBytesJustAllocated);
		assert(pNewBuff != NULL);
		pFileMetaInfo->nSizeAllocated += nBytesJustAllocated;
		pFileMetaInfo->st_blocks = (pFileMetaInfo->nSizeAllocated) >> 9;	// the number of blocks of 512 bytes

		if(nDirectPointer < NUM_DIRCT_PT)	{	// append this pointer
			pFileMetaInfo->DiretData[nDirectPointer].AddressofData = (ULongInt)pNewBuff;
			pFileMetaInfo->DiretData[nDirectPointer].FileOffset = (nDirectPointer > 0) ? (pFileMetaInfo->DiretData[nDirectPointer-1].FileOffset + pFileMetaInfo->DiretData[nDirectPointer-1].DataBlockSize) : (0L);
			pFileMetaInfo->DiretData[nDirectPointer].DataBlockSize = nBytesJustAllocated;
			pFileMetaInfo->nDirectPointer++;
		}
		else	{
			nExtraPointer = pFileMetaInfo->nExtraPointer;
			if(pFileMetaInfo->nMaxExtraPointer <= nExtraPointer)	{	// Need to reallocate the storage for pExtraData[]
				pExtraData_Org = pFileMetaInfo->pExtraData;
				nExtraPointerNewlyAllocated = pFileMetaInfo->nMaxExtraPointer + max((pFileMetaInfo->nMaxExtraPointer)>>2, DEFAULT_NUM_EXTRA_PT);
				pFileMetaInfo->pExtraData = (DirectPointer *)ncx_slab_alloc(sp_ExtraPointers, nExtraPointerNewlyAllocated*sizeof(DirectPointer));
				if(nExtraPointer)	memcpy(pFileMetaInfo->pExtraData, pExtraData_Org, sizeof(DirectPointer)*nExtraPointer);
				if(pExtraData_Org)	ncx_slab_free(sp_ExtraPointers, pExtraData_Org);
				pFileMetaInfo->nMaxExtraPointer = nExtraPointerNewlyAllocated;
			}
			pFileMetaInfo->pExtraData[nExtraPointer].AddressofData = (ULongInt)pNewBuff;
			pFileMetaInfo->pExtraData[nExtraPointer].FileOffset = (nExtraPointer > 0) ? (pFileMetaInfo->pExtraData[nExtraPointer-1].FileOffset + pFileMetaInfo->pExtraData[nExtraPointer-1].DataBlockSize) : (pFileMetaInfo->DiretData[NUM_DIRCT_PT-1].FileOffset + pFileMetaInfo->DiretData[NUM_DIRCT_PT-1].DataBlockSize);
			pFileMetaInfo->pExtraData[nExtraPointer].DataBlockSize = nBytesJustAllocated;
			pFileMetaInfo->nExtraPointer++;
		}
	}
	pFileMetaInfo->st_size = max(pFileMetaInfo->st_size, nOffsetMax);	// update file size
	Determine_Index_StorageBlock_for_Offset(fd, offset);
	pthread_mutex_unlock(&(file_lock[idx_file & MAX_NUM_FILE_OP_LOCK_M1]));

	nBytes_Written = 0;

	if(nDirectPointer == 0)	{	// a new file
//		memcpy(pNewBuff, buf, count);
		memcpy(pNewBuff+offset, buf, count);
		fd_List[fd].Offset += count;
		nBytes_Written = count;
		if(count == pFileMetaInfo->DiretData[0].DataBlockSize)	fd_List[fd].idx_block++;
	}
	else	{
		while(Done == 0)	{
			if(fd_List[fd].idx_block < NUM_DIRCT_PT)	{
				if( (pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset + pFileMetaInfo->DiretData[fd_List[fd].idx_block].DataBlockSize - fd_List[fd].Offset) <= count)	{	// need to go on
					nBytes_Written_OneTime = pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset + pFileMetaInfo->DiretData[fd_List[fd].idx_block].DataBlockSize - fd_List[fd].Offset;
					memcpy((void*)(pFileMetaInfo->DiretData[fd_List[fd].idx_block].AddressofData + fd_List[fd].Offset - pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset), (char*)buf+nBytes_Written, nBytes_Written_OneTime);
					fd_List[fd].idx_block++;	// move to next block!!!
				}
				else	{
					nBytes_Written_OneTime = count;
					memcpy((void*)(pFileMetaInfo->DiretData[fd_List[fd].idx_block].AddressofData + fd_List[fd].Offset - pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset), (char*)buf+nBytes_Written, nBytes_Written_OneTime);
				}
//				nBytes_Written_OneTime = min(pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset + pFileMetaInfo->DiretData[fd_List[fd].idx_block].DataBlockSize - fd_List[fd].Offset, count);
				nBytes_Written += nBytes_Written_OneTime;
				fd_List[fd].Offset += nBytes_Written_OneTime;
				count -= nBytes_Written_OneTime;
			}
			if( (fd_List[fd].idx_block >= NUM_DIRCT_PT) && (pFileMetaInfo->nExtraPointer > 0) )	{	// within extra blocks!!!
				if( (pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset + pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].DataBlockSize - fd_List[fd].Offset) <= count)	{	// need to go on
					nBytes_Written_OneTime = pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset + pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].DataBlockSize - fd_List[fd].Offset;
					memcpy((void*)(pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].AddressofData + fd_List[fd].Offset - pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset), (char*)buf+nBytes_Written, nBytes_Written_OneTime);
					fd_List[fd].idx_block++;	// move to next block!!!
				}
				else	{
					nBytes_Written_OneTime = count;
					memcpy((void*)(pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].AddressofData + fd_List[fd].Offset - pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset), (char*)buf+nBytes_Written, nBytes_Written_OneTime);
				}
				nBytes_Written += nBytes_Written_OneTime;
				fd_List[fd].Offset += nBytes_Written_OneTime;
				count -= nBytes_Written_OneTime;
			}
			if( nBytes_Written >= count_save )	{
				Done = 1;
				break;
			}
		}
	}
//	pthread_mutex_lock(&file_lock);
//	pFileMetaInfo->st_size = max(pFileMetaInfo->st_size, nOffsetMax);	// update file size
//	pthread_mutex_unlock(&file_lock);

	return nBytes_Written;
}

// loc_buf - the buffer was registered. rem_buf - the address on client side. dest_buf - the destination address in file system. 
inline void my_Adaptive_Write(int idx_qp, void *loc_buf, unsigned int lkey, void *rem_buf, unsigned int rkey, size_t count, void *dest_buf)
{
	unsigned long int offset;
	int i, nBlocks, nBlocksM1, BlockSize;
	struct ibv_mr *mr_tmp;
	char *pDest;

	if(count > MAX_SIZE_MR_BLOCK)	{	// multiple times RDMA
		nBlocks = (count % MAX_SIZE_MR_BLOCK) ? ( (count / MAX_SIZE_MR_BLOCK) + 1) : (count / MAX_SIZE_MR_BLOCK);
		nBlocksM1 = nBlocks - 1;

		offset = 0;
		for(i=0; i<nBlocksM1; i++)	{
			offset += MAX_SIZE_MR_BLOCK;
			pDest = (char*)dest_buf + offset;
			mr_tmp = Server_qp.IB_RegisterBuf_RW_Local_Remote(pDest, MAX_SIZE_MR_BLOCK);
			assert(mr_tmp != NULL);
			Server_qp.IB_Get(idx_qp, (void*)pDest, mr_tmp->lkey, (void*)((char*)rem_buf+offset), rkey, MAX_SIZE_MR_BLOCK);

//			nSizeReg -= mr_tmp->length;
//			printf("DBG> nSizeReg = %ld\n", nSizeReg);

			ibv_dereg_mr(mr_tmp);
		}
		BlockSize = count % MAX_SIZE_MR_BLOCK;
		offset += MAX_SIZE_MR_BLOCK;
		pDest = (char*)dest_buf + offset;
		if(BlockSize > DATA_COPY_THRESHOLD_SIZE)	{
			mr_tmp = Server_qp.IB_RegisterBuf_RW_Local_Remote(pDest, BlockSize);
			assert(mr_tmp != NULL);
			Server_qp.IB_Get(idx_qp, (void*)pDest, mr_tmp->lkey, (void*)((char*)rem_buf+offset), rkey, BlockSize);

//			nSizeReg -= mr_tmp->length;
//			printf("DBG> nSizeReg = %ld\n", nSizeReg);

			ibv_dereg_mr(mr_tmp);
		}
		else	{
			Server_qp.IB_Get(idx_qp, loc_buf, lkey, (void*)((char*)rem_buf+offset), rkey, BlockSize);	// loc_buf was registered previously. 
			memcpy((void*)pDest, loc_buf, BlockSize);
		}
	}
	else if(count > DATA_COPY_THRESHOLD_SIZE)	{	// RDMA
		mr_tmp = Server_qp.IB_RegisterBuf_RW_Local_Remote(dest_buf, count);
		assert(mr_tmp != NULL);
		Server_qp.IB_Get(idx_qp, (void*)dest_buf, mr_tmp->lkey, (void*)rem_buf, rkey, count);

//			nSizeReg -= mr_tmp->length;
//			printf("DBG> nSizeReg = %ld\n", nSizeReg);

			ibv_dereg_mr(mr_tmp);
	}
	else	{	// use register local buffer RMDA then memcpy. 
		Server_qp.IB_Get(idx_qp, loc_buf, lkey, rem_buf, rkey, count);	// loc_buf was registered previously. 
		memcpy(dest_buf, loc_buf, count);
	}
}

size_t my_write_RDMA(int fd, int idx_qp, void *loc_buf, unsigned int lkey, void *rem_buf, unsigned int rkey, size_t count, off_t offset)
{
	int Done = 0, i, nDirectPointer, nExtraPointer, nExtraPointerNewlyAllocated, idx_file;
	size_t nBytes_Written, nBytes_Written_OneTime, nOffsetMax, nAllocatedSize, nBytesJustAllocated, nPrevOffset, count_save;
	META_INFO *pFileMetaInfo;
	void *pNewBuff=NULL;
	DirectPointer *pExtraData_Org;

	count_save = count;
	idx_file = fd_List[fd].idx_file;
	pFileMetaInfo = &(pMetaData[idx_file]);

//	pthread_mutex_lock(&(file_lock[fd & MAX_NUM_FILE_OP_LOCK_M1]));
	pthread_mutex_lock(&(file_lock[idx_file & MAX_NUM_FILE_OP_LOCK_M1]));

	nAllocatedSize = pFileMetaInfo->nSizeAllocated;
	nOffsetMax = offset + count;
//	nOffsetMax = fd_List[fd].Offset + count;	// the tentative end of this file
	nDirectPointer = pFileMetaInfo->nDirectPointer;
	if(nOffsetMax > nAllocatedSize)	{	// need allocate new pages to accomodate new incoming data.
		pNewBuff = pMem_Allocator->Mem_Alloc(nOffsetMax - nAllocatedSize, &nBytesJustAllocated);
		assert( (pNewBuff != NULL) && (nBytesJustAllocated > 0) );
//		pFileMetaInfo->nSizeAllocated += nBytesJustAllocated;
//		pFileMetaInfo->st_blocks = (pFileMetaInfo->nSizeAllocated) >> 9;	// the number of blocks of 512 bytes

		if(nDirectPointer < NUM_DIRCT_PT)	{	// append this pointer
			pFileMetaInfo->DiretData[nDirectPointer].AddressofData = (ULongInt)pNewBuff;
			pFileMetaInfo->DiretData[nDirectPointer].FileOffset = (nDirectPointer > 0) ? (pFileMetaInfo->DiretData[nDirectPointer-1].FileOffset + pFileMetaInfo->DiretData[nDirectPointer-1].DataBlockSize) : (0L);
			pFileMetaInfo->DiretData[nDirectPointer].DataBlockSize = nBytesJustAllocated;
			pFileMetaInfo->nDirectPointer++;
		}
		else	{
			nExtraPointer = pFileMetaInfo->nExtraPointer;
			if(pFileMetaInfo->nMaxExtraPointer <= nExtraPointer)	{	// Need to reallocate the storage for pExtraData[]
				pExtraData_Org = pFileMetaInfo->pExtraData;
				nExtraPointerNewlyAllocated = pFileMetaInfo->nMaxExtraPointer + max((pFileMetaInfo->nMaxExtraPointer)>>2, DEFAULT_NUM_EXTRA_PT);
				pFileMetaInfo->pExtraData = (DirectPointer *)ncx_slab_alloc(sp_ExtraPointers, nExtraPointerNewlyAllocated*sizeof(DirectPointer));
				if(nExtraPointer)	memcpy(pFileMetaInfo->pExtraData, pExtraData_Org, sizeof(DirectPointer)*nExtraPointer);
				if(pExtraData_Org)	ncx_slab_free(sp_ExtraPointers, pExtraData_Org);
				pFileMetaInfo->nMaxExtraPointer = nExtraPointerNewlyAllocated;
			}
			pFileMetaInfo->pExtraData[nExtraPointer].AddressofData = (ULongInt)pNewBuff;
			pFileMetaInfo->pExtraData[nExtraPointer].FileOffset = (nExtraPointer > 0) ? (pFileMetaInfo->pExtraData[nExtraPointer-1].FileOffset + pFileMetaInfo->pExtraData[nExtraPointer-1].DataBlockSize) : (pFileMetaInfo->DiretData[NUM_DIRCT_PT-1].FileOffset + pFileMetaInfo->DiretData[NUM_DIRCT_PT-1].DataBlockSize);
			pFileMetaInfo->pExtraData[nExtraPointer].DataBlockSize = nBytesJustAllocated;
			pFileMetaInfo->nExtraPointer++;
		}
		pFileMetaInfo->nSizeAllocated += nBytesJustAllocated;
		pFileMetaInfo->st_blocks = (pFileMetaInfo->nSizeAllocated) >> 9;	// the number of blocks of 512 bytes
	}

	pFileMetaInfo->st_size = max(pFileMetaInfo->st_size, nOffsetMax);	// update file size
	Determine_Index_StorageBlock_for_Offset(fd, offset);

	pthread_mutex_unlock(&(file_lock[idx_file & MAX_NUM_FILE_OP_LOCK_M1]));

	nBytes_Written = 0;

	if(nDirectPointer == 0)	{	// a new file
//		memcpy(pNewBuff, buf, count);
		my_Adaptive_Write(idx_qp, loc_buf, lkey, rem_buf, rkey, count, pNewBuff + offset);
		fd_List[fd].Offset += count;
		nBytes_Written = count;
		if(count == pFileMetaInfo->DiretData[0].DataBlockSize)	fd_List[fd].idx_block++;
	}
	else	{
		while(Done == 0)	{
			if(fd_List[fd].idx_block < NUM_DIRCT_PT)	{
				if( (pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset + pFileMetaInfo->DiretData[fd_List[fd].idx_block].DataBlockSize - fd_List[fd].Offset) <= count)	{	// need to go on
					nBytes_Written_OneTime = pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset + pFileMetaInfo->DiretData[fd_List[fd].idx_block].DataBlockSize - fd_List[fd].Offset;
//					memcpy((void*)(pFileMetaInfo->DiretData[fd_List[fd].idx_block].AddressofData + fd_List[fd].Offset - pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset), (char*)buf+nBytes_Written, nBytes_Written_OneTime);
					my_Adaptive_Write(idx_qp, loc_buf, lkey, (char*)rem_buf+nBytes_Written, rkey, nBytes_Written_OneTime, (void*)(pFileMetaInfo->DiretData[fd_List[fd].idx_block].AddressofData + fd_List[fd].Offset - pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset));
					fd_List[fd].idx_block++;	// move to next block!!!
				}
				else	{
					nBytes_Written_OneTime = count;
//					memcpy((void*)(pFileMetaInfo->DiretData[fd_List[fd].idx_block].AddressofData + fd_List[fd].Offset - pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset), (char*)buf+nBytes_Written, nBytes_Written_OneTime);
					my_Adaptive_Write(idx_qp, loc_buf, lkey, (char*)rem_buf+nBytes_Written, rkey, nBytes_Written_OneTime, (void*)(pFileMetaInfo->DiretData[fd_List[fd].idx_block].AddressofData + fd_List[fd].Offset - pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset));
				}
//				nBytes_Written_OneTime = min(pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset + pFileMetaInfo->DiretData[fd_List[fd].idx_block].DataBlockSize - fd_List[fd].Offset, count);
				nBytes_Written += nBytes_Written_OneTime;
				fd_List[fd].Offset += nBytes_Written_OneTime;
				count -= nBytes_Written_OneTime;
			}
			if( (fd_List[fd].idx_block >= NUM_DIRCT_PT) && (pFileMetaInfo->nExtraPointer > 0) )	{	// within extra blocks!!!
				if( (pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset + pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].DataBlockSize - fd_List[fd].Offset) <= count)	{	// need to go on
					nBytes_Written_OneTime = pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset + pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].DataBlockSize - fd_List[fd].Offset;
					my_Adaptive_Write(idx_qp, loc_buf, lkey, (char*)rem_buf+nBytes_Written, rkey, nBytes_Written_OneTime, (void*)(pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].AddressofData + fd_List[fd].Offset - pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset));
//					memcpy((void*)(pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].AddressofData + fd_List[fd].Offset - pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset), (char*)buf+nBytes_Written, nBytes_Written_OneTime);
					fd_List[fd].idx_block++;	// move to next block!!!
				}
				else	{
					nBytes_Written_OneTime = count;
//					memcpy((void*)(pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].AddressofData + fd_List[fd].Offset - pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset), (char*)buf+nBytes_Written, nBytes_Written_OneTime);
					my_Adaptive_Write(idx_qp, loc_buf, lkey, (char*)rem_buf+nBytes_Written, rkey, nBytes_Written_OneTime, (void*)(pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].AddressofData + fd_List[fd].Offset - pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset));
				}
				nBytes_Written += nBytes_Written_OneTime;
				fd_List[fd].Offset += nBytes_Written_OneTime;
				count -= nBytes_Written_OneTime;
			}
			if( nBytes_Written >= count_save )	{
				Done = 1;
				break;
			}
		}
	}
//	pthread_mutex_lock(&file_lock);
//	pFileMetaInfo->st_size = max(pFileMetaInfo->st_size, nOffsetMax);	// update file size
//	pthread_mutex_unlock(&file_lock);
	
	return nBytes_Written;
}

size_t my_read(int fd, void *buf, size_t count, off_t offset)
{
	int Done = 0, i, nExtraPointer, idx_file;
	size_t nBytes_Read, nBytes_Read_OneTime, nAllocatedSize, nPrevOffset, count_save, nFileSize, nBytesLeft, nBytesLeftInThisBlock;
	META_INFO *pFileMetaInfo;
//	Determine_Index_StorageBlock_for_Offset(fd, fd_List[fd].Offset);

	count_save = count;
	idx_file = fd_List[fd].idx_file;
	pFileMetaInfo = &(pMetaData[idx_file]);

//	pthread_mutex_lock(&(file_lock[fd & MAX_NUM_FILE_OP_LOCK_M1]));
	pthread_mutex_lock(&(file_lock[idx_file & MAX_NUM_FILE_OP_LOCK_M1]));
	Determine_Index_StorageBlock_for_Offset(fd, offset);

	nAllocatedSize = pFileMetaInfo->nSizeAllocated;
	nFileSize = pFileMetaInfo->st_size;
	
	nBytes_Read = 0;
	nBytesLeft = nFileSize - fd_List[fd].Offset;

	pthread_mutex_unlock(&(file_lock[idx_file & MAX_NUM_FILE_OP_LOCK_M1]));

	if(pFileMetaInfo->nDirectPointer == 0)	{	// a new file
		return 0;	// no data available
	}
	else	{
		while(Done == 0)	{
			if(fd_List[fd].idx_block < NUM_DIRCT_PT)	{
				nBytesLeftInThisBlock = pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset + pFileMetaInfo->DiretData[fd_List[fd].idx_block].DataBlockSize - fd_List[fd].Offset;
//				if(count <= nBytesLeftInThisBlock)	{	// no more blocks are needed. 
				if(count < nBytesLeftInThisBlock)	{	// no more blocks are needed. 
					nBytes_Read_OneTime = min(count, nBytesLeft);
					memcpy((char*)buf+nBytes_Read, (void*)(pFileMetaInfo->DiretData[fd_List[fd].idx_block].AddressofData + fd_List[fd].Offset - pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset), nBytes_Read_OneTime);
				}
				else	{
					if(nBytesLeft > nBytesLeftInThisBlock)	{	// Need to access next block!!!
						nBytes_Read_OneTime = nBytesLeftInThisBlock;
						memcpy((char*)buf+nBytes_Read, (void*)(pFileMetaInfo->DiretData[fd_List[fd].idx_block].AddressofData + fd_List[fd].Offset - pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset), nBytes_Read_OneTime);
						fd_List[fd].idx_block++;	// move to next block!!!
					}
					else	{	// No need to access next block!!!
						nBytes_Read_OneTime = nBytesLeft;
						memcpy((char*)buf+nBytes_Read, (void*)(pFileMetaInfo->DiretData[fd_List[fd].idx_block].AddressofData + fd_List[fd].Offset - pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset), nBytes_Read_OneTime);
					}
				}
				nBytes_Read += nBytes_Read_OneTime;
				fd_List[fd].Offset += nBytes_Read_OneTime;
				count -= nBytes_Read_OneTime;
				nBytesLeft -= nBytes_Read_OneTime;
			}
			if( (nBytesLeft == 0) || (nBytes_Read >= count_save) )	{	// reaching the end of file or finished reading requested size
				Done = 1;
				break;
			}
			if(fd_List[fd].idx_block >= NUM_DIRCT_PT)	{	// within extra blocks!!!
				nBytesLeftInThisBlock = pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset + pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].DataBlockSize - fd_List[fd].Offset;
//				if(count <= nBytesLeftInThisBlock)	{	// no more blocks are needed. 
				if(count < nBytesLeftInThisBlock)	{	// no more blocks are needed. 
					nBytes_Read_OneTime = min(count, nBytesLeft);
					memcpy((char*)buf+nBytes_Read, (void*)(pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].AddressofData + fd_List[fd].Offset - pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset), nBytes_Read_OneTime);
				}
				else	{
					if(nBytesLeft > nBytesLeftInThisBlock)	{	// Need to access next block!!!
						nBytes_Read_OneTime = nBytesLeftInThisBlock;
						memcpy((char*)buf+nBytes_Read, (void*)(pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].AddressofData + fd_List[fd].Offset - pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset), nBytes_Read_OneTime);
						fd_List[fd].idx_block++;	// move to next block!!!
					}
					else	{	// No need to access next block!!!
						nBytes_Read_OneTime = nBytesLeft;
						memcpy((char*)buf+nBytes_Read, (void*)(pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].AddressofData + fd_List[fd].Offset - pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset), nBytes_Read_OneTime);
					}
				}
				nBytes_Read += nBytes_Read_OneTime;
				fd_List[fd].Offset += nBytes_Read_OneTime;
				count -= nBytes_Read_OneTime;
				nBytesLeft -= nBytes_Read_OneTime;

				if( (nBytesLeft == 0) || (nBytes_Read >= count_save) )	{	// reaching the end of file or finished reading requested size
					Done = 1;
					break;
				}
			}
		}
	}

	return nBytes_Read;
}

// loc_buf - the buffer was registered. rem_buf - the address on client side. src_buf - the source address in file system. 
inline void my_Adaptive_Read(int idx_qp, void *loc_buf, unsigned int lkey, void *rem_buf, unsigned int rkey, size_t count, void *src_buf)
{
	unsigned long int offset;
	int i, nBlocks, nBlocksM1, BlockSize;
	struct ibv_mr *mr_tmp;
	char *pSrc;

	if(count > MAX_SIZE_MR_BLOCK)	{	// multiple times RDMA
		nBlocks = (count % MAX_SIZE_MR_BLOCK) ? ( (count / MAX_SIZE_MR_BLOCK) + 1) : (count / MAX_SIZE_MR_BLOCK);
		nBlocksM1 = nBlocks - 1;

		offset = 0;
		for(i=0; i<nBlocksM1; i++)	{
			offset += MAX_SIZE_MR_BLOCK;
			pSrc = (char*)src_buf + offset;
			mr_tmp = Server_qp.IB_RegisterBuf_RW_Local_Remote(pSrc, MAX_SIZE_MR_BLOCK);
			assert(mr_tmp != NULL);
			Server_qp.IB_Put(idx_qp, (void*)pSrc, mr_tmp->lkey, (void*)((char*)rem_buf+offset), rkey, MAX_SIZE_MR_BLOCK);

//			nSizeReg -= mr_tmp->length;
//			printf("DBG> nSizeReg = %ld\n", nSizeReg);

			ibv_dereg_mr(mr_tmp);
		}
		BlockSize = count % MAX_SIZE_MR_BLOCK;
		offset += MAX_SIZE_MR_BLOCK;
		pSrc = (char*)src_buf + offset;
		if(BlockSize > DATA_COPY_THRESHOLD_SIZE)	{
			mr_tmp = Server_qp.IB_RegisterBuf_RW_Local_Remote(pSrc, BlockSize);
			assert(mr_tmp != NULL);
			Server_qp.IB_Put(idx_qp, (void*)pSrc, mr_tmp->lkey, (void*)((char*)rem_buf+offset), rkey, BlockSize);


//			nSizeReg -= mr_tmp->length;
//			printf("DBG> nSizeReg = %ld\n", nSizeReg);

			ibv_dereg_mr(mr_tmp);
		}
		else	{
			memcpy(loc_buf, pSrc, BlockSize);
			Server_qp.IB_Put(idx_qp, loc_buf, lkey, (void*)((char*)rem_buf+offset), rkey, BlockSize);	// loc_buf was registered previously. Will do later
		}
	}
	else if(count > DATA_COPY_THRESHOLD_SIZE)	{	// RDMA
		mr_tmp = Server_qp.IB_RegisterBuf_RW_Local_Remote(src_buf, count);
		assert(mr_tmp != NULL);
		Server_qp.IB_Put(idx_qp, (void*)src_buf, mr_tmp->lkey, (void*)rem_buf, rkey, count);


//			nSizeReg -= mr_tmp->length;
//			printf("DBG> nSizeReg = %ld\n", nSizeReg);

		ibv_dereg_mr(mr_tmp);
	}
	else	{	// use register local buffer RMDA then memcpy. 
		memcpy(loc_buf, src_buf, count);
		Server_qp.IB_Put(idx_qp, loc_buf, lkey, rem_buf, rkey, count);	// loc_buf was registered previously. 
	}
}

size_t my_read_RDMA(int fd, int idx_qp, void *loc_buf, unsigned int lkey, void *rem_buf, unsigned int rkey, size_t count, off_t offset)
{
	int Done = 0, i, nExtraPointer, idx_file;
	size_t nBytes_Read, nBytes_Read_OneTime, nAllocatedSize, nPrevOffset, count_save, nFileSize, nBytesLeft, nBytesLeftInThisBlock;
	META_INFO *pFileMetaInfo;
//	Determine_Index_StorageBlock_for_Offset(fd, fd_List[fd].Offset);

	count_save = count;
	idx_file = fd_List[fd].idx_file;
	pFileMetaInfo = &(pMetaData[idx_file]);

//	pthread_mutex_lock(&(file_lock[fd & MAX_NUM_FILE_OP_LOCK_M1]));
	pthread_mutex_lock(&(file_lock[idx_file & MAX_NUM_FILE_OP_LOCK_M1]));
	Determine_Index_StorageBlock_for_Offset(fd, offset);

	nAllocatedSize = pFileMetaInfo->nSizeAllocated;
	nFileSize = pFileMetaInfo->st_size;
	
	nBytesLeft = nFileSize - fd_List[fd].Offset;

	pthread_mutex_unlock(&(file_lock[idx_file & MAX_NUM_FILE_OP_LOCK_M1]));

	nBytes_Read = 0;

	if(pFileMetaInfo->nDirectPointer == 0)	{	// a new file
		return 0;	// no data available
	}
	else	{
		while(Done == 0)	{
			if(fd_List[fd].idx_block < NUM_DIRCT_PT)	{
				nBytesLeftInThisBlock = pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset + pFileMetaInfo->DiretData[fd_List[fd].idx_block].DataBlockSize - fd_List[fd].Offset;
//				if(count <= nBytesLeftInThisBlock)	{	// no more blocks are needed. 
				if(count < nBytesLeftInThisBlock)	{	// no more blocks are needed. 
					nBytes_Read_OneTime = min(count, nBytesLeft);
//					memcpy((char*)buf+nBytes_Read, (void*)(pFileMetaInfo->DiretData[fd_List[fd].idx_block].AddressofData + fd_List[fd].Offset - pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset), nBytes_Read_OneTime);
					my_Adaptive_Read(idx_qp, loc_buf, lkey, (void*)((char*)rem_buf+nBytes_Read), rkey, nBytes_Read_OneTime, (void*)(pFileMetaInfo->DiretData[fd_List[fd].idx_block].AddressofData + fd_List[fd].Offset - pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset));
				}
				else	{
					if(nBytesLeft > nBytesLeftInThisBlock)	{	// Need to access next block!!!
						nBytes_Read_OneTime = nBytesLeftInThisBlock;
//						memcpy((char*)buf+nBytes_Read, (void*)(pFileMetaInfo->DiretData[fd_List[fd].idx_block].AddressofData + fd_List[fd].Offset - pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset), nBytes_Read_OneTime);
						my_Adaptive_Read(idx_qp, loc_buf, lkey, (void*)((char*)rem_buf+nBytes_Read), rkey, nBytes_Read_OneTime, (void*)(pFileMetaInfo->DiretData[fd_List[fd].idx_block].AddressofData + fd_List[fd].Offset - pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset));
						fd_List[fd].idx_block++;	// move to next block!!!
					}
					else	{	// No need to access next block!!!
						nBytes_Read_OneTime = nBytesLeft;
//						memcpy((char*)buf+nBytes_Read, (void*)(pFileMetaInfo->DiretData[fd_List[fd].idx_block].AddressofData + fd_List[fd].Offset - pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset), nBytes_Read_OneTime);
						my_Adaptive_Read(idx_qp, loc_buf, lkey, (void*)((char*)rem_buf+nBytes_Read), rkey, nBytes_Read_OneTime, (void*)(pFileMetaInfo->DiretData[fd_List[fd].idx_block].AddressofData + fd_List[fd].Offset - pFileMetaInfo->DiretData[fd_List[fd].idx_block].FileOffset));
					}
				}
				nBytes_Read += nBytes_Read_OneTime;
				fd_List[fd].Offset += nBytes_Read_OneTime;
				count -= nBytes_Read_OneTime;
				nBytesLeft -= nBytes_Read_OneTime;
			}
			if( (nBytesLeft == 0) || (nBytes_Read >= count_save) )	{	// reaching the end of file or finished reading requested size
				Done = 1;
				break;
			}
			if(fd_List[fd].idx_block >= NUM_DIRCT_PT)	{	// within extra blocks!!!
				nBytesLeftInThisBlock = pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset + pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].DataBlockSize - fd_List[fd].Offset;
//				if(count <= nBytesLeftInThisBlock)	{	// no more blocks are needed. 
				if(count < nBytesLeftInThisBlock)	{	// no more blocks are needed. 
					nBytes_Read_OneTime = min(count, nBytesLeft);
//					memcpy((char*)buf+nBytes_Read, (void*)(pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].AddressofData + fd_List[fd].Offset - pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset), nBytes_Read_OneTime);
					my_Adaptive_Read(idx_qp, loc_buf, lkey, (void*)((char*)rem_buf+nBytes_Read), rkey, nBytes_Read_OneTime, (void*)(pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].AddressofData + fd_List[fd].Offset - pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset));
				}
				else	{
					if(nBytesLeft > nBytesLeftInThisBlock)	{	// Need to access next block!!!
						nBytes_Read_OneTime = nBytesLeftInThisBlock;
//						memcpy((char*)buf+nBytes_Read, (void*)(pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].AddressofData + fd_List[fd].Offset - pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset), nBytes_Read_OneTime);
						my_Adaptive_Read(idx_qp, loc_buf, lkey, (void*)((char*)rem_buf+nBytes_Read), rkey, nBytes_Read_OneTime, (void*)(pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].AddressofData + fd_List[fd].Offset - pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset));
						fd_List[fd].idx_block++;	// move to next block!!!
					}
					else	{	// No need to access next block!!!
						nBytes_Read_OneTime = nBytesLeft;
//						memcpy((char*)buf+nBytes_Read, (void*)(pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].AddressofData + fd_List[fd].Offset - pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset), nBytes_Read_OneTime);
						my_Adaptive_Read(idx_qp, loc_buf, lkey, (void*)((char*)rem_buf+nBytes_Read), rkey, nBytes_Read_OneTime, (void*)(pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].AddressofData + fd_List[fd].Offset - pFileMetaInfo->pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset));
					}
				}
				nBytes_Read += nBytes_Read_OneTime;
				fd_List[fd].Offset += nBytes_Read_OneTime;
				count -= nBytes_Read_OneTime;
				nBytesLeft -= nBytes_Read_OneTime;

				if( (nBytesLeft == 0) || (nBytes_Read >= count_save) )	{	// reaching the end of file or finished reading requested size
					Done = 1;
					break;
				}
			}
		}
	}

	return nBytes_Read;
}

void Determine_Index_StorageBlock_for_Offset(int fd, off_t offset)
{
	int i, nDirectPointer, nExtraPointer, left, mid, right, Done = 0;
	long int nAllocatedSize, nFileSize;
	META_INFO *pFileMetaInfo;

	pFileMetaInfo = &(pMetaData[fd_List[fd].idx_file]);
	nDirectPointer = pFileMetaInfo->nDirectPointer;
	nExtraPointer = pFileMetaInfo->nExtraPointer;
	nAllocatedSize = pFileMetaInfo->nSizeAllocated;
	nFileSize = pFileMetaInfo->st_size;

	if(nDirectPointer == 0)	{
		fd_List[fd].idx_block = 0;	// empty file
		fd_List[fd].Offset = offset;
		return;
	}

	if(offset >= nAllocatedSize)	{	// Set after the last index
		fd_List[fd].idx_block = pFileMetaInfo->nExtraPointer + pFileMetaInfo->nDirectPointer;
		fd_List[fd].Offset = offset;
	}
//	else if(offset >= nFileSize)	{
//		fd_List[fd].idx_block = pFileMetaInfo->nExtraPointer + pFileMetaInfo->nDirectPointer - 1;
//		fd_List[fd].Offset = offset;
//	}
	else if(offset >= ( pFileMetaInfo->DiretData[nDirectPointer-1].FileOffset + pFileMetaInfo->DiretData[nDirectPointer-1].DataBlockSize ) )	{	// in extra data region
//		if(nExtraPointer <= 4)	{	// direct ordered search
//			for(i=0; i<nExtraPointer; i++)	{	// ordered search from front to end
//				if( (offset >= pFileMetaInfo->pExtraData[i].FileOffset) && ( offset < (pFileMetaInfo->pExtraData[i].FileOffset + pFileMetaInfo->pExtraData[i].DataBlockSize ) ) )	{
//					fd_List[fd].idx_block = i + NUM_DIRCT_PT;
//					break;
//				}
//			}
//		}
//		else	{	// Bisearch
			left = 0;
			right = nExtraPointer - 1;
			mid = (left + right) >> 1;	// (left + right)/2
			Done = 0;
			while(1)	{
				if(offset < pFileMetaInfo->pExtraData[mid].FileOffset)	{
					right = mid;
				}
				else	{
					left = mid;
				}
				mid = (left + right) >> 1;
				if( (left + 1) >= right )	{
					if( (offset>=pFileMetaInfo->pExtraData[left].FileOffset) && ( offset < (pFileMetaInfo->pExtraData[left].FileOffset + pFileMetaInfo->pExtraData[left].DataBlockSize) ) )	{
						fd_List[fd].idx_block = left + NUM_DIRCT_PT;
						fd_List[fd].Offset = offset;
						Done = 1;
					}
					else if( (offset>=pFileMetaInfo->pExtraData[right].FileOffset) && ( offset < (pFileMetaInfo->pExtraData[right].FileOffset + pFileMetaInfo->pExtraData[right].DataBlockSize) ) )	{
						fd_List[fd].idx_block = right + NUM_DIRCT_PT;
						fd_List[fd].Offset = offset;
						Done = 1;
					}
					if(! Done)	{
						printf("ERROR> Fail to determine the index of data block! (%d, %td)\n", fd, offset);
					}
					break;
				}
			}
//		}
	}
	else	{	// in direct data region
		for(i=0; i<nDirectPointer; i++)	{	// ordered search from front to end
			if( (offset >= pFileMetaInfo->DiretData[i].FileOffset) && (offset < (pFileMetaInfo->DiretData[i].FileOffset + pFileMetaInfo->DiretData[i].DataBlockSize ) ) )	{
				fd_List[fd].idx_block = i;
				fd_List[fd].Offset = offset;
				break;
			}
		}
	}
}

int Query_Index_StorageBlock_with_Offset(int idx_file, off_t offset)	// return idx_block
{
	int i, nDirectPointer, nExtraPointer, left, mid, right, Done = 0;
	long int nAllocatedSize, nFileSize;
	META_INFO *pFileMetaInfo;

	pFileMetaInfo = &(pMetaData[idx_file]);
	nDirectPointer = pFileMetaInfo->nDirectPointer;
	nExtraPointer = pFileMetaInfo->nExtraPointer;
	nAllocatedSize = pFileMetaInfo->nSizeAllocated;
	nFileSize = pFileMetaInfo->st_size;

	if(nDirectPointer == 0)	{	// empty file! 
		return 0;	// New size is larger than zero since zero size has been handled already. 
	}

	if(offset > nAllocatedSize)	{	// Set after the last index
		return ( pFileMetaInfo->nExtraPointer + pFileMetaInfo->nDirectPointer );	// need to allocate storage and fill with zero!!!
	}
	else if(offset == nAllocatedSize)	{	// the last index
		return ( pFileMetaInfo->nExtraPointer + pFileMetaInfo->nDirectPointer - 1 );
	}
	else if(offset > ( pFileMetaInfo->DiretData[nDirectPointer-1].FileOffset + pFileMetaInfo->DiretData[nDirectPointer-1].DataBlockSize ) )	{	// in extra data region
		left = 0;
		right = nExtraPointer - 1;
		mid = (left + right) >> 1;	// (left + right)/2
		Done = 0;
		while(1)	{
			if(offset < pFileMetaInfo->pExtraData[mid].FileOffset)	{
				right = mid;
			}
			else	{
				left = mid;
			}
			mid = (left + right) >> 1;
			if( (left + 1) >= right )	{
				if( (offset>pFileMetaInfo->pExtraData[left].FileOffset) && ( offset <= (pFileMetaInfo->pExtraData[left].FileOffset + pFileMetaInfo->pExtraData[left].DataBlockSize) ) )	{
					Done = 1;
					return (left + NUM_DIRCT_PT);
				}
				else if( (offset>pFileMetaInfo->pExtraData[right].FileOffset) && ( offset <= (pFileMetaInfo->pExtraData[right].FileOffset + pFileMetaInfo->pExtraData[right].DataBlockSize) ) )	{
					Done = 1;
					return (right + NUM_DIRCT_PT);
				}
				if(! Done)	{
					printf("ERROR> Fail to determine the index of data block! (file_idx = %d, %td)\n", idx_file, offset);
				}
				break;
			}
		}
	}
	else	{	// in direct data region
		for(i=0; i<nDirectPointer; i++)	{	// ordered search from front to end
			if( (offset > pFileMetaInfo->DiretData[i].FileOffset) && (offset <= (pFileMetaInfo->DiretData[i].FileOffset + pFileMetaInfo->DiretData[i].DataBlockSize ) ) )	{
				return i;
			}
		}
	}
}

off_t my_lseek(int fd, off_t offset, int whence)
{
	int i;
	off_t new_offset;

	switch(whence)	{
	case SEEK_SET:
		new_offset = offset;
		break;
	case SEEK_CUR:
		new_offset = fd_List[fd].Offset + offset;
		break;
	case SEEK_END:
		new_offset = pMetaData[fd_List[fd].idx_file].st_size + offset;
		break;
	default:
		printf("ERROR> Unknown parameter for lseek(). (%d, %td, %d)\n", fd, offset, whence);
		new_offset = fd_List[fd].Offset;
		break;
	}

	if(new_offset < 0)	{
		errno = EINVAL;
		return (-1);
	}
	else if(new_offset == fd_List[fd].Offset)	{	// do nothing
		return new_offset;
	}
	else	{
		pthread_mutex_lock(&(file_lock[fd_List[fd].idx_file & MAX_NUM_FILE_OP_LOCK_M1]));
		Determine_Index_StorageBlock_for_Offset(fd, new_offset);
		pthread_mutex_unlock(&(file_lock[fd_List[fd].idx_file & MAX_NUM_FILE_OP_LOCK_M1]));
		return new_offset;
	}
}

void my_RemoveEntryInfo(int my_file_idx)
{
	int idx_Parent_dir, IdxEntry_in_Dir, nEntry, *p_nEntryNameOffset, idx_file_need_update_idx_in_dir_entry, nLenEntryToRemove;
	char *p_EntryName, szFile_ToMove[DEFAULT_FULL_FILE_NAME_LEN];
	unsigned long long file_ToMove_hash = 0;

	idx_Parent_dir = pMetaData[my_file_idx].idx_Parent_Dir;
	assert(idx_Parent_dir>=0);

	pthread_mutex_lock(&(dir_entry_lock[idx_Parent_dir & MAX_NUM_FILE_OP_LOCK_M1]));

//	if(strcmp(pDirMetaData[idx_Parent_dir].szDirName, "/myfs/mdtets/test-dir.0-0/mdtest_tree.0")==0)	printf("DBG> Del %s\n", pMetaData[my_file_idx].szFileName);

	IdxEntry_in_Dir = pMetaData[my_file_idx].IdxEntry_in_Dir;
	nEntry = pDirMetaData[idx_Parent_dir].nEntries;
	p_nEntryNameOffset = (int *)(p_DirEntryNameOffsetBuff + pDirMetaData[idx_Parent_dir].nOffset_To_EntryNameOffsetList);

	nLenEntryToRemove = strlen(p_DirEntryNameBuff + p_nEntryNameOffset[IdxEntry_in_Dir] + 1) + 2;
	if(IdxEntry_in_Dir != (nEntry - 1) )	{	// not the last entry? Move the last entry to this spot!
		p_EntryName = p_DirEntryNameBuff + p_nEntryNameOffset[IdxEntry_in_Dir];	// to be freed
		p_nEntryNameOffset[IdxEntry_in_Dir] = p_nEntryNameOffset[nEntry-1];	// Move the last one here
		memcpy(szFile_ToMove, pMetaData[my_file_idx].szFileName, pMetaData[my_file_idx].nLenParentDirName);
		sprintf(szFile_ToMove+pMetaData[my_file_idx].nLenParentDirName, "/%s", p_DirEntryNameBuff + p_nEntryNameOffset[IdxEntry_in_Dir] + 1);	// file/dir type at the beginning of entry name;
//		sprintf(szFile_ToMove, "%s/%s", szParentDir, p_DirEntryNameBuff + p_nEntryNameOffset[IdxEntry_in_Dir] + 1);	// file/dir type at the beginning of entry name
		idx_file_need_update_idx_in_dir_entry = p_Hash_File->DictSearch(szFile_ToMove, &elt_list_file, &ht_table_file, &file_ToMove_hash);
		assert(idx_file_need_update_idx_in_dir_entry >= 0);
		pMetaData[idx_file_need_update_idx_in_dir_entry].IdxEntry_in_Dir = IdxEntry_in_Dir;	// the spot just freed!
	}
	else	{	// the last entry. Simply remove it!
		p_EntryName = p_DirEntryNameBuff + p_nEntryNameOffset[IdxEntry_in_Dir];
	}
	pDirMetaData[idx_Parent_dir].nLenAllEntries -= nLenEntryToRemove;
	pDirMetaData[idx_Parent_dir].nEntries--;

	pthread_mutex_unlock(&(dir_entry_lock[idx_Parent_dir & MAX_NUM_FILE_OP_LOCK_M1]));

	ncx_slab_free(sp_DirEntryName, p_EntryName);	// free in memory pool

	return;
}

int my_rmdir(char szDirName[])
{
	int dir_idx, file_idx;
	unsigned long long fn_hash=0;

//	printf("DBG> rmdir(%s)\n", szDirName);

	fn_hash = XXH64(szDirName, strlen(szDirName), 0);
	pthread_mutex_lock(&(unlink_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));

	dir_idx = p_Hash_Dir->DictSearch(szDirName, &elt_list_dir, &ht_table_dir, &fn_hash);
	if(dir_idx < 0)	{
		pthread_mutex_unlock(&(unlink_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		errno = ENOENT;
		return (-1);
	}
	else	{
		if(pDirMetaData[dir_idx].nEntries > 0)	{
			pthread_mutex_unlock(&(unlink_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
			errno = ENOTEMPTY;	// Directory not empty
			return -1;
		}

		file_idx = p_Hash_File->DictSearch(szDirName, &elt_list_file, &ht_table_file, &fn_hash);
		assert(file_idx>0);
		my_RemoveEntryInfo(file_idx);

		ncx_slab_free(sp_DirEntryNameOffset, (void*)((long int)p_DirEntryNameOffsetBuff + pDirMetaData[dir_idx].nOffset_To_EntryNameOffsetList));

		pthread_mutex_lock(&ht_lock);
		p_Hash_File->DictDelete(szDirName, &elt_list_file, &ht_table_file);	// remove hash table record
		nFile--;
		p_Hash_Dir->DictDelete(szDirName, &elt_list_dir, &ht_table_dir);	// remove hash table record
		nDir--;
		pthread_mutex_unlock(&ht_lock);
		pthread_mutex_unlock(&(unlink_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
	}
	return 0;
}

int my_unlink(char szFileName[])	// remove a regular file!
{
	int file_idx;
	unsigned long long fn_hash=0;

//	printf("DBG> unlink(%s)\n", szFileName);

	fn_hash = XXH64(szFileName, strlen(szFileName), 0);
	pthread_mutex_lock(&(unlink_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
	file_idx = p_Hash_File->DictSearch(szFileName, &elt_list_file, &ht_table_file, &fn_hash);
	if(file_idx < 0)	{
		pthread_mutex_unlock(&(unlink_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		errno = ENOENT;
		return (-1);
	}
	else	{
//		printf("DBG> unlink(%s)\n", szFileName);
		Truncate_File(file_idx, 0);	// Release storage.
		my_RemoveEntryInfo(file_idx);

		pthread_mutex_lock(&ht_lock);
		p_Hash_File->DictDelete(szFileName, &elt_list_file, &ht_table_file);	// remove hash table record
		nFile--;
		pthread_mutex_unlock(&ht_lock);
	}
	pthread_mutex_unlock(&(unlink_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
	return 0;
}

inline int my_strlen(const char szEntryName[])
{
	int i=0;

	while(szEntryName[i])	{
		i++;
	}
	return i;
}

int my_AddEntryInfo(int my_file_idx, int dir_idx)
{
	int nEntry, nLenEntryName;
	int *p_nEntryNameOffset=NULL, *p_nEntryNameOffsetNew=NULL;
	char *pEntryName;

	pthread_mutex_lock(&(dir_entry_lock[dir_idx & MAX_NUM_FILE_OP_LOCK_M1]));

//	if(strcmp(pDirMetaData[dir_idx].szDirName, "/myfs/mdtets/test-dir.0-0/mdtest_tree.0")==0)	printf("DBG> Add %s\n", pMetaData[my_file_idx].szFileName);
	// append the new file/dir at the end of parent dir entry list
	pEntryName = pMetaData[my_file_idx].szFileName+pMetaData[my_file_idx].nLenParentDirName + 1;
	nLenEntryName = my_strlen(pEntryName);
	nEntry = pDirMetaData[dir_idx].nEntries;
	p_nEntryNameOffset = (int *)(p_DirEntryNameOffsetBuff + pDirMetaData[dir_idx].nOffset_To_EntryNameOffsetList);
	p_nEntryNameOffset[nEntry] = (int) ( (long int)(ncx_slab_alloc(sp_DirEntryName, nLenEntryName+2)) - (long int)p_DirEntryNameBuff );	// only store offset
	strcpy(p_DirEntryNameBuff+p_nEntryNameOffset[nEntry] + 1, pEntryName);
	p_DirEntryNameBuff[p_nEntryNameOffset[nEntry]] = (pMetaData[my_file_idx].idx_dir_ht >= 0) ? (DIR_ENT_TYPE_DIR) : (DIR_ENT_TYPE_FILE);	// !!!!!!!!!!!!!!!!!!!!!!!!
//	p_DirEntryNameBuff[p_nEntryNameOffset[nEntry]] = (char)(pRf_Op_Msg->flag & 0xFF);	// !!!!!!!!!!!!!!!!!!!!!!!!
	pMetaData[my_file_idx].IdxEntry_in_Dir = nEntry;
	pDirMetaData[dir_idx].nEntries++;
	pDirMetaData[dir_idx].nLenAllEntries += (nLenEntryName+2);
	if(pDirMetaData[dir_idx].nEntries >= pDirMetaData[dir_idx].nMaxEntry)	{
		pDirMetaData[dir_idx].nMaxEntry *= 2;
		p_nEntryNameOffsetNew = (int *)ncx_slab_alloc(sp_DirEntryNameOffset, pDirMetaData[dir_idx].nMaxEntry*sizeof(int));
		memcpy((void*)p_nEntryNameOffsetNew, (void*)p_nEntryNameOffset, sizeof(int)*pDirMetaData[dir_idx].nEntries);
		pDirMetaData[dir_idx].nOffset_To_EntryNameOffsetList = (long int)p_nEntryNameOffsetNew - (long int)p_DirEntryNameOffsetBuff;
		pthread_mutex_unlock(&(dir_entry_lock[dir_idx & MAX_NUM_FILE_OP_LOCK_M1]));
		ncx_slab_free(sp_DirEntryNameOffset, (void*)p_nEntryNameOffset);	// free in memory pool
	}
	else	pthread_mutex_unlock(&(dir_entry_lock[dir_idx & MAX_NUM_FILE_OP_LOCK_M1]));

	return 0;
}

int my_ls(char szDirName[])	// list entries under a directory
{
	int i, dir_idx, nEntry;
	unsigned long long fn_hash=0;
	unsigned char *szDirEntryBuff=NULL;// entry data buffer
	int *p_nBytesDirEntryBuff, *p_nDirEntries, *p_DirEntryOffset, nBytesDirEntryNameAccu=0, nBytesEntryName, *p_nEntryNameOffset;
	char *p_szDirEntryName;

	dir_idx = p_Hash_Dir->DictSearch(szDirName, &elt_list_dir, &ht_table_dir, &fn_hash);
	if(dir_idx < 0)	{
		errno = ENOENT;
		return (-1);
	}
	else	{
		nEntry = pDirMetaData[dir_idx].nEntries;
		szDirEntryBuff = (unsigned char*) ncx_slab_alloc(sp_DirEntryList, pDirMetaData[dir_idx].nLenAllEntries + sizeof(int)*(3 + nEntry));
		p_szDirEntryName = (char*)szDirEntryBuff + sizeof(int) * (3 + nEntry);
		p_DirEntryOffset = (int*)( szDirEntryBuff + sizeof(int)*3 );
		p_nDirEntries = (int*)( szDirEntryBuff + sizeof(int)*1 );
		*p_nDirEntries = pDirMetaData[dir_idx].nEntries;
		printf("DBG> %d entries under directory %s\n", nEntry, szDirName);
		for(i=0; i<nEntry; i++)	{
			p_nEntryNameOffset = (int *)(p_DirEntryNameOffsetBuff + pDirMetaData[dir_idx].nOffset_To_EntryNameOffsetList);
			nBytesEntryName = strlen(p_DirEntryNameBuff + p_nEntryNameOffset[i]);
			strcpy(p_szDirEntryName + nBytesDirEntryNameAccu, p_DirEntryNameBuff + p_nEntryNameOffset[i]);	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
//			strcpy(p_szDirEntryName + nBytesDirEntryNameAccu, p_DirEntryNameBuff + p_nEntryNameOffset[i] + 1);	// !!!!!!!!!!!!!!!!!!!!!!!!!!
			printf("DBG> %d entry, %s\n", i+1, p_szDirEntryName + nBytesDirEntryNameAccu + 1);
			p_DirEntryOffset[i] = nBytesDirEntryNameAccu;
			nBytesDirEntryNameAccu += (nBytesEntryName + 1);
		}
		p_nBytesDirEntryBuff = (int*)szDirEntryBuff;
		*p_nBytesDirEntryBuff = nBytesDirEntryNameAccu + sizeof(int) * (3 + (*p_nDirEntries));

		ncx_slab_free(sp_DirEntryList, (void*)szDirEntryBuff);
	}

	return 0;
}

int my_fdopendir(int fd, void *loc_buf)
{
	int dir_idx;

	dir_idx = fd_List[fd].idx_file;
	return my_opendir_by_index(dir_idx, loc_buf);
}

int my_opendir_by_index(int dir_idx, void *loc_buf)
{
	int i, nEntry, nDirEntryListBuffSize;
//	unsigned char *szDirEntryBuff=NULL;// entry data buffer
	int *p_nDirEntries, *p_DirEntryOffset, nBytesDirEntryNameAccu=0, nBytesEntryName, *p_nEntryNameOffset;
	char *p_szDirEntryName, *pResult_buf;
	
	nEntry = pDirMetaData[dir_idx].nEntries;
	nDirEntryListBuffSize = pDirMetaData[dir_idx].nLenAllEntries + sizeof(int)*(1 + nEntry);
	if(nDirEntryListBuffSize > (IO_RESULT_BUFFER_SIZE - sizeof(RW_FUNC_RETURN)) )	{	// too large to fit in result buffer (loc_buf)
		printf("ERROR> The result buffer is not enough to hold dir entry list for %s. Need %d bytes.\n", pDirMetaData[dir_idx].szDirName, nDirEntryListBuffSize);
		errno = ENOMEM;
		return (-1);
	}

	pResult_buf = (char*)loc_buf;
	p_szDirEntryName = (char*)pResult_buf + sizeof(int) * (1 + nEntry);
	p_DirEntryOffset = (int*)( pResult_buf + sizeof(int)*1 );
	p_nDirEntries = (int*)( pResult_buf );
	*p_nDirEntries = pDirMetaData[dir_idx].nEntries;
//	printf("DBG> %d entries under directory %s\n", nEntry, pDirMetaData[dir_idx].szDirName);
	for(i=0; i<nEntry; i++)	{
		p_nEntryNameOffset = (int *)(p_DirEntryNameOffsetBuff + pDirMetaData[dir_idx].nOffset_To_EntryNameOffsetList);
		nBytesEntryName = strlen(p_DirEntryNameBuff + p_nEntryNameOffset[i]);
		strcpy(p_szDirEntryName + nBytesDirEntryNameAccu, p_DirEntryNameBuff + p_nEntryNameOffset[i]);	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
//		printf("DBG> %d entry, %s\n", i+1, p_szDirEntryName + nBytesDirEntryNameAccu + 1);
		p_DirEntryOffset[i] = nBytesDirEntryNameAccu;
		nBytesDirEntryNameAccu += (nBytesEntryName + 1);
	}
	
	return (nDirEntryListBuffSize);	// always larger than zero. 
}

int my_opendir(char szDirName[], void *loc_buf)
{
	int dir_idx;
	unsigned long long fn_hash=0;

	dir_idx = p_Hash_Dir->DictSearch(szDirName, &elt_list_dir, &ht_table_dir, &fn_hash);
	if(dir_idx < 0)	{
		errno = ENOENT;
		return (-1);	// fail
	}
	return my_opendir_by_index(dir_idx, loc_buf);
}

int my_chmod(char szFileName[], int mode)
{
	int file_idx;
	unsigned long long fn_hash=0;

	file_idx = p_Hash_File->DictSearch(szFileName, &elt_list_file, &ht_table_file, &fn_hash);
	if(file_idx < 0)	{
		errno = ENOENT;
		return (-1);	// fail
	}

	pMetaData[file_idx].st_mode = mode;

	return 0;
}

int my_setuserinfo(char szFileName[])
{
	int file_idx;
	unsigned long long fn_hash=0;
	struct timespec t_spec;

	file_idx = p_Hash_File->DictSearch(szFileName, &elt_list_file, &ht_table_file, &fn_hash);
	if(file_idx < 0)	{
		errno = ENOENT;
		return (-1);	// fail
	}

	pMetaData[file_idx].st_uid = my_uid;
	pMetaData[file_idx].st_gid = my_gid;

	clock_gettime(CLOCK_REALTIME, &t_spec);
	pMetaData[file_idx].st_atim.tv_sec = t_spec.tv_sec;
	pMetaData[file_idx].st_atim.tv_nsec = t_spec.tv_nsec;
	pMetaData[file_idx].st_mtim.tv_sec = t_spec.tv_sec;
	pMetaData[file_idx].st_mtim.tv_nsec = t_spec.tv_nsec;
	pMetaData[file_idx].st_ctim.tv_sec = t_spec.tv_sec;
	pMetaData[file_idx].st_ctim.tv_nsec = t_spec.tv_nsec;

	return 0;
}

void Test_File_System_Local(void)
{
	int fd;
	ncx_slab_stat_t ncx_stat;

	printf("DBG> Before my_mkdir(). sp_DirEntryName\n");
	ncx_slab_stat(sp_DirEntryName, &ncx_stat);
	printf("DBG> Before my_mkdir(). sp_DirEntryNameOffset\n");
	ncx_slab_stat(sp_DirEntryNameOffset, &ncx_stat);

	my_mkdir("/myfs/tmp", S_IRWXU | S_IRWXG | S_IRWXO, my_uid, my_gid);

	printf("DBG> After my_mkdir(). sp_DirEntryName\n");
	ncx_slab_stat(sp_DirEntryName, &ncx_stat);
	printf("DBG> After my_mkdir(). sp_DirEntryNameOffset\n");
	ncx_slab_stat(sp_DirEntryNameOffset, &ncx_stat);

	my_rmdir("/myfs/tmp");

//	my_mkdir("/myfs/tmp_0");

	printf("DBG> After my_rmdir(). sp_DirEntryName\n");
	ncx_slab_stat(sp_DirEntryName, &ncx_stat);
	printf("DBG> After my_rmdir(). sp_DirEntryNameOffset\n");
	ncx_slab_stat(sp_DirEntryNameOffset, &ncx_stat);


	Readin_All_Dir();
	Readin_All_File();

	fd = my_openfile("/myfs/3/k.rnd", O_RDONLY);
	my_close(fd);

	printf("DBG> After my_mkdir(). sp_DirEntryName\n");
	ncx_slab_stat(sp_DirEntryName, &ncx_stat);
	printf("DBG> After my_mkdir(). sp_DirEntryNameOffset\n");
	ncx_slab_stat(sp_DirEntryNameOffset, &ncx_stat);

/*
	int fd;
	char szFileName[128];

	my_mkdir("/myfs/tmp");

	sprintf(szFileName, "%s/tmp/test.txt", szFSRoot);
	fd = my_openfile(szFileName, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
	my_close(fd);

	sprintf(szFileName, "%s/tmp/a.txt", szFSRoot);
	fd = my_openfile(szFileName, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
	my_close(fd);

	sprintf(szFileName, "%s/tmp/bbb.txt", szFSRoot);
	fd = my_openfile(szFileName, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
	my_close(fd);

	my_ls("/myfs/tmp");

	my_unlink("/myfs/tmp/a.txt");
	my_ls("/myfs/tmp");

	my_unlink("/myfs/tmp/test.txt");
	my_ls("/myfs/tmp");

	my_unlink("/myfs/tmp/bbb.txt");
	my_ls("/myfs/tmp");

	my_ls("/myfs");
	my_rmdir("/myfs/tmp");
	my_ls("/myfs");
*/
}

/*
#define TEST_BUFF	(4096*4)
void Test_File_System_Local(void)
{
	int fd, i, j;
	char szFileName[256];
	int input[TEST_BUFF], output[TEST_BUFF], nBytesRead, nBytesTotal, nBytes, nBytesRead_Accum, nBytesWritten, nBytesWritten_Accum;

	for(i=0; i<TEST_BUFF; i++)	{
		input[i] = i;
	}

	sprintf(szFileName, "%s/test.txt", szFSRoot);

//	fd = my_openfile(szFileName, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
//	for(i=0; i<16; i++)	{
//		my_write(fd, (char*)input + 4096*i, 4096);
//	}
//	my_write(fd, (char*)input, 16*4096);
//	my_close(fd);

	srand(123);

	printf("DBG> %d free pages.\n", pMem_Allocator->Get_Num_Free_Page());
	for(j=0; j<100000; j++)	{
		nBytesTotal = TEST_BUFF * sizeof(int);
		nBytesWritten_Accum = 0;
		fd = my_openfile(szFileName, O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR);
//		printf("DBG> %d free pages.\n", pMem_Allocator->Get_Num_Free_Page());
		while(nBytesTotal > 0)	{
			nBytes = random()%3228;
//			nBytes = 8192;
			nBytes = min(nBytes, nBytesTotal);
//			nFreePages = pMem_Allocator->Get_Num_Free_Page();
//            printf("DBG> nBytes = %d nPage = %d\n", nBytes, nBytes >> 12);
			nBytesWritten = my_write(fd, (char*)input+nBytesWritten_Accum, nBytes);
			assert(nBytesWritten == nBytes);
			nBytesTotal -= nBytesWritten;
			nBytesWritten_Accum += nBytesWritten;
		}

		nBytes = 0;
		for(i=0; i<pMetaData[fd_List[fd].idx_file].nDirectPointer; i++)	{
			printf("DBG> %3d %6ld  %6ld\n", i, pMetaData[fd_List[fd].idx_file].DiretData[i].FileOffset, pMetaData[fd_List[fd].idx_file].DiretData[i].FileOffset + pMetaData[fd_List[fd].idx_file].DiretData[i].DataBlockSize);
		}
		for(i=0; i<pMetaData[fd_List[fd].idx_file].nExtraPointer; i++)	{
			printf("DBG> %3d %6ld  %6ld\n", i+NUM_DIRCT_PT, pMetaData[fd_List[fd].idx_file].pExtraData[i].FileOffset, pMetaData[fd_List[fd].idx_file].pExtraData[i].FileOffset + pMetaData[fd_List[fd].idx_file].pExtraData[i].DataBlockSize);
		}
		for(i=0; i<80; i++)	{
			nBytes += (random()%3228);
			Determine_Index_StorageBlock_for_Offset(fd, nBytes);
			printf("DBG> Test %3d, %d in Block %d (%6ld, %6ld)\n", i, nBytes, fd_List[fd].idx_block, 
				(fd_List[fd].idx_block >= NUM_DIRCT_PT) ? (pMetaData[fd_List[fd].idx_file].pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset) : (pMetaData[fd_List[fd].idx_file].DiretData[fd_List[fd].idx_block].FileOffset), 
				(fd_List[fd].idx_block >= NUM_DIRCT_PT) ? (pMetaData[fd_List[fd].idx_file].pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].FileOffset + pMetaData[fd_List[fd].idx_file].pExtraData[fd_List[fd].idx_block-NUM_DIRCT_PT].DataBlockSize) : (pMetaData[fd_List[fd].idx_file].DiretData[fd_List[fd].idx_block].FileOffset + pMetaData[fd_List[fd].idx_file].DiretData[fd_List[fd].idx_block].DataBlockSize) );
		}

		my_close(fd);

//		nFreePages = pMem_Allocator->Get_Num_Free_Page();
//		printf("DBG> %d free pages.\n", nFreePages);
		fd = my_openfile(szFileName, O_RDONLY);
//		nFreePages = pMem_Allocator->Get_Num_Free_Page();
//		printf("DBG> %d free pages.\n", nFreePages);
		nBytesRead = my_read(fd, (char*)output, 16*4096);
		my_close(fd);
		for(i=0; i<TEST_BUFF; i++)	{
			if(output[i] != i)	{
				printf("%d %d\n", i, output[i]);
			}
		}
	}

/*
	fd = my_openfile(szFileName, O_RDONLY);
	nBytesRead = my_read(fd, output, 16*4096);
	my_close(fd);
*/
/*
	fd = my_openfile(szFileName, O_RDONLY);
	nBytesRead = my_read(fd, (char*)output, 16*4096);
	my_close(fd);
	for(i=0; i<TEST_BUFF; i++)	{
		if(output[i] != i)	printf("%d %d\n", i, output[i]);
	}
*/
/*
	for(j=0; j<10000; j++)	{
		nBytesTotal = TEST_BUFF * sizeof(int);
		nBytesRead_Accum = 0;
		fd = my_openfile(szFileName, O_RDONLY);
		while(nBytesTotal > 0)	{
			nBytes = rand()%52288;
//			nBytes = 8192;
			nBytes = min(nBytes, nBytesTotal);
			nBytesRead = my_read(fd, (char*)output+nBytesRead_Accum, nBytes);
			assert(nBytesRead == nBytes);
			nBytesTotal -= nBytesRead;
			nBytesRead_Accum += nBytesRead;
		}
		my_close(fd);
		for(i=0; i<TEST_BUFF; i++)	{
			if(output[i] != input[i])	{
				printf("%d %d %d\n", i, input[i], output[i]);
			}
		}
	}
*/
//}

static void Readin_All_Dir(void)
{
	FILE *fIn;
	char *ReadLine, szLine[256], szDirName[256], szName[256], szPrefix[]="FanStore/test_case/data";
	int nLen, i;

	sprintf(szName, "%s/dir.list", szPrefix);
	fIn = fopen(szName, "r");
	if(fIn == NULL)	{
		printf("Warning> Fail to open file: %s\nQuit.\n", szName);
		return;
	}
	while(1)	{
		if(feof(fIn))	{
			break;
		}
		ReadLine = fgets(szLine, 256, fIn);
		if(ReadLine == NULL)	{
			break;
		}
		nLen = strlen(szLine);
		if(szLine[nLen-1] == 0xA)	{
			szLine[nLen-1] = 0;
			nLen--;
		}
		if(szLine[nLen-2] == 0xD)	{
			szLine[nLen-2] = 0;
			nLen--;
		}

		sprintf(szDirName, "%s/%s", szFSRoot, szLine);
		my_mkdir(szDirName, S_IRWXU | S_IRWXG | S_IRWXO, my_uid, my_gid);
		my_chmod(szDirName, 17407);	// copy the mode of "/dev/shm"
		my_setuserinfo(szDirName);
	}
	fclose(fIn);
//	my_ls("/myfs");
}

ssize_t read_all(int fd, void *buf, size_t count)
{
	ssize_t ret, nBytes=0;

	while (count != 0 && (ret = read(fd, buf, count)) != 0) {
		if (ret == -1) {
			if (errno == EINTR)
				continue;
			perror ("read");
			break;
		}
		nBytes += ret;
		count -= ret;
		buf += ret;
	}
	return nBytes;
}

#define MAX_FILE_SIZE	(256*1024*1024)

static void Readin_All_File(void)
{
	int nLen_File_Name=168, nLen_stat =144;	// set in prep
	int fd_in, fd_out, i, j, nReadBytes, nWriteBytes, nfile_in_this_partition, nFileLocal_Max=0, ret, nPartition;
	char szNameIn[256], szNameOut[256], *szNameList_local=NULL, szFileName[256], szPrefix[]="FanStore/test_case/data", szOrgFileName[256], szReadinCmp[1024*64], szOrgFile[1024*64];
	struct stat file_stat;
	unsigned char *pBuff;
	int *p_nFileLocalList=NULL, *displs, *recvcounts;
	long int nBytesAllFiles=0, nBytesPacked_Sum=0, nBytesPacked, nBytes_to_Read;
	int fd_org, k;

	pBuff = (unsigned char *)malloc(MAX_FILE_SIZE);
	if(!pBuff)	{
		printf("Fail to allocate memory for pBuff.\nQuit\n");
		exit(1);
	}

	for(i=0; ; i++)	{
		sprintf(szNameIn, "%s/fs_%d", szPrefix, i);
		fd_in = open(szNameIn, O_RDONLY);
		if(fd_in == -1)	{
			nPartition = i;
			break;
		}
		
		nReadBytes = read_all(fd_in, &nfile_in_this_partition, sizeof(int));
		if(nReadBytes != sizeof(int))	{
			printf("Error in reading file %s\nQuit\n", szNameIn);
			exit(1);
		}		
		for(j=0; j<nfile_in_this_partition; j++)	{
			nReadBytes = read_all(fd_in, szFileName, nLen_File_Name);
			if(nReadBytes != nLen_File_Name)	{
				printf("Error in reading file %s. nReadBytes != nLen_File_Name\nQuit\n", szNameIn);
				exit(1);
			}
			nReadBytes = read_all(fd_in, &file_stat, nLen_stat);
			if(nReadBytes != nLen_stat)	{
				printf("Error in reading file %s. nReadBytes != nLen_stat\nQuit\n", szNameIn);
				exit(1);
			}
			nReadBytes = read_all(fd_in, &nBytesPacked, sizeof(long int));
			assert(nReadBytes == sizeof(long int));
			nBytesPacked_Sum += nBytesPacked;	// == 0 means unpacked
			
			if(nBytesPacked != file_stat.st_size) {
				if(nBytesPacked == 0) { // read stat.st_size
					nBytes_to_Read = file_stat.st_size;
				}
				else { // read nBytesPacked
					nBytes_to_Read = nBytesPacked;
				}
			}
			else {	// read stat.st_size
				nBytes_to_Read = file_stat.st_size;
			}
			
			nReadBytes = read_all(fd_in, pBuff, nBytes_to_Read);
			if(nReadBytes != nBytes_to_Read)	{
				printf("Error in reading file %s. nReadBytes != nBytes_to_Read\nQuit\n", szNameIn);
				exit(1);
			}
			if( ( (file_stat.st_mode & S_IFMT) == S_IFDIR) )	{	// Existing dir. Expected behavior.
				continue;
			}

			sprintf(szNameOut, "%s/%s", szFSRoot, szFileName);
			fd_out = my_openfile(szNameOut, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR);
			if(fd_out == -1)	{
				if( ( (file_stat.st_mode & S_IFMT) == S_IFDIR) )	{	// Existing dir. Expected behavior.
				}
				else	{
					printf("DBG>Fail to open file %s\nQuit\n", szNameOut);
					exit(1);
				}
			}
			else	{
				nWriteBytes = my_write(fd_out, pBuff, nBytes_to_Read, 0);
				
				if(nWriteBytes != nBytes_to_Read)	{
					printf("Error in writing file for %s. nWriteBytes (%d) != nBytes_to_Read (%d)\nQuit\n", szNameOut, nWriteBytes, nBytes_to_Read);
					exit(1);
				}
				my_close(fd_out);
				my_chmod(szNameOut, file_stat.st_mode);
				//					printf("Rank = %d Write file %s, size_packed = %d\n", rank, p_filerec_local[nFileLocal].szName, p_filerec_local[nFileLocal].size_packed);
				nBytesAllFiles += file_stat.st_size;
/*
				fd_out = my_openfile(szNameOut, O_RDONLY, S_IRUSR | S_IWUSR);	// verify the file content!
				my_read(fd_out, szReadinCmp, file_stat.st_size);
				my_close(fd_out);

				sprintf(szOrgFileName, "%s/%s", szPrefix, szFileName);
				fd_org = open(szOrgFileName, O_RDONLY);
				assert(fd_org > 0);
				nReadBytes = read_all(fd_org, szOrgFile, file_stat.st_size);
				assert(nReadBytes == file_stat.st_size);
				close(fd_org);
				for(k=0; k<nReadBytes; k++)	{
					if(szOrgFile[k] != szReadinCmp[k])	{
						printf("%d %c %c\n", k, szOrgFile[k], szReadinCmp[k]);
					}
				}
*/
			}
			
		}
		
		close(fd_in);
	}


	free(pBuff);
}

