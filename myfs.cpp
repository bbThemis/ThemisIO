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
#include <stddef.h>
#include <bits/stat.h>
#include <immintrin.h>
#include <sys/syscall.h>

#include "myfs.h"
#include "qp.h"
#include "buddy.h"
#include "xxhash.h"
//#include "queue.h"
#include "dict.h"
#include "utility.h"
#include "ncx_slab.h"
#include "queue_free_mem.h"
#include "fixed_mem_allocator.h"

#define MIN_BATCH_SIZE_FREE_EX_POINTER	(128*1024)

extern long int nSizeReg;
extern __thread uint64_t rseed[2];
extern __thread int idx_qp_server;

//#define MAX_DIR_FD	(65536)
//#define MAX_DIR_FD_M1	((MAX_DIR_FD) - 1)
extern SERVER_QUEUEPAIR Server_qp;
extern CQUEUE_FREE_MEM CQueue_Free_Mem;


const int IO_Msg_Size_op = ( offsetof(IO_CMD_MSG,op) + sizeof(int) );
static size_t File_Strip_Size_All_server, Stripe_Offset_Shift;

static int my_uid, my_gid, idx_fs_FSRoot=-1;

int nFile=0, nDir=0;	// number of file and dir on this server
ULongInt FSSize, HashTableFileSize, HashTableLargeFileSize, FileMetaDataSize, LargeFileStripeDataSize, HashTableDirSize, DirMetaDataSize, AllocatorSize, DataAreaSize;
// hash table for files, dirs;
int fd_shm;
char szNameShm[]="myfs_shm";
char szFSRoot[64]=MYFS_ROOT_DIR;

pthread_mutex_t create_new_lock[MAX_NUM_FILE_OP_LOCK];	// the lock used by the right server
//pthread_mutex_t create_new_ext_lock[MAX_NUM_FILE_OP_LOCK];	// the lock used by extended server
pthread_mutex_t unlink_lock[MAX_NUM_FILE_OP_LOCK];
pthread_mutex_t unlinkDir_lock[MAX_NUM_FILE_OP_LOCK];
pthread_mutex_t file_lock[MAX_NUM_FILE_OP_LOCK];
//pthread_mutex_t file_wr_lock[MAX_NUM_FILE_OP_LOCK];
pthread_mutex_t dir_entry_lock[MAX_NUM_FILE_OP_LOCK];
pthread_mutex_t fd_lock;
pthread_mutex_t ht_file_lock;	// modify hashtable lock
pthread_mutex_t ht_dir_lock;	// modify hashtable lock
pthread_mutex_t ht_stripe_lock;	// modify hashtable lock
//pthread_mutex_t ht_DirEntry_lock[MAX_NUM_FILE_OP_LOCK];	// modify hashtable lock
pthread_mutex_t *pAccess_qp0_lock;	// Only one token is available to access queue pair 0 since the target buffer on remote server is shared!!!! 

CFIXEDSIZE_MEM_ALLOCATOR CFixedSizeMemAllcator;
//ncx_slab_pool_t *sp_CallReturnBuff=NULL;
//char *p_CallReturnBuff=NULL;

ncx_slab_pool_t *sp_LongFileNameBuff=NULL, *sp_DirEntryHashTableBuff=NULL;
ncx_slab_pool_t *sp_ExtraPointers=NULL;
ncx_slab_pool_t *sp_DirEntryList=NULL;
//static char *p_DirEntryNameOffsetBuff=NULL;
//static char *p_DirEntryNameBuff=NULL;
ncx_slab_pool_t *sp_OpenDirEntryBuff=NULL;

void *pMyfs=NULL;

CMEM_ALLOCATOR *pMem_Allocator=NULL;

CHASHTABLE_CHAR *p_Hash_File=NULL;
struct elt_Char *elt_list_file=NULL;
int *ht_table_file=NULL;

CHASHTABLE_CHAR *p_Hash_Dir=NULL;
struct elt_Char *elt_list_dir=NULL;
int *ht_table_dir=NULL;

CHASHTABLE_CHAR *p_Hash_LargeFile=NULL;
struct elt_Char *elt_list_LargeFile=NULL;
int *ht_table_LargeFile=NULL;

META_INFO *pMetaData=NULL;
DIR_META_INFO *pDirMetaData=NULL;

STRIPE_DATA_INFO *pStripeData=NULL;

extern int mpi_rank, nFSServer, nNUMAPerNode;	// rank and size of MPI, number of numa nodes per compute node

ACTIVEFILE __attribute__((aligned(16))) fd_List[MAX_FD_ACTIVE];
int nActiveFd=0, First_Av_Fd=0, IdxLastFd=-1;

void Init_Memory(void)
{
	ULongInt Offset;
	int i, nQP_Server;
	char szNameShm_Full[128];

	sprintf(szNameShm_Full, "%s_%d", szNameShm, mpi_rank%nNUMAPerNode);
	fd_shm = shm_open(szNameShm_Full, O_RDWR | O_CREAT, 0600);
	if(fd_shm == -1)    {	// failed to create
		printf("Error to create %s\nQuit\n", szNameShm_Full);
		exit(1);
	}

	HashTableFileSize = CHASHTABLE_CHAR::GetStorageSize(MAX_NUM_FILE);
	HashTableDirSize = CHASHTABLE_CHAR::GetStorageSize(MAX_NUM_DIR);
	HashTableLargeFileSize = CHASHTABLE_CHAR::GetStorageSize(MAX_NUM_LARGE_FILE);

	LargeFileStripeDataSize = sizeof(STRIPE_DATA_INFO)*MAX_NUM_LARGE_FILE;
	FileMetaDataSize = sizeof(META_INFO)*MAX_NUM_FILE;
	DirMetaDataSize = sizeof(DIR_META_INFO)*MAX_NUM_DIR;
	AllocatorSize = _NPAGES * sizeof(struct page);
	DataAreaSize = _NPAGES * BUDDY_PAGE_SIZE;

//	FSSize = HashTableFileSize + HashTableDirSize + FileMetaDataSize + DirMetaDataSize + sizeof(CMEM_ALLOCATOR) + AllocatorSize + DataAreaSize;
	FSSize = HashTableFileSize + HashTableDirSize + HashTableLargeFileSize + LargeFileStripeDataSize + FileMetaDataSize + DirMetaDataSize + BUDDY_PAGE_SIZE + AllocatorSize + DataAreaSize;
	// printf("FSSize = HashTableFileSize (%lu) + HashTableDirSize (%lu) + HashTableLargeFileSize (%lu) + LargeFileStripeDataSize (%lu) + FileMetaDataSize (%lu) + DirMetaDataSize (%lu) + BUDDY_PAGE_SIZE (%lu) + AllocatorSize (%lu) + DataAreaSize (%lu)\n", HashTableFileSize, HashTableDirSize, HashTableLargeFileSize, LargeFileStripeDataSize, FileMetaDataSize, DirMetaDataSize, BUDDY_PAGE_SIZE, AllocatorSize, DataAreaSize);

	if (ftruncate(fd_shm, FSSize) != 0) {
		perror("ftruncate for fd_shm");
	}

	pMyfs = mmap(NULL, FSSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd_shm, 0);
	if(pMyfs == MAP_FAILED)	{
		// perror("mmap for pMyfs");
		printf("mmap failed with FSSize=%luGB file=%s filedes=%d: %s\n", FSSize / (1<<30), szNameShm_Full, fd_shm, strerror(errno));
		exit(1);
	}
	Offset = 0;

	pMem_Allocator = (CMEM_ALLOCATOR *)pMyfs;
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

	p_Hash_LargeFile = (CHASHTABLE_CHAR *)((ULongInt)pMyfs + Offset);
	p_Hash_LargeFile->DictCreate(MAX_NUM_LARGE_FILE, &elt_list_LargeFile, &ht_table_LargeFile);	// init hash table
//	printf("%lx \n", (ULongInt)p_Hash_Dir);
	Offset += HashTableLargeFileSize;

	pStripeData = (STRIPE_DATA_INFO *)((ULongInt)pMyfs + Offset);
//	printf("%lx \n", (ULongInt)pDirMetaData);
	Offset += LargeFileStripeDataSize;


	sp_LongFileNameBuff = ncx_slab_init(NULL, MAX_LEN_LONG_FILE_NAME_BUFF);
	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	sp_ExtraPointers = ncx_slab_init(NULL, MAX_LEN_EXTRA_POINTERS_BUFF);
	sp_DirEntryList = ncx_slab_init(NULL, MAX_LEN_DIR_ENTRY_LIST_BUFF);
	sp_DirEntryHashTableBuff = ncx_slab_init(NULL, MAX_LEN_DIR_ENTRY_HASHTABLE_BUFF);

	sp_OpenDirEntryBuff = ncx_slab_init(NULL, MAX_LEN_OPEN_DIR_BUFF);

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
                if(pthread_mutex_init(&(unlinkDir_lock[i]), NULL) != 0) {
                        printf("\n mutex unlinkDir_lock init failed\n");
                        exit(1);
                }
		if(pthread_mutex_init(&(file_lock[i]), NULL) != 0) { 
			printf("\n mutex file_lock init failed\n"); 
			exit(1);
		}
		if(pthread_mutex_init(&(dir_entry_lock[i]), NULL) != 0) { 
			printf("\n mutex dir_entry_lock init failed\n"); 
			exit(1);
		}
//		if(pthread_mutex_init(&(ht_DirEntry_lock[i]), NULL) != 0) { 
//			printf("\n mutex ht_dir_lock init failed\n"); 
//			exit(1);
//		}

	}
	if(pthread_mutex_init(&fd_lock, NULL) != 0) { 
		printf("\n mutex fd_lock init failed\n"); 
		exit(1);
	}
	if(pthread_mutex_init(&ht_file_lock, NULL) != 0) { 
		printf("\n mutex ht_file_lock init failed\n"); 
		exit(1);
	}
	if(pthread_mutex_init(&ht_dir_lock, NULL) != 0) { 
		printf("\n mutex ht_dir_lock init failed\n"); 
		exit(1);
	}
	if(pthread_mutex_init(&ht_stripe_lock, NULL) != 0) { 
		printf("\n mutex ht_stripe_lock init failed\n"); 
		exit(1);
	}

	nQP_Server = nFSServer * NUM_THREAD_IO_WORKER_INTER_SERVER;
	pAccess_qp0_lock = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t) * nQP_Server);
	for(i=0; i<nQP_Server; i++)	{
		if(pthread_mutex_init(&(pAccess_qp0_lock[i]), NULL) != 0) { 
			printf("\n mutex pAccess_qp0_lock[%d] init failed\n", i); 
			exit(1);
		}
	}
	
	printf("INFO> Finished Init_Memory().\n");

	my_uid = getuid();
	my_gid = getgid();

	unsigned long long int fn_hash;
	fn_hash = XXH64(szFSRoot, strlen(szFSRoot), 0);
	idx_fs_FSRoot = fn_hash % nFSServer;

	File_Strip_Size_All_server = nFSServer * FILE_STRIPE_SIZE;
	Stripe_Offset_Shift = File_Strip_Size_All_server - FILE_STRIPE_SIZE;
}

inline int Query_Server_Index(char szPath[], int nLen)
{
	unsigned long long fn_hash=0;
	int nReadItem, Idx_Server;

	if(strcmp(szPath, szFSRoot)==0)	{
		return idx_fs_FSRoot;
	}

	if( nLen>=4 )	{	// specific tag used to choose specific server!!!!!!!!!!!!!!!!!!!!
		if( (szPath[nLen-4] == '_') && (szPath[nLen-3] == 'S') )	{
			nReadItem = sscanf(szPath+nLen-2, "%x", &Idx_Server);
			if( (nReadItem == 1) && (Idx_Server < nFSServer) )	{
				if((Idx_Server) < nFSServer)	return Idx_Server;
			}
		}
		if(nLen > 10)	{
			if( (szPath[nLen-9] == '.') && (szPath[nLen-8] == '0')  && (szPath[nLen-7] == '0') )	{
				nReadItem = sscanf(szPath+nLen-6, "%d", &Idx_Server);
				if( (nReadItem == 1) && (Idx_Server < nFSServer) )	{
					if((Idx_Server) < nFSServer)	return Idx_Server;
				}
			}
		}
	}
	fn_hash = XXH64(szPath, nLen, 0);
	Idx_Server = fn_hash % nFSServer;
	return Idx_Server;
}

inline int Query_Server_Index_Dir_Index(char szPath[], int nLen, int *pIdx_Server)
{
	unsigned long long fn_hash=0;
	int dir_idx, nReadItem;

	*pIdx_Server = -1;

	if(strcmp(szPath, szFSRoot)==0)	{
		*pIdx_Server = idx_fs_FSRoot;
		if(idx_fs_FSRoot == mpi_rank)	{
			return 0;	// dir_idx = 0 for szFSRoot		
		}
		else	return (-1);	// on other server. 
	}

	if( (nLen>=4) && (szPath[nLen-4] == '_') && (szPath[nLen-3] == 'S') )	{	// specific tag used to choose specific server!!!!!!!!!!!!!!!!!!!!
		nReadItem = sscanf(szPath+nLen-2, "%x", pIdx_Server);
		if(nReadItem != 1)	{
			printf("Warning> Failed to extract server index from %s\n", szPath+nLen-4);
		}
		else	{
			if((*pIdx_Server) >= nFSServer)	{
				printf("Warning> idx_Server (%d) > nFSServer (%d) in %s\n", *pIdx_Server, nFSServer, szPath+nLen-4);
//				exit(1);
			}
			else	{
				if(mpi_rank != (*pIdx_Server) ) 	{	// on other server
					return (-1);	// dir_idx = -1;
				}
			}
		}
	}
	if( (nLen > 10) && (szPath[nLen-9] == '.') && (szPath[nLen-8] == '0')  && (szPath[nLen-7] == '0') )	{
		nReadItem = sscanf(szPath+nLen-6, "%d", pIdx_Server);
		if(nReadItem != 1)	{
			printf("Warning> Failed to extract server index from %s\n", szPath+nLen-4);
		}
		else	{
			*pIdx_Server = (*pIdx_Server) % nFSServer;
			if(mpi_rank != (*pIdx_Server) ) 	{	// on other server
				return (-1);	// dir_idx = -1;
			}
		}
	}

	dir_idx = p_Hash_Dir->DictSearch(szPath, &elt_list_dir, &ht_table_dir, &fn_hash);
	if(dir_idx >= 0)	{
		*pIdx_Server = mpi_rank;	// current node
	}
	else	{
		*pIdx_Server = fn_hash % nFSServer;
	}
	return dir_idx;
}

int Query_Parent_Dir(char szDirName[], char szParentDir[], int *nLenParentDirName, int *nLenFileName, int *pIdx_Server)
{
	int i, dir_idx;

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

	return Query_Server_Index_Dir_Index(szParentDir, i, pIdx_Server);
}

inline int Wait_For_IO_Request_Result(int Tag_Magic, RW_FUNC_RETURN *pIO_Result)
{
	RW_FUNC_RETURN *pResult = pIO_Result;
	int *pTag_End, Tag_Ini;
	struct timeval tm1, tm2;	// tm1.tv_sec
	long int t1_ms, t2_ms;

	gettimeofday(&tm1, NULL);
	t1_ms = (tm1.tv_sec * 1000) + (tm1.tv_usec / 1000);
	while(1)	{
		if(pResult->nDataSize > 0)	break;	// waiting for the size of result. 

		gettimeofday(&tm2, NULL);
		t2_ms = (tm2.tv_sec * 1000) + (tm2.tv_usec / 1000);
		if( (t2_ms - t1_ms) > QP_WAIT_RESULT_TIMEOUT_MS )	{
			return 1;	// time out
		}
	}
	Tag_Ini = pResult->Tag_Ini;
	pTag_End =  (int*)((char*)pIO_Result + pResult->nDataSize - sizeof(int));

	while(1)	{
		if( ( Tag_Ini ^ (*pTag_End) ) == Tag_Magic )	break;	// waiting for the ending tag. 

		gettimeofday(&tm2, NULL);
		t2_ms = (tm2.tv_sec * 1000) + (tm2.tv_usec / 1000);
		if( (t2_ms - t1_ms) > QP_WAIT_RESULT_TIMEOUT_MS )	{
			return 1;	// time out
		}
	}
	return 0;
}


void Query_Other_Server(int idx_server)
{
	RW_FUNC_RETURN *pResult;
	IO_CMD_MSG *pIO_Cmd_ToSend_Other_Server;
	char *pNewMsg_ToSend, *pMemAlloc;
	int idx_qp;

	idx_qp = (idx_server * NUM_THREAD_IO_WORKER_INTER_SERVER) + idx_qp_server;

	pMemAlloc = CFixedSizeMemAllcator.Allocate_a_Block();	
//	pMemAlloc = (char*)ncx_slab_alloc(sp_CallReturnBuff, SIZE_FOR_NEW_MSG + sizeof(IO_CMD_MSG) + sizeof(RW_FUNC_RETURN));
	pNewMsg_ToSend = (char*)pMemAlloc;
	pIO_Cmd_ToSend_Other_Server = (IO_CMD_MSG *)(pNewMsg_ToSend + SIZE_FOR_NEW_MSG);

	pIO_Cmd_ToSend_Other_Server->rkey = Server_qp.mr_shm_global->rkey;
	pIO_Cmd_ToSend_Other_Server->rem_buff = (void*)(pNewMsg_ToSend + SIZE_FOR_NEW_MSG + sizeof(IO_CMD_MSG));
	pIO_Cmd_ToSend_Other_Server->tag_magic = (int)(next(rseed) & 0xFFFFFFFF);
	pIO_Cmd_ToSend_Other_Server->op = IO_OP_MAGIC | RF_RW_OP_HELLO;

	pResult = (RW_FUNC_RETURN *)pIO_Cmd_ToSend_Other_Server->rem_buff;
	pResult->nDataSize = 0; // init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data.

	if (pthread_mutex_lock(&(pAccess_qp0_lock[idx_qp])) != 0) {
		perror("pthread_mutex_lock");
		exit(2);
	}

	Server_qp.IB_Put(idx_qp, (void*)pIO_Cmd_ToSend_Other_Server, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].remote_addr_IO_CMD), Server_qp.pQP_Data[idx_qp].rem_key, IO_Msg_Size_op);
//	Server_qp.IB_Put(idx_qp, (void*)pIO_Cmd_ToSend_Other_Server, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].remote_addr_IO_CMD), Server_qp.pQP_Data[idx_qp].rem_key, sizeof(IO_CMD_MSG));
	pNewMsg_ToSend[0] = TAG_NEW_REQUEST;	// new msg!
	Server_qp.IB_Put(idx_qp, (void*)pNewMsg_ToSend, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].remote_addr_new_msg), Server_qp.pQP_Data[idx_qp].rem_key, 1);

	Wait_For_IO_Request_Result(pIO_Cmd_ToSend_Other_Server->tag_magic, (RW_FUNC_RETURN*)(pIO_Cmd_ToSend_Other_Server->rem_buff));
	if (pthread_mutex_unlock(&(pAccess_qp0_lock[idx_qp])) != 0) {
		perror("pthread_mutex_unlock");
		exit(2);
	}
	printf("DBG> Rank = %d result = %d nDataSize = %d\n", mpi_rank, pResult->ret_value, pResult->nDataSize);

	CFixedSizeMemAllcator.Free_a_Block(pMemAlloc);
//	ncx_slab_free(sp_CallReturnBuff, pMemAlloc);
}

int Request_Is_ParentDIr_Existing(int idx_server, char szPath[], int *pIdx_Parent_Dir)	// return 1 if existing, 0 if non-existing
{
	int ret;
	RW_FUNC_RETURN *pResult;
	IO_CMD_MSG *pIO_Cmd_ToSend_Other_Server;
	char *pNewMsg_ToSend, *pMemAlloc;
	PARENTDIR_FUNC_RETURN *pReturnResult_Dir_Exist;
	int idx_qp;

	idx_qp = (idx_server * NUM_THREAD_IO_WORKER_INTER_SERVER) + idx_qp_server;
        pMemAlloc = CFixedSizeMemAllcator.Allocate_a_Block();
//	pMemAlloc = (char*)ncx_slab_alloc(sp_CallReturnBuff, SIZE_FOR_NEW_MSG + sizeof(IO_CMD_MSG) + sizeof(RW_FUNC_RETURN) + 1*sizeof(int));
	pNewMsg_ToSend = (char*)pMemAlloc;
	pIO_Cmd_ToSend_Other_Server = (IO_CMD_MSG *)(pNewMsg_ToSend + SIZE_FOR_NEW_MSG);

	pIO_Cmd_ToSend_Other_Server->rkey = Server_qp.mr_shm_global->rkey;
	pIO_Cmd_ToSend_Other_Server->rem_buff = (void*)(pNewMsg_ToSend + SIZE_FOR_NEW_MSG + sizeof(IO_CMD_MSG));
	strcpy(pIO_Cmd_ToSend_Other_Server->szName, szPath);
	pIO_Cmd_ToSend_Other_Server->tag_magic = (int)(next(rseed) & 0xFFFFFFFF);
	pIO_Cmd_ToSend_Other_Server->op = IO_OP_MAGIC | RF_RW_OP_DIR_EXIST;

	pResult = (RW_FUNC_RETURN *)pIO_Cmd_ToSend_Other_Server->rem_buff;
	pResult->nDataSize = 0; // init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data.

	if (pthread_mutex_lock(&(pAccess_qp0_lock[idx_qp])) != 0) {
		perror("pthread_mutex_lock");
		exit(2);
	}
//	Server_qp.IB_Put(idx_qp, (void*)pIO_Cmd_ToSend_Other_Server, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].remote_addr_IO_CMD), Server_qp.pQP_Data[idx_qp].rem_key, sizeof(IO_CMD_MSG));
	Server_qp.IB_Put(idx_qp, (void*)pIO_Cmd_ToSend_Other_Server, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].remote_addr_IO_CMD), Server_qp.pQP_Data[idx_qp].rem_key, IO_Msg_Size_op);
	pNewMsg_ToSend[0] = TAG_NEW_REQUEST;	// new msg!
	Server_qp.IB_Put(idx_qp, (void*)pNewMsg_ToSend, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].remote_addr_new_msg), Server_qp.pQP_Data[idx_qp].rem_key, 1);

	Wait_For_IO_Request_Result(pIO_Cmd_ToSend_Other_Server->tag_magic, (RW_FUNC_RETURN*)(pIO_Cmd_ToSend_Other_Server->rem_buff));
	if (pthread_mutex_unlock(&(pAccess_qp0_lock[idx_qp])) != 0) {
		perror("pthread_mutex_unlock");
		exit(2);
	}
	pReturnResult_Dir_Exist = (PARENTDIR_FUNC_RETURN *)((char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int) );
	*pIdx_Parent_Dir = pReturnResult_Dir_Exist->idx_Parent_Dir;

	ret = pResult->ret_value;
//	ncx_slab_free(sp_CallReturnBuff, pMemAlloc);
        CFixedSizeMemAllcator.Free_a_Block(pMemAlloc);

	return ret;
}

void Request_Free_Stripe_Data(int idx_server, char szFileName[])
{
	IO_CMD_MSG *pIO_Cmd_ToSend_Other_Server;
	char *pNewMsg_ToSend, *pMemAlloc;
	int idx_qp;

	idx_qp = (idx_server * NUM_THREAD_IO_WORKER_INTER_SERVER) + idx_qp_server;
        pMemAlloc = CFixedSizeMemAllcator.Allocate_a_Block();

//	pMemAlloc = (char*)ncx_slab_alloc(sp_CallReturnBuff, SIZE_FOR_NEW_MSG + sizeof(IO_CMD_MSG) + sizeof(RW_FUNC_RETURN) + 1*sizeof(int));
	pNewMsg_ToSend = (char*)pMemAlloc;
	pIO_Cmd_ToSend_Other_Server = (IO_CMD_MSG *)(pNewMsg_ToSend + SIZE_FOR_NEW_MSG);

	strcpy(pIO_Cmd_ToSend_Other_Server->szName, szFileName);
	pIO_Cmd_ToSend_Other_Server->tag_magic = (int)(next(rseed) & 0xFFFFFFFF);
	pIO_Cmd_ToSend_Other_Server->op = IO_OP_MAGIC | RF_RW_OP_FREE_STRIPE_DATA;

	if (pthread_mutex_lock(&(pAccess_qp0_lock[idx_qp])) != 0) {
		perror("pthread_mutex_lock");
		exit(2);
	}
	Server_qp.IB_Put(idx_qp, (void*)pIO_Cmd_ToSend_Other_Server, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].remote_addr_IO_CMD), Server_qp.pQP_Data[idx_qp].rem_key, IO_Msg_Size_op);
	pNewMsg_ToSend[0] = TAG_NEW_REQUEST;	// new msg!
	Server_qp.IB_Put(idx_qp, (void*)pNewMsg_ToSend, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].remote_addr_new_msg), Server_qp.pQP_Data[idx_qp].rem_key, 1);

	// No need to wait other server finish this request. 
//	Wait_For_IO_Request_Result(pIO_Cmd_ToSend_Other_Server->tag_magic, (RW_FUNC_RETURN*)(pIO_Cmd_ToSend_Other_Server->rem_buff));
	if (pthread_mutex_unlock(&(pAccess_qp0_lock[idx_qp])) != 0) {
		perror("pthread_mutex_unlock");
		exit(2);
	}
//	ncx_slab_free(sp_CallReturnBuff, pMemAlloc);
        CFixedSizeMemAllcator.Free_a_Block(pMemAlloc);
}

// return 0 if succeed, -1 if fail
int Request_ParentDir_Add_Entry(int idx_server, char szPath[], int nLenParentDirName, int EntryType, int *pIdx_Parent_Dir)
{
	int ret;
	RW_FUNC_RETURN *pResult;
	IO_CMD_MSG *pIO_Cmd_ToSend_Other_Server;
	char *pNewMsg_ToSend, *pMemAlloc;
	PARENTDIR_FUNC_RETURN *pParentDir_Func_Ret;
	int idx_qp;

	idx_qp = (idx_server * NUM_THREAD_IO_WORKER_INTER_SERVER) + idx_qp_server;
//	pMemAlloc = (char*)ncx_slab_alloc(sp_CallReturnBuff, SIZE_FOR_NEW_MSG + sizeof(IO_CMD_MSG) + sizeof(RW_FUNC_RETURN) + 1*sizeof(int));
        pMemAlloc = CFixedSizeMemAllcator.Allocate_a_Block();
	pNewMsg_ToSend = (char*)pMemAlloc;
	pIO_Cmd_ToSend_Other_Server = (IO_CMD_MSG *)(pNewMsg_ToSend + SIZE_FOR_NEW_MSG);

	pIO_Cmd_ToSend_Other_Server->rkey = Server_qp.mr_shm_global->rkey;
	pIO_Cmd_ToSend_Other_Server->rem_buff = (void*)(pNewMsg_ToSend + SIZE_FOR_NEW_MSG + sizeof(IO_CMD_MSG));
	strcpy(pIO_Cmd_ToSend_Other_Server->szName, szPath);
	pIO_Cmd_ToSend_Other_Server->nLen_Parent_Dir_Name = nLenParentDirName;
	pIO_Cmd_ToSend_Other_Server->flag = EntryType;
	pIO_Cmd_ToSend_Other_Server->tag_magic = (int)(next(rseed) & 0xFFFFFFFF);
	pIO_Cmd_ToSend_Other_Server->op = IO_OP_MAGIC | RF_RW_OP_ADDENTRY_PARENT_DIR;

	pResult = (RW_FUNC_RETURN *)pIO_Cmd_ToSend_Other_Server->rem_buff;
	pResult->nDataSize = 0; // init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data.

	if (pthread_mutex_lock(&(pAccess_qp0_lock[idx_qp])) != 0) {
		perror("pthread_mutex_lock");
		exit(2);
	}
//	Server_qp.IB_Put(idx_qp, (void*)pIO_Cmd_ToSend_Other_Server, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].remote_addr_IO_CMD), Server_qp.pQP_Data[idx_qp].rem_key, sizeof(IO_CMD_MSG));
	Server_qp.IB_Put(idx_qp, (void*)pIO_Cmd_ToSend_Other_Server, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].remote_addr_IO_CMD), Server_qp.pQP_Data[idx_qp].rem_key, IO_Msg_Size_op);
	pNewMsg_ToSend[0] = TAG_NEW_REQUEST;	// new msg!
	Server_qp.IB_Put(idx_qp, (void*)pNewMsg_ToSend, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].remote_addr_new_msg), Server_qp.pQP_Data[idx_qp].rem_key, 1);

	Wait_For_IO_Request_Result(pIO_Cmd_ToSend_Other_Server->tag_magic, (RW_FUNC_RETURN*)(pIO_Cmd_ToSend_Other_Server->rem_buff));
	if (pthread_mutex_unlock(&(pAccess_qp0_lock[idx_qp])) != 0) {
		perror("pthread_mutex_unlock");
		exit(2);
	}
	if(pResult->ret_value == 0)	{
		pParentDir_Func_Ret = (PARENTDIR_FUNC_RETURN *)((char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int));
		*pIdx_Parent_Dir = pParentDir_Func_Ret->idx_Parent_Dir;
	}
	else	errno = pResult->myerrno;

	ret = pResult->ret_value;
//	ncx_slab_free(sp_CallReturnBuff, pMemAlloc);
        CFixedSizeMemAllcator.Free_a_Block(pMemAlloc);

	return ret;
}

void Request_ParentDir_Remove_Entry(char szEntryName_ToRemove[], int idx_server, int idx_Parent_Dir, int nLenParentDirName)
{
	RW_FUNC_RETURN *pResult;
	IO_CMD_MSG *pIO_Cmd_ToSend_Other_Server;
	char *pNewMsg_ToSend, *pMemAlloc;
	PARENTDIR_FUNC_RETURN *pParentDir_Func_Ret;
	int *pIntParam;
	int idx_qp;

	idx_qp = (idx_server * NUM_THREAD_IO_WORKER_INTER_SERVER) + idx_qp_server;
        pMemAlloc = CFixedSizeMemAllcator.Allocate_a_Block();
//	pMemAlloc = (char*)ncx_slab_alloc(sp_CallReturnBuff, SIZE_FOR_NEW_MSG + sizeof(IO_CMD_MSG) + sizeof(RW_FUNC_RETURN));
	pNewMsg_ToSend = (char*)pMemAlloc;
	pIO_Cmd_ToSend_Other_Server = (IO_CMD_MSG *)(pNewMsg_ToSend + SIZE_FOR_NEW_MSG);

	pIO_Cmd_ToSend_Other_Server->rkey = Server_qp.mr_shm_global->rkey;
	pIO_Cmd_ToSend_Other_Server->rem_buff = (void*)(pNewMsg_ToSend + SIZE_FOR_NEW_MSG + sizeof(IO_CMD_MSG));

	pIntParam = (int*)pIO_Cmd_ToSend_Other_Server->szName;
	pIntParam[0] = idx_Parent_Dir;
	pIntParam[1] = nLenParentDirName;
	strcpy((char*)(&(pIntParam[2])), szEntryName_ToRemove);

	pIO_Cmd_ToSend_Other_Server->tag_magic = (int)(next(rseed) & 0xFFFFFFFF);
	pIO_Cmd_ToSend_Other_Server->op = IO_OP_MAGIC | RF_RW_OP_REMOVEENTRY_PARENT_DIR;

	pResult = (RW_FUNC_RETURN *)pIO_Cmd_ToSend_Other_Server->rem_buff;
	pResult->nDataSize = 0; // init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data.

	if (pthread_mutex_lock(&(pAccess_qp0_lock[idx_qp])) != 0) {
		perror("pthread_mutex_lock");
		exit(2);
	}
//	Server_qp.IB_Put(idx_qp, (void*)pIO_Cmd_ToSend_Other_Server, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].remote_addr_IO_CMD), Server_qp.pQP_Data[idx_qp].rem_key, sizeof(IO_CMD_MSG));
	Server_qp.IB_Put(idx_qp, (void*)pIO_Cmd_ToSend_Other_Server, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].remote_addr_IO_CMD), Server_qp.pQP_Data[idx_qp].rem_key, IO_Msg_Size_op);
	pNewMsg_ToSend[0] = TAG_NEW_REQUEST;	// new msg!
	Server_qp.IB_Put(idx_qp, (void*)pNewMsg_ToSend, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].remote_addr_new_msg), Server_qp.pQP_Data[idx_qp].rem_key, 1);

	Wait_For_IO_Request_Result(pIO_Cmd_ToSend_Other_Server->tag_magic, (RW_FUNC_RETURN*)(pIO_Cmd_ToSend_Other_Server->rem_buff));
	if (pthread_mutex_unlock(&(pAccess_qp0_lock[idx_qp])) != 0) {
		perror("pthread_mutex_unlock");
		exit(2);
	}
	if(pResult->ret_value != 0)	errno = pResult->myerrno;

//	ncx_slab_free(sp_CallReturnBuff, pMemAlloc);
        CFixedSizeMemAllcator.Free_a_Block(pMemAlloc);
}


int my_mkdir(char szDirName[], int mode, int uid, int gid)
{
	char szParentDir[512];
	int dir_idx, file_idx, Parent_dir_idx, nLenParentDirName, nLenFileName, idx_server_ParentDir, bParentDirExisting;
	unsigned long long fn_hash=0, fn_hash_2=0;
	struct timespec t_spec;
	struct stat dir_stat;

//	printf("DBG> mkdir(%s)\n", szDirName);
	clock_gettime(CLOCK_REALTIME, &t_spec);
	fn_hash = XXH64(szDirName, strlen(szDirName), 0);
	pthread_mutex_lock(&(create_new_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
	dir_idx = p_Hash_Dir->DictSearch(szDirName, &elt_list_dir, &ht_table_dir, &fn_hash_2);
	if(dir_idx < 0)	{
		Parent_dir_idx = Query_Parent_Dir(szDirName, szParentDir, &nLenParentDirName, &nLenFileName, &idx_server_ParentDir);
		
		if(Parent_dir_idx >= 0)	{	// Parent dir locates on this server and exists! 
			bParentDirExisting = 1;
		}
		else	{
			if(idx_server_ParentDir == mpi_rank)	{	// Parent dir should be handled by this server, but it does NOT exists! 
				bParentDirExisting = 0;
			}
			else	{
				if(Request_ParentDir_Add_Entry(idx_server_ParentDir, szDirName, nLenParentDirName, DIR_ENT_TYPE_DIR, &Parent_dir_idx) == 0)	{	// succeed
					bParentDirExisting = 1;
				}
//				if(Request_Is_ParentDIr_Existing(idx_server_ParentDir, szParentDir, &Parent_dir_idx) == 1)	{	// existing
//				if(Request_ParentDir_Add_Entry(idx_server_ParentDir, szDirName, nLenParentDirName, DIR_ENT_TYPE_DIR, &Parent_dir_idx, &IdxEntry_in_Dir) == 0)	{	// succeed
//					bParentDirExisting = 1;
//				}
				else	{
					bParentDirExisting = 0;
				}
			}
		}

		if( bParentDirExisting )	{	// parent dir exists. 
//			pthread_mutex_lock(&ht_dir_lock);
			dir_idx = p_Hash_Dir->DictInsertAuto(szDirName, &elt_list_dir, &ht_table_dir, &nDir, 1);
//			nDir++;
//			pthread_mutex_unlock(&ht_dir_lock);

			strcpy(pDirMetaData[dir_idx].szDirName, szDirName);

			// init dir entry list hash table!
			pDirMetaData[dir_idx].nEntries = 0;	// init number of entries
			pDirMetaData[dir_idx].nMaxEntry = DEFAULT_MAX_ENTRY_PER_DIR;
			pDirMetaData[dir_idx].nEntryTriggerExpand = (int)(pDirMetaData[dir_idx].nMaxEntry * RATIO_DIRENTRY_HASHTABLE_EXPAND);
			pDirMetaData[dir_idx].nEntryTriggerShrink = (int)(pDirMetaData[dir_idx].nMaxEntry * RATIO_DIRENTRY_HASHTABLE_SHRINK);
			pDirMetaData[dir_idx].p_Hash_DirEntry = (CHASHTABLE_DirEntry *)ncx_slab_alloc(sp_DirEntryHashTableBuff, CHASHTABLE_DirEntry::GetStorageSize(DEFAULT_MAX_ENTRY_PER_DIR));
			pDirMetaData[dir_idx].p_Hash_DirEntry->DictCreate(DEFAULT_MAX_ENTRY_PER_DIR, &(pDirMetaData[dir_idx].elt_list_DirEntry), &(pDirMetaData[dir_idx].ht_table_DirEntry));	// init hash table
			pDirMetaData[dir_idx].nLenAllEntries = 0;

			// insert into file hash table too.
//			pthread_mutex_lock(&ht_file_lock);
			file_idx = p_Hash_File->DictInsertAuto(szDirName, &elt_list_file, &ht_table_file, &nFile, 1);
//			nFile++;
//			pthread_mutex_unlock(&ht_file_lock);

			strcpy(pMetaData[file_idx].szFileName, szDirName);
			pMetaData[file_idx].idx_Server_Parent_Dir = idx_server_ParentDir;
			pMetaData[file_idx].idx_Parent_Dir = Parent_dir_idx;
//			if(idx_server_ParentDir != mpi_rank)	{	// parent dir is on remote server. This is ONLY a tentertive value. 
//				pMetaData[file_idx].IdxEntry_in_Dir = IdxEntry_in_Dir;
//			}

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

			pthread_mutex_unlock(&(create_new_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));

			if(nLenParentDirName)	{
				if(mpi_rank == idx_server_ParentDir)	{	// the parent dir is handled by current server
					my_AddEntryInfo(file_idx, Parent_dir_idx);	// only for non-root directory. IdxEntry_in_Dir is updated inside this call. 
				}
				else	{	// Need to send request to remote server. ALREADY done in Request_ParentDir_Add_Entry(). 
					pMetaData[file_idx].idx_Parent_Dir = Parent_dir_idx;
				}
			}


//			printf("DBG> Rank = %d mkdir(%s) idx_dir_entry = %d idx_server_ParentDir = %d\n", mpi_rank, pMetaData[file_idx].szFileName, pMetaData[file_idx].IdxEntry_in_Dir, idx_server_ParentDir);
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

int my_openfile(size_t DataReturn[], char szFileName[], int oflags, ...)
{
	char szParentDir[512];
	int file_idx, dir_idx, bFlagCreate=0, bFlagTrunc=0, bAppend=0, nLenParentDirName, nLenFileName, idx_server_ParentDir, bParentDirExisting, nEntryType;
//	int dir_idx, bFlagCreate=0, bFlagTrunc=0, bAppend=0, nLenParentDirName, nLenFileName, idx_server_ParentDir, bParentDirExisting, nEntryType;
	unsigned long long fn_hash=0, fn_hash_2=0;
//	unsigned long int file_idx;
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

//	if(strcmp(szFileName, "/myfs/mdtets_S00/test-dir.0-0/mdtest_tree.0/file.mdtest.0.1")==0)	{
//		printf("DBG> szFileName = %s\n", szFileName);
//	}

//	file_idx = p_Hash_File->DictSearch(szFileName, &elt_list_file, &ht_table_file, &fn_hash);
	if(bFlagCreate)	{
		fn_hash = XXH64(szFileName, strlen(szFileName), 0);
		pthread_mutex_lock(&(create_new_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		file_idx = p_Hash_File->DictSearch(szFileName, &elt_list_file, &ht_table_file, &fn_hash_2);

		if(file_idx < 0)	{	// not existing. Create a new file
			dir_idx = Query_Parent_Dir(szFileName, szParentDir, &nLenParentDirName, &nLenFileName, &idx_server_ParentDir);

			if(dir_idx >= 0)	{	// Parent dir locates on this server and exists! 
				bParentDirExisting = 1;
			}
			else	{
				if(idx_server_ParentDir == mpi_rank)	{	// Parent dir should be handled by this server, but it does NOT exists! 
					bParentDirExisting = 0;
				}
				else	{
					if(oflags & O_DIRECTORY)	nEntryType = DIR_ENT_TYPE_DIR;
					else	nEntryType = DIR_ENT_TYPE_FILE;

					if(Request_ParentDir_Add_Entry(idx_server_ParentDir, szFileName, nLenParentDirName, nEntryType, &dir_idx) == 0)	{	// succeed
						bParentDirExisting = 1;
					}
//					if(Request_Is_ParentDIr_Existing(idx_server_ParentDir, szParentDir, &dir_idx) == 1)	{	// existing
//						bParentDirExisting = 1;
//					}
					else	{
						bParentDirExisting = 0;
					}
				}
			}

			if( bParentDirExisting )	{	// parent dir exists on current server!
				// insert into file hash table too.

//				pthread_mutex_lock(&ht_file_lock);
				file_idx = p_Hash_File->DictInsertAuto(szFileName, &elt_list_file, &ht_table_file, &nFile, 1);
//				nFile++;
//				pthread_mutex_unlock(&ht_file_lock);

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
				pMetaData[file_idx].idx_Server_Parent_Dir = idx_server_ParentDir;
				pMetaData[file_idx].idx_Parent_Dir = dir_idx;
//				if(idx_server_ParentDir != mpi_rank)	pMetaData[file_idx].IdxEntry_in_Dir = IdxEntry_in_Dir;

//				gettimeofday(&tm, NULL);
				clock_gettime(CLOCK_REALTIME, &t_spec);
				pMetaData[file_idx].idx_dir_ht = -1;	// regular file
				pMetaData[file_idx].nOpen = 1;

				pMetaData[file_idx].nExtraPointer = 0;
				pMetaData[file_idx].nMaxExtraPointer = 0;
				pMetaData[file_idx].MaxOffset = 0;
				pMetaData[file_idx].MaxDataRange = 0;
				pMetaData[file_idx].pExtraData = NULL;

				pMetaData[file_idx].st_dev = 0;	// const
				pMetaData[file_idx].st_ino = file_idx;
				pMetaData[file_idx].st_nlink = 1;
				pMetaData[file_idx].st_mode = 0x8180;	// regular file and -rw-------
				pMetaData[file_idx].st_uid = 800193;	// user id
				pMetaData[file_idx].st_gid = 25276;	// group id
				pMetaData[file_idx].st_rdev = 0;	// const
				pMetaData[file_idx].st_size = 0;	// zero size for a newly created file
				pMetaData[file_idx].st_blksize = DEFAULT_FILE_STAT_BLOCKSIZE;	// const. Larger size may lead to better performance!!!
				pMetaData[file_idx].st_blocks = 0;
				pMetaData[file_idx].st_atim.tv_sec = t_spec.tv_sec;
				pMetaData[file_idx].st_atim.tv_nsec = t_spec.tv_nsec;
				pMetaData[file_idx].st_mtim.tv_sec = t_spec.tv_sec;
				pMetaData[file_idx].st_mtim.tv_nsec = t_spec.tv_nsec;
				pMetaData[file_idx].st_ctim.tv_sec = t_spec.tv_sec;
				pMetaData[file_idx].st_ctim.tv_nsec = t_spec.tv_nsec;
	
				pMetaData[file_idx].idx_Parent_Dir = dir_idx;

				pthread_mutex_unlock(&(create_new_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));

				if(mpi_rank == idx_server_ParentDir)	my_AddEntryInfo(file_idx, dir_idx);
				else	{
					pMetaData[file_idx].idx_Parent_Dir = dir_idx;
				}
				DataReturn[0] = file_idx;	// for inode
				DataReturn[1] = 0;			// size of the file

//				printf("DBG> Rank = %d open(%s) idx_dir_entry = %d idx_server_ParentDir = %d\n", mpi_rank, pMetaData[file_idx].szFileName, pMetaData[file_idx].IdxEntry_in_Dir, idx_server_ParentDir);

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
			DataReturn[0] = file_idx;	// for inode
			DataReturn[1] = pMetaData[file_idx].st_size;			// size of the file
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
			DataReturn[0] = file_idx;	// for inode
			DataReturn[1] = pMetaData[file_idx].st_size;			// size of the file
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
//	if(bAppend)	{
//		fd_List[fd].idx_block = (pMetaData[idx_file].st_size < pMetaData[idx_file].nSizeAllocated) ? (pMetaData[idx_file].nDirectPointer + pMetaData[idx_file].nExtraPointer - 1) : (pMetaData[idx_file].nDirectPointer + pMetaData[idx_file].nExtraPointer);
//		fd_List[fd].Offset = pMetaData[idx_file].st_size;
//	}
//	else	{
//		fd_List[fd].idx_block = 0;
//		fd_List[fd].Offset = 0;
//	}

//	printf("DBG> open(%s) fd = %d idx = %d\n", pMetaData[idx_file].szFileName, fd, fd_List[fd].idx_file);
	return fd;
}

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! TO optimize lock !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
int my_close(int fd, META_DATA_ON_CLOSE *pMetaData_OnClose)
{
	int i, idx_file;
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
	if(pMetaData_OnClose)	{
		if(pMetaData[idx_file].st_mtim.tv_sec < pMetaData_OnClose->modification_time.tv_sec)	{
			pMetaData[idx_file].st_mtim.tv_sec = pMetaData_OnClose->modification_time.tv_sec;
			pMetaData[idx_file].st_mtim.tv_nsec = pMetaData_OnClose->modification_time.tv_nsec;
		}
		else if(pMetaData[idx_file].st_mtim.tv_sec == pMetaData_OnClose->modification_time.tv_sec)	{
			if(pMetaData[idx_file].st_mtim.tv_nsec < pMetaData_OnClose->modification_time.tv_nsec)	{
				pMetaData[idx_file].st_mtim.tv_nsec = pMetaData_OnClose->modification_time.tv_nsec;
			}
		}

		if(pMetaData[idx_file].st_size < pMetaData_OnClose->MaxDataRange)	{
			pMetaData[idx_file].st_size = pMetaData_OnClose->MaxDataRange;
			pMetaData[idx_file].st_blocks = pMetaData[idx_file].st_size >> 9;
		}
	}

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

void my_Free_Stripe_Data_Common(DirectPointer *pExtraData, int nExtraPointer)
{
	int i, count;
	void *Addr_List[MAX_NUM_BLOCKS_TO_FREE];

	i = 0;
	count = 0;
	while(i<nExtraPointer)	{
		if(pExtraData[i].AddressofData)	{
			Addr_List[count] = (void *)(pExtraData[i].AddressofData);
			count++;
			if(count == MAX_NUM_BLOCKS_TO_FREE_M1)	{
				Addr_List[count] = NULL;
				CQueue_Free_Mem.Enqueue(Addr_List, MAX_NUM_BLOCKS_TO_FREE_M1);
//				pMem_Allocator->Mem_Batch_Free(Addr_List);
				count = 0;	// reset
			}
		}
		i++;
	}
	if(count > 0)	{
		Addr_List[count] = NULL;
		CQueue_Free_Mem.Enqueue(Addr_List, count);
//		pMem_Allocator->Mem_Batch_Free(Addr_List);
		count = 0;	// reset
	}
	if(nExtraPointer >= MIN_BATCH_SIZE_FREE_EX_POINTER)	{	// free memory immediately! 
		ncx_slab_free(sp_ExtraPointers, pExtraData);
	}
	else	{
		CQueue_Free_Mem.Enqueue_Ex_Pointer(pExtraData);
	}
//	ncx_slab_free(sp_ExtraPointers, pExtraData);
}

void my_Free_Stripe_Data_Ext_Server(char szFileName[])
{
	void *Addr_List[MAX_NUM_BLOCKS_TO_FREE];
	unsigned long long fn_hash=0, fn_hash_2=0;
	int idx_file=-1;

	fn_hash = XXH64(szFileName, strlen(szFileName), 0);

	idx_file = p_Hash_LargeFile->DictSearch(szFileName, &elt_list_LargeFile, &ht_table_LargeFile, &fn_hash_2);
	pthread_mutex_lock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
//	idx_file = p_Hash_LargeFile->DictSearch(szFileName, &elt_list_LargeFile, &ht_table_LargeFile, &fn_hash_2);
	if(idx_file >= 0)	my_Free_Stripe_Data_Common(pStripeData[idx_file].pExtraData, pStripeData[idx_file].nExtraPointer);
	pthread_mutex_unlock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));

	if(idx_file >= 0)	{
//		pthread_mutex_lock(&ht_stripe_lock);
		p_Hash_LargeFile->DictDelete(szFileName, &elt_list_LargeFile, &ht_table_LargeFile, NULL, 0);	// remove hash table record
//		pthread_mutex_unlock(&ht_stripe_lock);
	}
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

//	pthread_mutex_lock(&(file_lock[file_idx & MAX_NUM_FILE_OP_LOCK_M1]));
/*
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
				nExtraPointerNewlyAllocated = pFileMetaInfo->nMaxExtraPointer + MAX((pFileMetaInfo->nMaxExtraPointer)>>2, DEFAULT_NUM_EXTRA_PT);
				pFileMetaInfo->pExtraData = (DirectPointer *)ncx_slab_alloc(sp_ExtraPointers, nExtraPointerNewlyAllocated*sizeof(DirectPointer));
				if(nExtraPointer)	memcpy(pFileMetaInfo->pExtraData, pExtraData_Org, sizeof(DirectPointer)*nExtraPointer);
				if(pExtraData_Org)	ncx_slab_free(sp_ExtraPointers, pExtraData_Org);
				printf("DBG> Resizing ExtraData. %d --> %d. Checking, pExtraData[0] = %x in Truncate_File().\n", pFileMetaInfo->nMaxExtraPointer, nExtraPointerNewlyAllocated, pFileMetaInfo->pExtraData[0].AddressofData);
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
//				memset(pExtraPointer, 0, sizeof(DirectPointer)*pFileMetaInfo->nMaxExtraPointer);	// reset the memory. Not necessary
				printf("DBG> Resizing ExtraData. %d --> 0. in Truncate_File().\n", pFileMetaInfo->nMaxExtraPointer);
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
			printf("DBG> memset(%p, 0, %ld)\n", (void*)(pFileMetaInfo->pExtraData[i-1].AddressofData + (size - pFileMetaInfo->pExtraData[i-1].FileOffset) ), pFileMetaInfo->pExtraData[i-1].FileOffset + pFileMetaInfo->pExtraData[i-1].DataBlockSize - size);
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
/*
			pFileMetaInfo->nSizeAllocated = pFileMetaInfo->pExtraData[idx_block-NUM_DIRCT_PT].FileOffset + pFileMetaInfo->pExtraData[idx_block-NUM_DIRCT_PT].DataBlockSize;
			pFileMetaInfo->st_size = size;
			pFileMetaInfo->st_blocks = (pFileMetaInfo->nSizeAllocated) >> 9;
		}
	}
*/
//	pthread_mutex_unlock(&(file_lock[file_idx & MAX_NUM_FILE_OP_LOCK_M1]));

	return 0;
}


void Append_DirectPointer_List(STRIPE_DATA_INFO *pStripeData, const ULongInt addr_newly_allocated, const size_t File_Offset, const ULongInt size_Allocated, const size_t FileOffsetMax)
{
	__m256i Stripe_Data;
	int nExtraPointer, nMaxExtraPointer;
	size_t nExtraPointerNewlyAllocated;
	DirectPointer *pExtraDataNew=NULL, *pExtraDataOrg;
	STRIPE_DATA_INFO *pStripeDataNew;
	size_t residue;
	
	nExtraPointer = pStripeData->nExtraPointer;
	nMaxExtraPointer = pStripeData->nMaxExtraPointer;
	pExtraDataOrg = pStripeData->pExtraData;

	Stripe_Data = _mm256_loadu_si256((__m256i *)pStripeData);	// make a local copy
	pStripeDataNew = (STRIPE_DATA_INFO *)(&Stripe_Data);

	if( nMaxExtraPointer < ( nExtraPointer + 1) )	{	// need to expand the array of pStripeData[]
		nExtraPointerNewlyAllocated = nMaxExtraPointer + MAX((nMaxExtraPointer>>2), DEFAULT_NUM_EXTRA_PT);
		pExtraDataNew = (DirectPointer *)ncx_slab_alloc(sp_ExtraPointers, nExtraPointerNewlyAllocated*sizeof(DirectPointer));
		pExtraDataNew[nExtraPointer].AddressofData = addr_newly_allocated;
		pExtraDataNew[nExtraPointer].FileOffset = File_Offset;
		pExtraDataNew[nExtraPointer].DataBlockSize = size_Allocated;
		if(nExtraPointer)	memcpy(pExtraDataNew, pExtraDataOrg, sizeof(DirectPointer)*nExtraPointer);

		pStripeDataNew->nExtraPointer ++;
		pStripeDataNew->nMaxExtraPointer = nExtraPointerNewlyAllocated;
		if( ( (pStripeDataNew->MaxOffset) >> SHIFT_FOR_DIV_FILE_STRIPE_SIZE) != ( (pStripeDataNew->MaxOffset + size_Allocated) >> SHIFT_FOR_DIV_FILE_STRIPE_SIZE) )	{	// crossed
			pStripeDataNew->MaxOffset += (size_Allocated + Stripe_Offset_Shift);
			residue = pStripeDataNew->MaxOffset & FILE_STRIPE_SIZE_M1;
			if(residue)	{
				pStripeDataNew->MaxOffset -= residue;
				pExtraDataNew[nExtraPointer].DataBlockSize -= residue;
			}
		}
		else	{
			pStripeDataNew->MaxOffset += size_Allocated;
		}
		pStripeDataNew->MaxDataRange = MAX(MIN(pStripeDataNew->MaxOffset, FileOffsetMax), pStripeDataNew->MaxDataRange);
		pStripeDataNew->pExtraData = pExtraDataNew;
		_mm256_storeu_si256 ((__m256i *)pStripeData, Stripe_Data);

//		if(pExtraDataOrg)	ncx_slab_free(sp_ExtraPointers, pExtraDataOrg);
//		if(pExtraDataOrg)	CQueue_Free_Mem.Enqueue_Ex_Pointer(pExtraDataOrg);

		if(pExtraDataOrg)	{
			if(nExtraPointer >= MIN_BATCH_SIZE_FREE_EX_POINTER)	{	// free memory immediately! 
				ncx_slab_free(sp_ExtraPointers, pExtraDataOrg);
			}
			else	{
				CQueue_Free_Mem.Enqueue_Ex_Pointer(pExtraDataOrg);
			}
		}
	}
	else	{
		pExtraDataOrg[nExtraPointer].AddressofData = addr_newly_allocated;
		pExtraDataOrg[nExtraPointer].FileOffset = File_Offset;
		pExtraDataOrg[nExtraPointer].DataBlockSize = size_Allocated;

		pStripeDataNew->nExtraPointer ++;
		if( ( (pStripeDataNew->MaxOffset) >> SHIFT_FOR_DIV_FILE_STRIPE_SIZE) != ( (pStripeDataNew->MaxOffset + size_Allocated) >> SHIFT_FOR_DIV_FILE_STRIPE_SIZE) )	{	// crossed
			pStripeDataNew->MaxOffset += (size_Allocated + Stripe_Offset_Shift);
			residue = pStripeDataNew->MaxOffset & FILE_STRIPE_SIZE_M1;
			if(residue)	{
				pStripeDataNew->MaxOffset -= residue;
				pExtraDataOrg[nExtraPointer].DataBlockSize -= residue;
			}
		}
		else	{
			pStripeDataNew->MaxOffset += size_Allocated;
		}
//		pStripeDataNew->MaxDataRange = pStripeDataNew->MaxOffset;
		pStripeDataNew->MaxDataRange = MAX(MIN(pStripeDataNew->MaxOffset, FileOffsetMax), pStripeDataNew->MaxDataRange);
		_mm256_storeu_si256 ((__m256i *)pStripeData, Stripe_Data);
	}

	if(nExtraPointer > 0)	{
		if(pStripeData->pExtraData[nExtraPointer].FileOffset == pStripeData->pExtraData[nExtraPointer-1].FileOffset)	{
			printf("DBG> Something really wrong!\n");
		}
	}

}

size_t my_write_stripe(int fd, const char *szFileName, int server_shift, const void *buf, size_t count, off_t offset)
{
	int Done = 0, i, nExtraPointer, nExtraPointerNewlyAllocated, idx_file, idx_Block, nBytesResidue, bInsertNewRecord;
	size_t nBytes_ToWrite, nBytes_Written, nBytes_Written_OneTime, nBytesToAllocate, nBytesJustAllocated, count_save;
	STRIPE_DATA_INFO *pStripeDataLocal=NULL;
	void *pNewBuff=NULL;
	unsigned long long fn_hash=0, fn_hash_2=0;
	size_t BaseOffset, offset_Loc=offset, nOffsetMax;

	count_save = count;
	nBytes_ToWrite = count;
	fn_hash = XXH64(szFileName, strlen(szFileName), 0);
	nOffsetMax = offset + count;
	BaseOffset = server_shift * FILE_STRIPE_SIZE;

//	pthread_mutex_lock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
	if(fd >= 0)	{	// The right server
		pthread_mutex_lock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		idx_file = fd_List[fd].idx_file;
		pStripeDataLocal = (STRIPE_DATA_INFO *)(&(pMetaData[idx_file].nExtraPointer));
	}
	else	{	// extended server
		idx_file = p_Hash_LargeFile->DictSearch(szFileName, &elt_list_LargeFile, &ht_table_LargeFile, &fn_hash_2);
		pthread_mutex_lock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		if(idx_file < 0)	{	// No record. Need to create an entry. 
//			pthread_mutex_lock(&ht_stripe_lock);
			idx_file = p_Hash_LargeFile->DictSearchAndInsertAuto(szFileName, &elt_list_LargeFile, &ht_table_LargeFile, &bInsertNewRecord);
//			pthread_mutex_unlock(&ht_stripe_lock);
			if(bInsertNewRecord)	{	// a newly inserted record. Not existing previously. 
				pStripeDataLocal = &(pStripeData[idx_file]);
				pStripeDataLocal->nExtraPointer = 0;
				pStripeDataLocal->nMaxExtraPointer = DEFAULT_NUM_EXTRA_PT;
				pStripeDataLocal->MaxOffset = BaseOffset;
				pStripeDataLocal->MaxDataRange = BaseOffset;
				pStripeDataLocal->pExtraData = (DirectPointer *)ncx_slab_alloc(sp_ExtraPointers, DEFAULT_NUM_EXTRA_PT*sizeof(DirectPointer));
			}
		}
		else	pStripeDataLocal = &(pStripeData[idx_file]);
	}

	nExtraPointer = pStripeDataLocal->nExtraPointer;

	while(nOffsetMax > pStripeDataLocal->MaxOffset)	{
		nBytesResidue = pStripeDataLocal->MaxOffset & FILE_STRIPE_SIZE_M1;
		nBytesToAllocate = MIN(nOffsetMax - pStripeDataLocal->MaxOffset, (FILE_STRIPE_SIZE-nBytesResidue));
		pNewBuff = pMem_Allocator->Mem_Alloc( nBytesToAllocate, &nBytesJustAllocated);
		assert( (pNewBuff != NULL) && (nBytesJustAllocated > 0) );
		Append_DirectPointer_List(pStripeDataLocal, (const ULongInt)pNewBuff, (const size_t)pStripeDataLocal->MaxOffset, (const ULongInt)nBytesJustAllocated, nOffsetMax);
	}
	if(pStripeDataLocal->MaxDataRange < nOffsetMax )  pStripeDataLocal->MaxDataRange = MAX(MIN(pStripeDataLocal->MaxOffset, nOffsetMax), pStripeDataLocal->MaxDataRange);

	idx_Block = Query_Index_StorageBlock_with_Offset_Stripe(pStripeDataLocal, offset);
	pthread_mutex_unlock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));

	nBytes_Written = 0;
	if(nExtraPointer == 0)	{	// a new file
		memcpy((char*)pStripeDataLocal->pExtraData[idx_Block].AddressofData + (offset-pStripeDataLocal->pExtraData[idx_Block].FileOffset), buf, count);
//		my_Adaptive_Write(idx_qp, loc_buf, lkey, rem_buf, rkey, count, (void*)((char*)pStripeDataLocal->pExtraData[idx_Block].AddressofData + (offset-pStripeDataLocal->pExtraData[idx_Block].FileOffset)));
//		pStripeDataLocal->MaxDataRange = MAX(pStripeDataLocal->MaxDataRange, offset_Loc+nBytes_Written_OneTime);
//		Atomic_Increase(offset_Loc+nBytes_Written_OneTime, &(pStripeDataLocal->MaxDataRange));
		nBytes_Written = count;
		nBytes_ToWrite -= count;
	}
	else	{
		while(Done == 0)	{
			if(idx_Block < pStripeDataLocal->nExtraPointer)	{
				if( (pStripeDataLocal->pExtraData[idx_Block].FileOffset + pStripeDataLocal->pExtraData[idx_Block].DataBlockSize - offset_Loc) <= nBytes_ToWrite)	{	// need to go on
					nBytes_Written_OneTime = pStripeDataLocal->pExtraData[idx_Block].FileOffset + pStripeDataLocal->pExtraData[idx_Block].DataBlockSize - offset_Loc;
					memcpy((void*)(pStripeDataLocal->pExtraData[idx_Block].AddressofData + offset_Loc - pStripeDataLocal->pExtraData[idx_Block].FileOffset), (char*)buf+nBytes_Written, nBytes_Written_OneTime);
//					my_Adaptive_Write(idx_qp, loc_buf, lkey, rem_buf, rkey, nBytes_Written_OneTime, (void*)(pStripeDataLocal->pExtraData[idx_Block].AddressofData + offset_Loc - pStripeDataLocal->pExtraData[idx_Block].FileOffset));
//					pStripeDataLocal->MaxDataRange = MAX(pStripeDataLocal->MaxDataRange, offset_Loc+nBytes_Written_OneTime);
//					Atomic_Increase(offset_Loc+nBytes_Written_OneTime, &(pStripeDataLocal->MaxDataRange));
					idx_Block++;	// move to next block!!!
				}
				else	{
					nBytes_Written_OneTime = nBytes_ToWrite;
					memcpy((void*)(pStripeDataLocal->pExtraData[idx_Block].AddressofData + offset_Loc - pStripeDataLocal->pExtraData[idx_Block].FileOffset), (char*)buf+nBytes_Written, nBytes_Written_OneTime);
//					my_Adaptive_Write(idx_qp, loc_buf, lkey, rem_buf, rkey, nBytes_Written_OneTime, (void*)(pStripeDataLocal->pExtraData[idx_Block].AddressofData + offset_Loc - pStripeDataLocal->pExtraData[idx_Block].FileOffset));
//					pStripeDataLocal->MaxDataRange = MAX(pStripeDataLocal->MaxDataRange, offset_Loc+nBytes_Written_OneTime);
//					Atomic_Increase(offset_Loc+nBytes_Written_OneTime, &(pStripeDataLocal->MaxDataRange));
				}
				nBytes_Written += nBytes_Written_OneTime;
				offset_Loc += nBytes_Written_OneTime;
				nBytes_ToWrite -= nBytes_Written_OneTime;
			}
			if( nBytes_Written >= count_save )	{
				Done = 1;
				break;
			}
		}
	}

	return nBytes_Written;
}

/*
size_t my_write_stripe(int fd, const char *szFileName, int server_shift, const void *buf, size_t count, off_t offset)
{
	int Done = 0, i, nExtraPointer, nExtraPointerNewlyAllocated, idx_file, idx_Block, nBytesResidue, bInsertNewRecord;
	size_t nBytes_ToWrite, nBytes_Written, nBytes_Written_OneTime, nOffsetMax, nBytesToAllocate, nBytesJustAllocated, count_save, MaxOffsetNew;
	STRIPE_DATA_INFO *pStripeDataLocal=NULL;
	void *pNewBuff=NULL;
	unsigned long long fn_hash=0, fn_hash_2=0;
	off_t BaseOffset, offset_Loc=offset;

	BaseOffset = server_shift * FILE_STRIPE_SIZE;
	count_save = count;
	nBytes_ToWrite = count;
	fn_hash = XXH64(szFileName, strlen(szFileName), 0);
	nOffsetMax = offset + count;

	pthread_mutex_lock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
	if(fd >= 0)	{	// The right server
		idx_file = fd_List[fd].idx_file;
		pStripeDataLocal = (STRIPE_DATA_INFO *)(&(pMetaData[idx_file].nExtraPointer));
	}
	else	{	// extended server
		idx_file = p_Hash_LargeFile->DictSearch(szFileName, &elt_list_LargeFile, &ht_table_LargeFile, &fn_hash_2);
		if(idx_file < 0)	{	// No record. Need to create an entry. 
			pthread_mutex_lock(&ht_stripe_lock);
			idx_file = p_Hash_LargeFile->DictSearchAndInsertAuto(szFileName, &elt_list_LargeFile, &ht_table_LargeFile, &bInsertNewRecord);
			pthread_mutex_unlock(&ht_stripe_lock);
			if(bInsertNewRecord)	{
				pStripeDataLocal = &(pStripeData[idx_file]);
				pStripeDataLocal->nExtraPointer = 0;
				pStripeDataLocal->nMaxExtraPointer = DEFAULT_NUM_EXTRA_PT;
				pStripeDataLocal->MaxOffset = BaseOffset;
				pStripeDataLocal->MaxDataRange = BaseOffset;
				pStripeDataLocal->pExtraData = (DirectPointer *)ncx_slab_alloc(sp_ExtraPointers, DEFAULT_NUM_EXTRA_PT*sizeof(DirectPointer));
			}
		}
		else	pStripeDataLocal = &(pStripeData[idx_file]);

	}

	nExtraPointer = pStripeDataLocal->nExtraPointer;
	MaxOffsetNew = pStripeDataLocal->MaxOffset;
	if(nOffsetMax > pStripeDataLocal->MaxOffset)	{	// Need to allocate memory to accommodate new data
		nBytesResidue = pStripeDataLocal->MaxOffset % FILE_STRIPE_SIZE;
		if(nBytesResidue)	{
			nBytesToAllocate = MIN(nOffsetMax - pStripeDataLocal->MaxOffset, FILE_STRIPE_SIZE-nBytesResidue);
			pNewBuff = pMem_Allocator->Mem_Alloc( nBytesToAllocate, &nBytesJustAllocated);
			assert( (pNewBuff != NULL) && (nBytesJustAllocated > 0) );
			MaxOffsetNew += nBytesToAllocate;
//			pStripeDataLocal->MaxOffset = pStripeDataLocal->MaxOffset + nBytesJustAllocated;
			pStripeDataLocal->MaxDataRange = pStripeDataLocal->MaxDataRange + nBytesToAllocate;
			Append_DirectPointer_List(pStripeDataLocal, (const ULongInt)pNewBuff, (const ULongInt)(pStripeDataLocal->MaxOffset), (const ULongInt)MIN(nBytesJustAllocated, FILE_STRIPE_SIZE-nBytesResidue));
			MaxOffsetNew = pStripeDataLocal->MaxOffset;
		}
		// now the allocated memory block should be FILE_STRIPE_SIZE aligned. 

		while(1)	{
			if(nOffsetMax > MaxOffsetNew)	{	// Need to allocate memory to accommodate new data
				nBytesToAllocate = MIN(nOffsetMax - MaxOffsetNew, FILE_STRIPE_SIZE);
				pNewBuff = pMem_Allocator->Mem_Alloc( nBytesToAllocate, &nBytesJustAllocated);
				assert( (pNewBuff != NULL) && (nBytesJustAllocated > 0) );
				pStripeDataLocal->MaxDataRange = MaxOffsetNew + nBytesToAllocate;
				if( (pStripeDataLocal->MaxOffset % FILE_STRIPE_SIZE == 0) && (pStripeDataLocal->nExtraPointer > 0) )	{
					pStripeDataLocal->MaxOffset += (File_Strip_Size_All_server - FILE_STRIPE_SIZE);
					pStripeDataLocal->MaxDataRange += (File_Strip_Size_All_server - FILE_STRIPE_SIZE);
					MaxOffsetNew = pStripeDataLocal->MaxOffset;
//					printf("DBG> offset = %ld size = %ld MaxDataRange = %ld\n", offset, count, pStripeDataLocal->MaxOffset);
				}
				Append_DirectPointer_List(pStripeDataLocal, (const ULongInt)pNewBuff, (const ULongInt)MaxOffsetNew, (const ULongInt)MIN(nBytesJustAllocated, FILE_STRIPE_SIZE-nBytesResidue));
				MaxOffsetNew = pStripeDataLocal->MaxOffset;
				// now the allocated memory block should be FILE_STRIPE_SIZE aligned. 
			}
			else	{
				break;
			}
		}
	}


	idx_Block = Query_Index_StorageBlock_with_Offset_Stripe(pStripeDataLocal, offset);
	pthread_mutex_unlock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));

	nBytes_Written = 0;

	if(nExtraPointer == 0)	{	// a new file
		memcpy((char*)pStripeDataLocal->pExtraData[idx_Block].AddressofData + (offset-pStripeDataLocal->pExtraData[idx_Block].FileOffset), buf, count);

		nBytes_Written = count;
		nBytes_ToWrite -= count;
	}
	else	{
		while(Done == 0)	{
			if(idx_Block < pStripeDataLocal->nExtraPointer)	{
				if( (pStripeDataLocal->pExtraData[idx_Block].FileOffset + pStripeDataLocal->pExtraData[idx_Block].DataBlockSize - offset_Loc) <= nBytes_ToWrite)	{	// need to go on
					nBytes_Written_OneTime = pStripeDataLocal->pExtraData[idx_Block].FileOffset + pStripeDataLocal->pExtraData[idx_Block].DataBlockSize - offset_Loc;
					memcpy((void*)(pStripeDataLocal->pExtraData[idx_Block].AddressofData + offset_Loc - pStripeDataLocal->pExtraData[idx_Block].FileOffset), (char*)buf+nBytes_Written, nBytes_Written_OneTime);
					pStripeDataLocal->MaxDataRange = MAX(pStripeDataLocal->MaxDataRange, offset_Loc+nBytes_Written_OneTime);
					idx_Block++;	// move to next block!!!
				}
				else	{
					nBytes_Written_OneTime = nBytes_ToWrite;
					memcpy((void*)(pStripeDataLocal->pExtraData[idx_Block].AddressofData + offset_Loc - pStripeDataLocal->pExtraData[idx_Block].FileOffset), (char*)buf+nBytes_Written, nBytes_Written_OneTime);
					pStripeDataLocal->MaxDataRange = MAX(pStripeDataLocal->MaxDataRange, offset_Loc+nBytes_Written_OneTime);
				}
				nBytes_Written += nBytes_Written_OneTime;
				offset_Loc += nBytes_Written_OneTime;
				nBytes_ToWrite -= nBytes_Written_OneTime;
			}
			if( nBytes_Written >= count_save )	{
				Done = 1;
				break;
			}
		}
	}

	return nBytes_Written;
}
*/


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

			nSizeReg -= mr_tmp->length;
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

			nSizeReg -= mr_tmp->length;
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

			nSizeReg -= mr_tmp->length;
//			printf("DBG> nSizeReg = %ld\n", nSizeReg);

			ibv_dereg_mr(mr_tmp);
	}
	else	{	// use register local buffer RMDA then memcpy. 
		Server_qp.IB_Get(idx_qp, loc_buf, lkey, rem_buf, rkey, count);	// loc_buf was registered previously. 
		memcpy(dest_buf, loc_buf, count);
	}
}

size_t my_write_stripe_RDMA(int fd, const char *szFileName, int server_shift, int idx_qp, void *loc_buf, unsigned int lkey, void *rem_buf, unsigned int rkey, size_t count, off_t offset)
{
	int Done = 0, i, nExtraPointer, nExtraPointerNewlyAllocated, idx_file, idx_Block, bInsertNewRecord, nBytesResidue;
	size_t nBytes_ToWrite, nBytes_Written, nBytes_Written_OneTime, nBytesToAllocate, nBytesJustAllocated, count_save;
	STRIPE_DATA_INFO *pStripeDataLocal=NULL;
	void *pNewBuff=NULL;
	unsigned long long fn_hash=0, fn_hash_2=0;
	off_t BaseOffset, offset_Loc=offset, nOffsetMax;

	count_save = count;
	nBytes_ToWrite = count;
	fn_hash = XXH64(szFileName, strlen(szFileName), 0);
	nOffsetMax = offset + count;
	BaseOffset = server_shift * FILE_STRIPE_SIZE;

//	pthread_mutex_lock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
	if(fd >= 0)	{	// The right server
		pthread_mutex_lock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		idx_file = fd_List[fd].idx_file;
		pStripeDataLocal = (STRIPE_DATA_INFO *)(&(pMetaData[idx_file].nExtraPointer));
	}
	else	{	// extended server
		idx_file = p_Hash_LargeFile->DictSearch(szFileName, &elt_list_LargeFile, &ht_table_LargeFile, &fn_hash_2);
		pthread_mutex_lock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		if(idx_file < 0)	{	// No record. Need to create an entry. 
//			pthread_mutex_lock(&ht_stripe_lock);
			idx_file = p_Hash_LargeFile->DictSearchAndInsertAuto(szFileName, &elt_list_LargeFile, &ht_table_LargeFile, &bInsertNewRecord);
//			pthread_mutex_unlock(&ht_stripe_lock);
			if(bInsertNewRecord)	{	// a newly inserted record. Not existing previously. 
				pStripeDataLocal = &(pStripeData[idx_file]);
				pStripeDataLocal->nExtraPointer = 0;
				pStripeDataLocal->nMaxExtraPointer = DEFAULT_NUM_EXTRA_PT;
				pStripeDataLocal->MaxOffset = BaseOffset;
				pStripeDataLocal->MaxDataRange = BaseOffset;
				pStripeDataLocal->pExtraData = (DirectPointer *)ncx_slab_alloc(sp_ExtraPointers, DEFAULT_NUM_EXTRA_PT*sizeof(DirectPointer));
			}
		}
		else	pStripeDataLocal = &(pStripeData[idx_file]);
	}

	nExtraPointer = pStripeDataLocal->nExtraPointer;
	
	while(nOffsetMax > pStripeDataLocal->MaxOffset)	{
		nBytesResidue = pStripeDataLocal->MaxOffset & FILE_STRIPE_SIZE_M1;
		nBytesToAllocate = MIN(nOffsetMax - pStripeDataLocal->MaxOffset, (FILE_STRIPE_SIZE-nBytesResidue));
		pNewBuff = pMem_Allocator->Mem_Alloc( nBytesToAllocate, &nBytesJustAllocated);
		assert( (pNewBuff != NULL) && (nBytesJustAllocated > 0) );
		Append_DirectPointer_List(pStripeDataLocal, (const ULongInt)pNewBuff, (const size_t)pStripeDataLocal->MaxOffset, (const ULongInt)nBytesJustAllocated, nOffsetMax);
	}
	if(pStripeDataLocal->MaxDataRange < nOffsetMax )  pStripeDataLocal->MaxDataRange = MAX(MIN(pStripeDataLocal->MaxOffset, nOffsetMax), pStripeDataLocal->MaxDataRange);
	idx_Block = Query_Index_StorageBlock_with_Offset_Stripe(pStripeDataLocal, offset);
	pthread_mutex_unlock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));

	nBytes_Written = 0;
	if(nExtraPointer == 0)	{	// a new file
		my_Adaptive_Write(idx_qp, loc_buf, lkey, rem_buf, rkey, count, (void*)((char*)pStripeDataLocal->pExtraData[idx_Block].AddressofData + (offset-pStripeDataLocal->pExtraData[idx_Block].FileOffset)));
//		pStripeDataLocal->MaxDataRange = MAX(pStripeDataLocal->MaxDataRange, offset_Loc+nBytes_Written_OneTime);
//		Atomic_Increase(offset_Loc+nBytes_Written_OneTime, &(pStripeDataLocal->MaxDataRange));
		nBytes_Written = count;
		nBytes_ToWrite -= count;
	}
	else	{
		while(Done == 0)	{
			if(idx_Block < pStripeDataLocal->nExtraPointer)	{
				if( (pStripeDataLocal->pExtraData[idx_Block].FileOffset + pStripeDataLocal->pExtraData[idx_Block].DataBlockSize - offset_Loc) <= nBytes_ToWrite)	{	// need to go on
					nBytes_Written_OneTime = pStripeDataLocal->pExtraData[idx_Block].FileOffset + pStripeDataLocal->pExtraData[idx_Block].DataBlockSize - offset_Loc;
					my_Adaptive_Write(idx_qp, loc_buf, lkey, rem_buf, rkey, nBytes_Written_OneTime, (void*)(pStripeDataLocal->pExtraData[idx_Block].AddressofData + offset_Loc - pStripeDataLocal->pExtraData[idx_Block].FileOffset));
//					pStripeDataLocal->MaxDataRange = MAX(pStripeDataLocal->MaxDataRange, offset_Loc+nBytes_Written_OneTime);
//					Atomic_Increase(offset_Loc+nBytes_Written_OneTime, &(pStripeDataLocal->MaxDataRange));
					idx_Block++;	// move to next block!!!
				}
				else	{
					nBytes_Written_OneTime = nBytes_ToWrite;
					my_Adaptive_Write(idx_qp, loc_buf, lkey, rem_buf, rkey, nBytes_Written_OneTime, (void*)(pStripeDataLocal->pExtraData[idx_Block].AddressofData + offset_Loc - pStripeDataLocal->pExtraData[idx_Block].FileOffset));
//					pStripeDataLocal->MaxDataRange = MAX(pStripeDataLocal->MaxDataRange, offset_Loc+nBytes_Written_OneTime);
//					Atomic_Increase(offset_Loc+nBytes_Written_OneTime, &(pStripeDataLocal->MaxDataRange));
				}
				nBytes_Written += nBytes_Written_OneTime;
				offset_Loc += nBytes_Written_OneTime;
				nBytes_ToWrite -= nBytes_Written_OneTime;
			}
			if( nBytes_Written >= count_save )	{
				Done = 1;
				break;
			}
		}
	}

	return nBytes_Written;
}

size_t my_read_stripe(int fd, const char *szFileName, int server_shift, void *buf, size_t count, off_t offset)
{
	int Done = 0, i, nExtraPointer, idx_file, idx_Block;
	size_t nBytes_Read, nBytes_ToRead, nBytes_Read_OneTime, nAllocatedSize, nPrevOffset, count_save, nFileSize, nBytesLeft, nBytesLeftInThisBlock;
	STRIPE_DATA_INFO *pStripeDataLocal=NULL;
	DirectPointer *pExtraData=NULL;
	off_t BaseOffset, offset_Loc=offset, MaxDataRange;
	unsigned long long fn_hash=0, fn_hash2=0;

	BaseOffset = server_shift * FILE_STRIPE_SIZE;
	count_save = count;
	nBytes_Read = 0;
	nBytes_ToRead = count;

	fn_hash = XXH64(szFileName, strlen(szFileName), 0);
//	pthread_mutex_lock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
	if(fd >= 0)	{	// The right server
		pthread_mutex_lock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		idx_file = fd_List[fd].idx_file;
		pStripeDataLocal = (STRIPE_DATA_INFO *)(&(pMetaData[idx_file].nExtraPointer));
	}
	else	{	// extended server
		idx_file = p_Hash_LargeFile->DictSearch(szFileName, &elt_list_LargeFile, &ht_table_LargeFile, &fn_hash2);
		pthread_mutex_lock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		if(idx_file < 0)	{	// No record. Need to create an entry. 
			printf("Warning> Failed to find file %s on server %d in my_read_stripe().\n", szFileName, mpi_rank);
			pthread_mutex_unlock(&(file_lock[idx_file & MAX_NUM_FILE_OP_LOCK_M1]));
			return 0;
		}
		else	pStripeDataLocal = &(pStripeData[idx_file]);
	}

	idx_Block = Query_Index_StorageBlock_with_Offset_Stripe(pStripeDataLocal, offset);
	MaxDataRange = pStripeDataLocal->MaxDataRange;
	pthread_mutex_unlock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));

	nBytesLeft = MaxDataRange - offset;

	nExtraPointer = pStripeDataLocal->nExtraPointer;
	pExtraData = pStripeDataLocal->pExtraData;
	if(nExtraPointer == 0)	{	// a new empty file
		return 0;	// no data available
	}
	else	{
		while(Done == 0)	{
			if(idx_Block < nExtraPointer)	{
				nBytesLeftInThisBlock = MIN(pExtraData[idx_Block].FileOffset + pExtraData[idx_Block].DataBlockSize, MaxDataRange) - offset_Loc;
				if(nBytes_ToRead < nBytesLeftInThisBlock)	{	// no more blocks are needed. 
					nBytes_Read_OneTime = MIN(nBytes_ToRead, nBytesLeft);
					memcpy((char*)buf+nBytes_Read, (void*)(pExtraData[idx_Block].AddressofData + offset_Loc - pExtraData[idx_Block].FileOffset), nBytes_Read_OneTime);
				}
				else	{
					if(nBytesLeft > nBytesLeftInThisBlock)	{	// Need to access next block!!!
						nBytes_Read_OneTime = nBytesLeftInThisBlock;
						memcpy((char*)buf+nBytes_Read, (void*)(pExtraData[idx_Block].AddressofData + offset_Loc - pExtraData[idx_Block].FileOffset), nBytes_Read_OneTime);
						idx_Block++;	// move to next block!!!
					}
					else	{	// No need to access next block!!!
						nBytes_Read_OneTime = nBytesLeft;
						memcpy((char*)buf+nBytes_Read, (void*)(pExtraData[idx_Block].AddressofData + offset_Loc - pExtraData[idx_Block].FileOffset), nBytes_Read_OneTime);
					}
				}
				nBytes_Read += nBytes_Read_OneTime;
				offset_Loc += nBytes_Read_OneTime;
				nBytes_ToRead -= nBytes_Read_OneTime;
				nBytesLeft -= nBytes_Read_OneTime;
				if( (nBytesLeft == 0) || (nBytes_Read >= count_save) )	{	// reaching the end of file or finished reading requested size
					Done = 1;
					break;
				}
			}
			else break;
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

			nSizeReg -= mr_tmp->length;
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


			nSizeReg -= mr_tmp->length;
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


			nSizeReg -= mr_tmp->length;
//			printf("DBG> nSizeReg = %ld\n", nSizeReg);

		ibv_dereg_mr(mr_tmp);
	}
	else	{	// use register local buffer RMDA then memcpy. 
		memcpy(loc_buf, src_buf, count);
		Server_qp.IB_Put(idx_qp, loc_buf, lkey, rem_buf, rkey, count);	// loc_buf was registered previously. 
	}
}

size_t my_read_stripe_RDMA(int fd, const char *szFileName, int server_shift, int idx_qp, void *loc_buf, unsigned int lkey, void *rem_buf, unsigned int rkey, size_t count, off_t offset)
{
	int Done = 0, i, nExtraPointer, idx_file, idx_Block;
	size_t nBytes_Read, nBytes_ToRead, nBytes_Read_OneTime, nAllocatedSize, nPrevOffset, count_save, nFileSize, nBytesLeft, nBytesLeftInThisBlock;
	STRIPE_DATA_INFO *pStripeDataLocal=NULL;
	DirectPointer *pExtraData=NULL;
	off_t BaseOffset, offset_Loc=offset, MaxDataRange;
	unsigned long long fn_hash=0, fn_hash2=0;

	BaseOffset = server_shift * FILE_STRIPE_SIZE;
	count_save = count;
	nBytes_Read = 0;
	nBytes_ToRead = count;

	fn_hash = XXH64(szFileName, strlen(szFileName), 0);
//	pthread_mutex_lock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
	if(fd >= 0)	{	// The right server
		pthread_mutex_lock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		idx_file = fd_List[fd].idx_file;
		pStripeDataLocal = (STRIPE_DATA_INFO *)(&(pMetaData[idx_file].nExtraPointer));
	}
	else	{	// extended server
		idx_file = p_Hash_LargeFile->DictSearch(szFileName, &elt_list_LargeFile, &ht_table_LargeFile, &fn_hash2);
		pthread_mutex_lock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		if(idx_file < 0)	{	// No record. Need to create an entry. 
			printf("Warning> Failed to find file %s on server %d in my_read_stripe().\n", szFileName, mpi_rank);
			pthread_mutex_unlock(&(file_lock[idx_file & MAX_NUM_FILE_OP_LOCK_M1]));
			return 0;
		}
		else	pStripeDataLocal = &(pStripeData[idx_file]);
	}

	idx_Block = Query_Index_StorageBlock_with_Offset_Stripe(pStripeDataLocal, offset);
	MaxDataRange = pStripeDataLocal->MaxDataRange;
	pthread_mutex_unlock(&(file_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));

	nBytesLeft = MaxDataRange - offset;

	nExtraPointer = pStripeDataLocal->nExtraPointer;
	pExtraData = pStripeDataLocal->pExtraData;
	if(nExtraPointer == 0)	{	// a new empty file
		return 0;	// no data available
	}
	else	{
		while(Done == 0)	{
			if(idx_Block < nExtraPointer)	{
				nBytesLeftInThisBlock = MIN(pExtraData[idx_Block].FileOffset + pExtraData[idx_Block].DataBlockSize, MaxDataRange) - offset_Loc;
				if(nBytes_ToRead < nBytesLeftInThisBlock)	{	// no more blocks are needed. 
					nBytes_Read_OneTime = MIN(nBytes_ToRead, nBytesLeft);
//					memcpy((char*)buf+nBytes_Read, (void*)(pExtraData[idx_Block].AddressofData + offset_Loc - pExtraData[idx_Block].FileOffset), nBytes_Read_OneTime);
					my_Adaptive_Read(idx_qp, loc_buf, lkey, (void*)((char*)rem_buf+nBytes_Read), rkey, nBytes_Read_OneTime, (void*)(pExtraData[idx_Block].AddressofData + offset_Loc - pExtraData[idx_Block].FileOffset));
				}
				else	{
					if(nBytesLeft > nBytesLeftInThisBlock)	{	// Need to access next block!!!
						nBytes_Read_OneTime = nBytesLeftInThisBlock;
//						memcpy((char*)buf+nBytes_Read, (void*)(pExtraData[idx_Block].AddressofData + offset_Loc - pExtraData[idx_Block].FileOffset), nBytes_Read_OneTime);
						my_Adaptive_Read(idx_qp, loc_buf, lkey, (void*)((char*)rem_buf+nBytes_Read), rkey, nBytes_Read_OneTime, (void*)(pExtraData[idx_Block].AddressofData + offset_Loc - pExtraData[idx_Block].FileOffset));
						idx_Block++;	// move to next block!!!
					}
					else	{	// No need to access next block!!!
						nBytes_Read_OneTime = nBytesLeft;
//						memcpy((char*)buf+nBytes_Read, (void*)(pExtraData[idx_Block].AddressofData + offset_Loc - pExtraData[idx_Block].FileOffset), nBytes_Read_OneTime);
						my_Adaptive_Read(idx_qp, loc_buf, lkey, (void*)((char*)rem_buf+nBytes_Read), rkey, nBytes_Read_OneTime, (void*)(pExtraData[idx_Block].AddressofData + offset_Loc - pExtraData[idx_Block].FileOffset));
					}
				}
				nBytes_Read += nBytes_Read_OneTime;
				offset_Loc += nBytes_Read_OneTime;
				nBytes_ToRead -= nBytes_Read_OneTime;
				nBytesLeft -= nBytes_Read_OneTime;
				if( (nBytesLeft == 0) || (nBytes_Read >= count_save) )	{	// reaching the end of file or finished reading requested size
					Done = 1;
					break;
				}
			}
			else break;
		}
	}

	return nBytes_Read;
}

/*
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
					nBytes_Read_OneTime = MIN(count, nBytesLeft);
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
					nBytes_Read_OneTime = MIN(count, nBytesLeft);
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
*/


/*
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
					nBytes_Read_OneTime = MIN(count, nBytesLeft);
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
					nBytes_Read_OneTime = MIN(count, nBytesLeft);
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
*/
/*
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
*/

int Query_Index_StorageBlock_with_Offset_Stripe(STRIPE_DATA_INFO *pStripeData, off_t offset)
{
	int i, left, mid, right, Done = 0, nExtraPointer = pStripeData->nExtraPointer;
//	long int nAllocatedSize;
	DirectPointer *pExtraData = pStripeData->pExtraData;

	if(nExtraPointer < 1)	{	// empty file! 
		return 0;	// New size is larger than zero since zero size has been handled already. 
	}

//	nAllocatedSize = pExtraData[nExtraPointer-1].FileOffset + pExtraData[nExtraPointer-1].DataBlockSize;
	if(offset >= pStripeData->MaxOffset)	{	// Should never happen!!!
		printf("DBG> Unexpected condition in Query_Index_StorageBlock_with_Offset_Stripe().\n");
		return nExtraPointer;
	}
	else	{
		left = 0;
		right = nExtraPointer - 1;
		mid = (left + right) >> 1;	// (left + right)/2
		Done = 0;
		while(1)	{
			if(offset < pExtraData[mid].FileOffset)	{
				right = mid;
			}
			else	{
				left = mid;
			}
			mid = (left + right) >> 1;
			if( (left + 1) >= right )	{
				if( (offset>=pExtraData[left].FileOffset) && ( offset < (pExtraData[left].FileOffset + pExtraData[left].DataBlockSize) ) )	{
					Done = 1;
					return left;
				}
				else if( (offset>=pExtraData[right].FileOffset) && ( offset < (pExtraData[right].FileOffset + pExtraData[right].DataBlockSize) ) )	{
					Done = 1;
					return right;
				}
				if(! Done)	{
					char szHostName[128];
					gethostname(szHostName, 63);
					printf("ERROR> Fail to determine the index of data block! (pExtraData = %p, nExtraPointer = %d Offset = %td)\n", pExtraData, nExtraPointer, offset);
					printf("DBG> Hostname = %s pid = %d\n", szHostName, getpid());
					fflush(stdout);
					sleep(300);
				}
				break;
			}
		}
	}
}
/*
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
*/

void my_RemoveEntryInfo_Remote_Request(char szEntryName_ToRemove[], int idx_Parent_dir, int nLenParentDirName)
{
	int i, nMaxEntrySave;
	CHASHTABLE_DirEntry *pHT_DirEntrySave;
	struct elt_CharEntry *elt_list_DirEntry_Save;

	assert(idx_Parent_dir>=0);

//	pDirMetaData[idx_Parent_dir].p_Hash_DirEntry->DictDelete(szEntryName_ToRemove, &(pDirMetaData[idx_Parent_dir].elt_list_DirEntry), &(pDirMetaData[idx_Parent_dir].ht_table_DirEntry));
//	fetch_and_add(&(pDirMetaData[idx_Parent_dir].nLenAllEntries), -(strlen(szEntryName_ToRemove) + 2) );
//	fetch_and_add(&(pDirMetaData[idx_Parent_dir].nEntries), -1 );

//	fetch_and_add(&(pDirMetaData[idx_Parent_dir].nLenAllEntries), -(strlen(szEntryName_ToRemove) + 2) );
//	fetch_and_add(&(pDirMetaData[idx_Parent_dir].nEntries), -1 );
	pthread_mutex_lock(&(dir_entry_lock[idx_Parent_dir & MAX_NUM_FILE_OP_LOCK_M1]));

	pDirMetaData[idx_Parent_dir].p_Hash_DirEntry->DictDelete(szEntryName_ToRemove, &(pDirMetaData[idx_Parent_dir].elt_list_DirEntry), &(pDirMetaData[idx_Parent_dir].ht_table_DirEntry));
	pDirMetaData[idx_Parent_dir].nLenAllEntries -= (strlen(szEntryName_ToRemove) + 2);
	pDirMetaData[idx_Parent_dir].nEntries--;
	

	if( (pDirMetaData[idx_Parent_dir].nEntries < pDirMetaData[idx_Parent_dir].nEntryTriggerShrink) && (pDirMetaData[idx_Parent_dir].nMaxEntry > DEFAULT_MAX_ENTRY_PER_DIR) )	{	// NEED to shrink hashtable
//		pthread_mutex_lock(&(dir_entry_lock[idx_Parent_dir & MAX_NUM_FILE_OP_LOCK_M1]));
		nMaxEntrySave = pDirMetaData[idx_Parent_dir].nMaxEntry;
		pDirMetaData[idx_Parent_dir].nMaxEntry /= 2;
		pHT_DirEntrySave = pDirMetaData[idx_Parent_dir].p_Hash_DirEntry;	// save it. We need to free it after transfering to newly allocated hashtable
		elt_list_DirEntry_Save = pDirMetaData[idx_Parent_dir].elt_list_DirEntry;

		pDirMetaData[idx_Parent_dir].nEntryTriggerExpand = (int)(pDirMetaData[idx_Parent_dir].nMaxEntry * RATIO_DIRENTRY_HASHTABLE_EXPAND);
		pDirMetaData[idx_Parent_dir].nEntryTriggerShrink = (int)(pDirMetaData[idx_Parent_dir].nMaxEntry * RATIO_DIRENTRY_HASHTABLE_SHRINK);
		pDirMetaData[idx_Parent_dir].p_Hash_DirEntry = (CHASHTABLE_DirEntry *)ncx_slab_alloc(sp_DirEntryHashTableBuff, CHASHTABLE_DirEntry::GetStorageSize(pDirMetaData[idx_Parent_dir].nMaxEntry));
		pDirMetaData[idx_Parent_dir].p_Hash_DirEntry->DictCreate(pDirMetaData[idx_Parent_dir].nMaxEntry, &(pDirMetaData[idx_Parent_dir].elt_list_DirEntry), &(pDirMetaData[idx_Parent_dir].ht_table_DirEntry));	// init hash table

		// need to transfering old ht to new ht
		for(i=0; i<nMaxEntrySave; i++)	{	// loop all buckets in the orginal HT
			if(elt_list_DirEntry_Save[i].key[0])	{	// a valid record
				pDirMetaData[idx_Parent_dir].p_Hash_DirEntry->DictInsert(elt_list_DirEntry_Save[i].key, elt_list_DirEntry_Save[i].value, &(pDirMetaData[idx_Parent_dir].elt_list_DirEntry), &(pDirMetaData[idx_Parent_dir].ht_table_DirEntry) );
			}
		}
		pthread_mutex_unlock(&(dir_entry_lock[idx_Parent_dir & MAX_NUM_FILE_OP_LOCK_M1]));
//		ncx_slab_free(sp_DirEntryHashTableBuff, pHT_DirEntrySave);	// free the old HT
		CQueue_Free_Mem.Enqueue_Dir_Entry((void*)pHT_DirEntrySave);
	}
	else	pthread_mutex_unlock(&(dir_entry_lock[idx_Parent_dir & MAX_NUM_FILE_OP_LOCK_M1]));
}

void my_RemoveEntryInfo(int my_file_idx)
{
	char szParentDir[512];
	char szEntryName_ToRemove[256];
	int i, nMaxEntrySave, idx_Parent_dir;
	int nLenParentDirName, nLenFileName, idx_server_ParentDir, idx_file_server_EntryToMove;
	CHASHTABLE_DirEntry *pHT_DirEntrySave;
	struct elt_CharEntry *elt_list_DirEntry_Save;


	if(pMetaData[my_file_idx].idx_Server_Parent_Dir != mpi_rank)	{	// parent dir is NOT handled by this server
		idx_Parent_dir = Query_Parent_Dir(pMetaData[my_file_idx].szFileName, szParentDir, &nLenParentDirName, &nLenFileName, &idx_server_ParentDir);	// find out where is the parent dir!!!
		return Request_ParentDir_Remove_Entry(pMetaData[my_file_idx].szFileName+pMetaData[my_file_idx].nLenParentDirName+1, idx_server_ParentDir, pMetaData[my_file_idx].idx_Parent_Dir, nLenParentDirName);
	}
	strcpy(szEntryName_ToRemove, pMetaData[my_file_idx].szFileName+pMetaData[my_file_idx].nLenParentDirName+1);

	idx_Parent_dir = pMetaData[my_file_idx].idx_Parent_Dir;

	pthread_mutex_lock(&(dir_entry_lock[idx_Parent_dir & MAX_NUM_FILE_OP_LOCK_M1]));

	pDirMetaData[idx_Parent_dir].p_Hash_DirEntry->DictDelete(szEntryName_ToRemove, &(pDirMetaData[idx_Parent_dir].elt_list_DirEntry), &(pDirMetaData[idx_Parent_dir].ht_table_DirEntry));
	pDirMetaData[idx_Parent_dir].nLenAllEntries -= (strlen(szEntryName_ToRemove) + 2);
	pDirMetaData[idx_Parent_dir].nEntries--;

	if( (pDirMetaData[idx_Parent_dir].nEntries < pDirMetaData[idx_Parent_dir].nEntryTriggerShrink)  && (pDirMetaData[idx_Parent_dir].nMaxEntry > DEFAULT_MAX_ENTRY_PER_DIR)  )	{	// NEED to shrink hashtable
		nMaxEntrySave = pDirMetaData[idx_Parent_dir].nMaxEntry;
		pDirMetaData[idx_Parent_dir].nMaxEntry /= 2;
		pHT_DirEntrySave = pDirMetaData[idx_Parent_dir].p_Hash_DirEntry;	// save it. We need to free it after transfering to newly allocated hashtable
		elt_list_DirEntry_Save = pDirMetaData[idx_Parent_dir].elt_list_DirEntry;

		pDirMetaData[idx_Parent_dir].nEntryTriggerExpand = (int)(pDirMetaData[idx_Parent_dir].nMaxEntry * RATIO_DIRENTRY_HASHTABLE_EXPAND);
		pDirMetaData[idx_Parent_dir].nEntryTriggerShrink = (int)(pDirMetaData[idx_Parent_dir].nMaxEntry * RATIO_DIRENTRY_HASHTABLE_SHRINK);
		pDirMetaData[idx_Parent_dir].p_Hash_DirEntry = (CHASHTABLE_DirEntry *)ncx_slab_alloc(sp_DirEntryHashTableBuff, CHASHTABLE_DirEntry::GetStorageSize(pDirMetaData[idx_Parent_dir].nMaxEntry));
		pDirMetaData[idx_Parent_dir].p_Hash_DirEntry->DictCreate(pDirMetaData[idx_Parent_dir].nMaxEntry, &(pDirMetaData[idx_Parent_dir].elt_list_DirEntry), &(pDirMetaData[idx_Parent_dir].ht_table_DirEntry));	// init hash table

		// need to transfering old ht to new ht
		for(i=0; i<nMaxEntrySave; i++)	{	// loop all buckets in the orginal HT
			if(elt_list_DirEntry_Save[i].key[0])	{	// a valid record
				pDirMetaData[idx_Parent_dir].p_Hash_DirEntry->DictInsert(elt_list_DirEntry_Save[i].key, elt_list_DirEntry_Save[i].value, &(pDirMetaData[idx_Parent_dir].elt_list_DirEntry), &(pDirMetaData[idx_Parent_dir].ht_table_DirEntry) );
			}
		}
		pthread_mutex_unlock(&(dir_entry_lock[idx_Parent_dir & MAX_NUM_FILE_OP_LOCK_M1]));
//		ncx_slab_free(sp_DirEntryHashTableBuff, pHT_DirEntrySave);	// free the old HT
		CQueue_Free_Mem.Enqueue_Dir_Entry((void*)pHT_DirEntrySave);
	}
	else	pthread_mutex_unlock(&(dir_entry_lock[idx_Parent_dir & MAX_NUM_FILE_OP_LOCK_M1]));

	return;
}
/*
void my_UpdateEntryIndex_in_ParentDir(char szPath[], int NewIdxEntry_in_Dir)
{
	int file_idx;
	unsigned long long fn_hash=0;

	fn_hash = XXH64(szPath, strlen(szPath), 0);
	pthread_mutex_lock(&(create_new_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
	file_idx = p_Hash_File->DictSearch(szPath, &elt_list_file, &ht_table_file, &fn_hash);
//	if(file_idx < 0)	{
//		printf("DBG> Found it! file_idx = %d \n", file_idx);
//		int flag = 1;
//		while(flag)	{
//			sleep(1);
//		}
//	}
//	assert(file_idx >= 0);
	if(file_idx >= 0)	pMetaData[file_idx].IdxEntry_in_Dir = NewIdxEntry_in_Dir;
	else	{
		printf("Warning> file_idx (%d) < 0", file_idx);
	}
	pthread_mutex_unlock(&(create_new_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
}
*/

/*
int my_rmdir(char szDirName[])
{
	int dir_idx, file_idx;
	unsigned long long fn_hash=0, fn_hash_2=0;
	CHASHTABLE_DirEntry *p_Hash_DirEntry_ToFree=NULL;

//	printf("DBG> rmdir(%s)\n", szDirName);

	fn_hash = XXH64(szDirName, strlen(szDirName), 0);

	dir_idx = p_Hash_Dir->DictSearch(szDirName, &elt_list_dir, &ht_table_dir, &fn_hash_2);
	if(dir_idx < 0)	{
//		pthread_mutex_unlock(&(unlinkDir_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		errno = ENOENT;
		return (-1);
	}
	else	{
		if(pDirMetaData[dir_idx].nEntries > 0)	{
//			pthread_mutex_unlock(&(unlinkDir_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
			errno = ENOTEMPTY;	// Directory not empty
			return -1;
		}

		file_idx = p_Hash_File->DictSearch(szDirName, &elt_list_file, &ht_table_file, &fn_hash_2);
		assert(file_idx>0);
//	printf("DBG> Rank = %d rmdir(%s) idx_Server_Parent_Dir = %d idx_Parent_Dir = %d IdxEntry_in_Dir = %d\n", mpi_rank, szDirName, pMetaData[file_idx].idx_Server_Parent_Dir, pMetaData[file_idx].idx_Parent_Dir);
		pthread_mutex_lock(&(unlinkDir_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		my_RemoveEntryInfo(file_idx);
//		ncx_slab_free(sp_DirEntryHashTableBuff, (void*)(pDirMetaData[dir_idx].p_Hash_DirEntry));
		CQueue_Free_Mem.Enqueue_Dir_Entry((void*)(pDirMetaData[dir_idx].p_Hash_DirEntry));

//		pthread_mutex_lock(&ht_file_lock);
//		nFile--;
//		pthread_mutex_unlock(&ht_file_lock);

//		pthread_mutex_lock(&ht_dir_lock);
//		nDir--;
//		pthread_mutex_unlock(&ht_dir_lock);

		pthread_mutex_unlock(&(unlinkDir_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));

		p_Hash_File->DictDelete(szDirName, &elt_list_file, &ht_table_file, &nFile, -1);	// remove hash table record
		p_Hash_Dir->DictDelete(szDirName, &elt_list_dir, &ht_table_dir, &nDir, -1);	// remove hash table record
	}
	return 0;
}
*/

int my_rmdir(char szDirName[])
{
	int dir_idx, file_idx;
	unsigned long long fn_hash=0, fn_hash_2=0;
	CHASHTABLE_DirEntry *p_Hash_DirEntry_ToFree=NULL;

//	printf("DBG> rmdir(%s)\n", szDirName);

	fn_hash = XXH64(szDirName, strlen(szDirName), 0);

	dir_idx = p_Hash_Dir->DictSearch(szDirName, &elt_list_dir, &ht_table_dir, &fn_hash_2);
	if(dir_idx < 0)	{
//		pthread_mutex_unlock(&(unlinkDir_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		errno = ENOENT;
		return (-1);
	}
	else	{
		if(pDirMetaData[dir_idx].nEntries > 0)	{
//			pthread_mutex_unlock(&(unlinkDir_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
			errno = ENOTEMPTY;	// Directory not empty
			return -1;
		}

		file_idx = p_Hash_File->DictSearch(szDirName, &elt_list_file, &ht_table_file, &fn_hash_2);
		assert(file_idx>0);

		pthread_mutex_lock(&(unlinkDir_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		my_RemoveEntryInfo(file_idx);
//		ncx_slab_free(sp_DirEntryHashTableBuff, (void*)(pDirMetaData[dir_idx].p_Hash_DirEntry));
		CQueue_Free_Mem.Enqueue_Dir_Entry((void*)(pDirMetaData[dir_idx].p_Hash_DirEntry));
		pthread_mutex_unlock(&(unlinkDir_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));

		p_Hash_File->DictDelete(szDirName, &elt_list_file, &ht_table_file, &nFile, -1);	// remove hash table record
		p_Hash_Dir->DictDelete(szDirName, &elt_list_dir, &ht_table_dir, &nDir, -1);	// remove hash table record
	}
	return 0;
}

int my_unlink(char szFileName[], size_t *nFileSize)	// remove a regular file!
{
	int file_idx=-1;
	unsigned long long fn_hash=0, fn_hash_2=0;

//	printf("DBG> unlink(%s)\n", szFileName);

	fn_hash = XXH64(szFileName, strlen(szFileName), 0);
	file_idx = p_Hash_File->DictSearch(szFileName, &elt_list_file, &ht_table_file, &fn_hash_2);
	pthread_mutex_lock(&(unlink_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
	if(file_idx < 0)	{
		*nFileSize = 0;
		szFileName[0] = 0;
		pthread_mutex_unlock(&(unlink_lock[fn_hash & MAX_NUM_FILE_OP_LOCK_M1]));
		errno = ENOENT;
		return (-1);
	}
	else	{
		*nFileSize = pMetaData[file_idx].st_size;	// return file size
		if(pMetaData[file_idx].nExtraPointer)	my_Free_Stripe_Data_Common(pMetaData[file_idx].pExtraData, pMetaData[file_idx].nExtraPointer);

		my_RemoveEntryInfo(file_idx);
	}
	if(file_idx >= 0)	p_Hash_File->DictDelete(pMetaData[file_idx].szFileName, &elt_list_file, &ht_table_file, &nFile, -1);

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
	int i, nMaxEntrySave, nLenEntryName, EntryType;
	int *p_nEntryNameOffset=NULL, *p_nEntryNameOffsetNew=NULL;
	char *pEntryName;
	CHASHTABLE_DirEntry *pHT_DirEntrySave;
	struct elt_CharEntry *elt_list_DirEntry_Save;

	pthread_mutex_lock(&(dir_entry_lock[dir_idx & MAX_NUM_FILE_OP_LOCK_M1]));

	pEntryName = pMetaData[my_file_idx].szFileName+pMetaData[my_file_idx].nLenParentDirName + 1;
	nLenEntryName = my_strlen(pEntryName);

	// assume pEntryName DOES NOT exist in hashtable. This should be always true! 
//	EntryNameAddrOffset = (int) ( (long int)(ncx_slab_alloc(sp_DirEntryName, nLenEntryName+2)) - (long int)p_DirEntryNameBuff );	// only store offset
//	strcpy(p_DirEntryNameBuff+EntryNameAddrOffset + 1, pEntryName);
//	p_DirEntryNameBuff[EntryNameAddrOffset] = (pMetaData[my_file_idx].idx_dir_ht >= 0) ? (DIR_ENT_TYPE_DIR) : (DIR_ENT_TYPE_FILE);	// !!!!!!!!!!!!!!!!!!!!!!!!
	EntryType = (pMetaData[my_file_idx].idx_dir_ht >= 0) ? (DIR_ENT_TYPE_DIR) : (DIR_ENT_TYPE_FILE);	// !!!!!!!!!!!!!!!!!!!!!!!!
	pDirMetaData[dir_idx].p_Hash_DirEntry->DictInsert(pEntryName, EntryType, &(pDirMetaData[dir_idx].elt_list_DirEntry), &(pDirMetaData[dir_idx].ht_table_DirEntry));

//	p_nEntryNameOffset = (int *)(p_DirEntryNameOffsetBuff + pDirMetaData[dir_idx].nOffset_To_EntryNameOffsetList);
//	p_nEntryNameOffset[nEntry] = (int) ( (long int)(ncx_slab_alloc(sp_DirEntryName, nLenEntryName+2)) - (long int)p_DirEntryNameBuff );	// only store offset
//	strcpy(p_DirEntryNameBuff+p_nEntryNameOffset[nEntry] + 1, pEntryName);
//	p_DirEntryNameBuff[p_nEntryNameOffset[nEntry]] = (pMetaData[my_file_idx].idx_dir_ht >= 0) ? (DIR_ENT_TYPE_DIR) : (DIR_ENT_TYPE_FILE);	// !!!!!!!!!!!!!!!!!!!!!!!!
//	p_DirEntryNameBuff[p_nEntryNameOffset[nEntry]] = (char)(pRf_Op_Msg->flag & 0xFF);	// !!!!!!!!!!!!!!!!!!!!!!!!
//	pMetaData[my_file_idx].IdxEntry_in_Dir = nEntry;

//	printf("DBG> Rank = %d my_AddEntryInfo(), %d entries in %s\nLists: ", mpi_rank, nEntry, pDirMetaData[dir_idx].szDirName);
//	for(int i=0; i<nEntry; i++)	{
//		printf(" %s", p_DirEntryNameBuff + p_nEntryNameOffset[i] + 1);
//	}
//	printf("\nTo Add %s\n", pEntryName);

	pDirMetaData[dir_idx].nEntries++;
	pDirMetaData[dir_idx].nLenAllEntries += (nLenEntryName+2);
	if( pDirMetaData[dir_idx].nEntries >= pDirMetaData[dir_idx].nEntryTriggerExpand )	{	// NEED to expand hashtable
		nMaxEntrySave = pDirMetaData[dir_idx].nMaxEntry;
		pDirMetaData[dir_idx].nMaxEntry *= 2;
		pHT_DirEntrySave = pDirMetaData[dir_idx].p_Hash_DirEntry;	// save it. We need to free it after transfering to newly allocated hashtable
		elt_list_DirEntry_Save = pDirMetaData[dir_idx].elt_list_DirEntry;

		pDirMetaData[dir_idx].nEntryTriggerExpand = (int)(pDirMetaData[dir_idx].nMaxEntry * RATIO_DIRENTRY_HASHTABLE_EXPAND);
		pDirMetaData[dir_idx].nEntryTriggerShrink = (int)(pDirMetaData[dir_idx].nMaxEntry * RATIO_DIRENTRY_HASHTABLE_SHRINK);
		pDirMetaData[dir_idx].p_Hash_DirEntry = (CHASHTABLE_DirEntry *)ncx_slab_alloc(sp_DirEntryHashTableBuff, CHASHTABLE_DirEntry::GetStorageSize(pDirMetaData[dir_idx].nMaxEntry));
		pDirMetaData[dir_idx].p_Hash_DirEntry->DictCreate(pDirMetaData[dir_idx].nMaxEntry, &(pDirMetaData[dir_idx].elt_list_DirEntry), &(pDirMetaData[dir_idx].ht_table_DirEntry));	// init hash table

		// need to transfering old ht to new ht
		for(i=0; i<nMaxEntrySave; i++)	{	// loop all buckets in the orginal HT
			if(elt_list_DirEntry_Save[i].key[0])	{	// a valid record
				pDirMetaData[dir_idx].p_Hash_DirEntry->DictInsert(elt_list_DirEntry_Save[i].key, elt_list_DirEntry_Save[i].value, &(pDirMetaData[dir_idx].elt_list_DirEntry), &(pDirMetaData[dir_idx].ht_table_DirEntry) );
			}
		}

		pthread_mutex_unlock(&(dir_entry_lock[dir_idx & MAX_NUM_FILE_OP_LOCK_M1]));
//		ncx_slab_free(sp_DirEntryHashTableBuff, elt_list_DirEntry_Save);	// free the old HT
//		ncx_slab_free(sp_DirEntryHashTableBuff, pHT_DirEntrySave);	// free the old HT
		CQueue_Free_Mem.Enqueue_Dir_Entry((void*)pHT_DirEntrySave);
	}
	else	pthread_mutex_unlock(&(dir_entry_lock[dir_idx & MAX_NUM_FILE_OP_LOCK_M1]));

	return 0;
}

int my_AddEntryInfo_Remote_Request(char *szFullName, int nLenParentDirName, int EntryType, int *pIdx_Parent_Dir)
{
	int i, nLenEntryName, dir_idx, nMaxEntrySave;
//	int *p_nEntryNameOffset=NULL, *p_nEntryNameOffsetNew=NULL;
	char *pEntryName;
	unsigned long long int fn_hash;
	char szParentDirName[512];
	CHASHTABLE_DirEntry *pHT_DirEntrySave;
	struct elt_CharEntry *elt_list_DirEntry_Save;

	memcpy(szParentDirName, szFullName, nLenParentDirName);
	szParentDirName[nLenParentDirName] = 0;

	dir_idx = p_Hash_Dir->DictSearch(szParentDirName, &elt_list_dir, &ht_table_dir, &fn_hash);
	*pIdx_Parent_Dir = dir_idx;

	if(dir_idx < 0 )	{	// Parent dir does not exist!!!
		errno = ENOENT;
		return (-1);
	}

	pthread_mutex_lock(&(dir_entry_lock[dir_idx & MAX_NUM_FILE_OP_LOCK_M1]));
	pEntryName = szFullName + nLenParentDirName + 1;
	nLenEntryName = my_strlen(pEntryName);
	// assume pEntryName DOES NOT exist in hashtable. This should be always true! 
//	EntryNameAddrOffset = (int) ( (long int)(ncx_slab_alloc(sp_DirEntryName, nLenEntryName+2)) - (long int)p_DirEntryNameBuff );	// only store offset
//	strcpy(p_DirEntryNameBuff+EntryNameAddrOffset + 1, pEntryName);
//	p_DirEntryNameBuff[EntryNameAddrOffset] = char(EntryType & 0xFF);	// !!!!!!!!!!!!!!!!!!!!!!!!
	pDirMetaData[dir_idx].p_Hash_DirEntry->DictInsert(pEntryName, EntryType, &(pDirMetaData[dir_idx].elt_list_DirEntry), &(pDirMetaData[dir_idx].ht_table_DirEntry));

	pDirMetaData[dir_idx].nEntries++;
	pDirMetaData[dir_idx].nLenAllEntries += (nLenEntryName+2);
	if( pDirMetaData[dir_idx].nEntries >= pDirMetaData[dir_idx].nEntryTriggerExpand )	{	// NEED to expand hashtable
		nMaxEntrySave = pDirMetaData[dir_idx].nMaxEntry;
		pDirMetaData[dir_idx].nMaxEntry *= 2;
		pHT_DirEntrySave = pDirMetaData[dir_idx].p_Hash_DirEntry;	// save it. We need to free it after transfering to newly allocated hashtable
		elt_list_DirEntry_Save = pDirMetaData[dir_idx].elt_list_DirEntry;

		pDirMetaData[dir_idx].nEntryTriggerExpand = (int)(pDirMetaData[dir_idx].nMaxEntry * RATIO_DIRENTRY_HASHTABLE_EXPAND);
		pDirMetaData[dir_idx].nEntryTriggerShrink = (int)(pDirMetaData[dir_idx].nMaxEntry * RATIO_DIRENTRY_HASHTABLE_SHRINK);
		pDirMetaData[dir_idx].p_Hash_DirEntry = (CHASHTABLE_DirEntry *)ncx_slab_alloc(sp_DirEntryHashTableBuff, CHASHTABLE_DirEntry::GetStorageSize(pDirMetaData[dir_idx].nMaxEntry));
		pDirMetaData[dir_idx].p_Hash_DirEntry->DictCreate(pDirMetaData[dir_idx].nMaxEntry, &(pDirMetaData[dir_idx].elt_list_DirEntry), &(pDirMetaData[dir_idx].ht_table_DirEntry));	// init hash table

		// need to transfering old ht to new ht
		for(i=0; i<nMaxEntrySave; i++)	{	// loop all buckets in the orginal HT
			if(elt_list_DirEntry_Save[i].key[0])	{	// a valid record
				pDirMetaData[dir_idx].p_Hash_DirEntry->DictInsert(elt_list_DirEntry_Save[i].key, elt_list_DirEntry_Save[i].value, &(pDirMetaData[dir_idx].elt_list_DirEntry), &(pDirMetaData[dir_idx].ht_table_DirEntry) );
			}
		}

		pthread_mutex_unlock(&(dir_entry_lock[dir_idx & MAX_NUM_FILE_OP_LOCK_M1]));
//		ncx_slab_free(sp_DirEntryHashTableBuff, pHT_DirEntrySave);	// free the old HT
		CQueue_Free_Mem.Enqueue_Dir_Entry((void*)pHT_DirEntrySave);
	}
	else	pthread_mutex_unlock(&(dir_entry_lock[dir_idx & MAX_NUM_FILE_OP_LOCK_M1]));

/*
	// append the new file/dir at the end of parent dir entry list
	pEntryName = szFullName + nLenParentDirName + 1;
	nLenEntryName = my_strlen(pEntryName);
	nEntry = pDirMetaData[*pIdx_Parent_Dir].nEntries;
	p_nEntryNameOffset = (int *)(p_DirEntryNameOffsetBuff + pDirMetaData[*pIdx_Parent_Dir].nOffset_To_EntryNameOffsetList);
	p_nEntryNameOffset[nEntry] = (int) ( (long int)(ncx_slab_alloc(sp_DirEntryName, nLenEntryName+2)) - (long int)p_DirEntryNameBuff );	// only store offset
	strcpy(p_DirEntryNameBuff+p_nEntryNameOffset[nEntry] + 1, pEntryName);
	p_DirEntryNameBuff[p_nEntryNameOffset[nEntry]] = char(EntryType & 0xFF);
	*pIdxEntry_in_Dir = nEntry;

//	printf("DBG> Rank = %d my_AddEntryInfo_Remote_Request(), %d entries in %s\nLists: ", mpi_rank, nEntry, pDirMetaData[*pIdx_Parent_Dir].szDirName);
//	for(int i=0; i<nEntry; i++)	{
//		printf(" %s", p_DirEntryNameBuff + p_nEntryNameOffset[i] + 1);
//	}
//	printf("\nTo Add %s\n", pEntryName);

//	pMetaData[my_file_idx].IdxEntry_in_Dir = nEntry;	// NEED to update this on the server who sent this request!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	pDirMetaData[*pIdx_Parent_Dir].nEntries++;
	pDirMetaData[*pIdx_Parent_Dir].nLenAllEntries += (nLenEntryName+2);
	if(pDirMetaData[*pIdx_Parent_Dir].nEntries >= pDirMetaData[*pIdx_Parent_Dir].nMaxEntry)	{
		pDirMetaData[*pIdx_Parent_Dir].nMaxEntry *= 2;
		p_nEntryNameOffsetNew = (int *)ncx_slab_alloc(sp_DirEntryNameOffset, pDirMetaData[*pIdx_Parent_Dir].nMaxEntry*sizeof(int));
		memcpy((void*)p_nEntryNameOffsetNew, (void*)p_nEntryNameOffset, sizeof(int)*pDirMetaData[*pIdx_Parent_Dir].nEntries);
		pDirMetaData[*pIdx_Parent_Dir].nOffset_To_EntryNameOffsetList = (long int)p_nEntryNameOffsetNew - (long int)p_DirEntryNameOffsetBuff;
		pthread_mutex_unlock(&(dir_entry_lock[(*pIdx_Parent_Dir) & MAX_NUM_FILE_OP_LOCK_M1]));
		ncx_slab_free(sp_DirEntryNameOffset, (void*)p_nEntryNameOffset);	// free in memory pool
	}
	else	pthread_mutex_unlock(&(dir_entry_lock[(*pIdx_Parent_Dir) & MAX_NUM_FILE_OP_LOCK_M1]));
*/
//	printf("DBG> my_AddEntryInfo_Remote_Request(%s). Dir %s nEntries = %d\n", 
//		szFullName, pDirMetaData[*pIdx_Parent_Dir].szDirName, pDirMetaData[*pIdx_Parent_Dir].nEntries);

	return 0;
}

int my_ls(char szDirName[])	// list entries under a directory
{
	int i, dir_idx, nEntry;
	unsigned long long fn_hash=0;
	unsigned char *szDirEntryBuff=NULL;// entry data buffer
//	int *p_nBytesDirEntryBuff, *p_nDirEntries, *p_DirEntryOffset, nBytesDirEntryNameAccu=0, nBytesEntryName, *p_nEntryNameOffset;
	int *p_nBytesDirEntryBuff, *p_nDirEntries, *p_DirEntryOffset, nBytesDirEntryNameAccu=0, nBytesEntryName;
	char *p_szDirEntryName;
	struct elt_CharEntry *elt_list_DirEntry;

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
		elt_list_DirEntry = pDirMetaData[dir_idx].elt_list_DirEntry;
		nEntry = 0;
		for(i=0; i<pDirMetaData[dir_idx].nMaxEntry; i++)	{
			if(elt_list_DirEntry[i].key[0])	{	// a valid entry. 
				nEntry++;
				nBytesEntryName = strlen(elt_list_DirEntry[i].key);
				strcpy(p_szDirEntryName + nBytesDirEntryNameAccu + 1, elt_list_DirEntry[i].key);
				p_szDirEntryName[nBytesDirEntryNameAccu] = (char)(elt_list_DirEntry[i].value & 0xFF);
				printf("DBG> %d entry, %s\n", nEntry, p_szDirEntryName + nBytesDirEntryNameAccu + 1);
				p_DirEntryOffset[i] = nBytesDirEntryNameAccu;
				nBytesDirEntryNameAccu += (nBytesEntryName + 2);
			}
		}
/*
		for(i=0; i<nEntry; i++)	{
			p_nEntryNameOffset = (int *)(p_DirEntryNameOffsetBuff + pDirMetaData[dir_idx].nOffset_To_EntryNameOffsetList);
			nBytesEntryName = strlen(p_DirEntryNameBuff + p_nEntryNameOffset[i]);
			strcpy(p_szDirEntryName + nBytesDirEntryNameAccu, p_DirEntryNameBuff + p_nEntryNameOffset[i]);	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
//			strcpy(p_szDirEntryName + nBytesDirEntryNameAccu, p_DirEntryNameBuff + p_nEntryNameOffset[i] + 1);	// !!!!!!!!!!!!!!!!!!!!!!!!!!
			printf("DBG> %d entry, %s\n", i+1, p_szDirEntryName + nBytesDirEntryNameAccu + 1);
			p_DirEntryOffset[i] = nBytesDirEntryNameAccu;
			nBytesDirEntryNameAccu += (nBytesEntryName + 1);
		}
*/
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
	return my_opendir_by_index(dir_idx, loc_buf, 0, MAX_SIZE_DIR_ENTRY_BUFF_PER_REQUEST);
}

int my_opendir_by_index(int dir_idx, void *loc_buf, long int offset, long int nBuffSize)
{
	int i, nEntry, nEntryToSend, nEntry_Save, nMaxEntry, nEntryCount=0, IdxLast=-1, NewLen, nEntryThreshold, bUseTmpBuff=1;
	size_t nDirEntryListBuffSize;
	int *p_nDirEntries, *p_DirEntryOffset, nBytesDirEntryNameAccu=0, nBytesDirEntryNameAccuMax, nBytesEntryName, *p_nEntryNameOffset, *pIdxMin, *pIdxMax;
	char *p_szDirEntryName, *pResult_buf, *pResult_New_Allocated=NULL;
	struct elt_CharEntry *elt_list_DirEntry;
	RW_FUNC_RETURN_EXT *pResult_Ext;
	
	nEntryThreshold = (IO_RESULT_BUFFER_SIZE - sizeof(RW_FUNC_RETURN) - 3*sizeof(int))/(sizeof(int) + MAX_ENTRY_NAME_LEN);
	nEntry = pDirMetaData[dir_idx].nEntries;
	nEntryToSend = MIN(MAX_NUM_DIR_ENTRY_PER_REQUEST, MAX(0, nEntry - offset) );	// limit the number of entries

	nDirEntryListBuffSize = pDirMetaData[dir_idx].nLenAllEntries + sizeof(int)*(3 + nEntryToSend);
	nDirEntryListBuffSize = MIN(MIN(nDirEntryListBuffSize, MAX_SIZE_DIR_ENTRY_BUFF_PER_REQUEST), nBuffSize);	// limit the size of buffer
	nBytesDirEntryNameAccuMax = nDirEntryListBuffSize - sizeof(int)*(3 + nEntryToSend);

	if(nEntryToSend <= nEntryThreshold)	{	// Returning buffer is sufficient
		pResult_buf = (char*)loc_buf;
		bUseTmpBuff = 0;
	}
	else	{	// Otherwise always setup a buffer		
		pResult_New_Allocated = (char *)ncx_slab_alloc(sp_OpenDirEntryBuff, nDirEntryListBuffSize + sizeof(int));
		assert(pResult_New_Allocated != NULL);
		pResult_Ext = (RW_FUNC_RETURN_EXT *)((long int)loc_buf + sizeof(int) - sizeof(RW_FUNC_RETURN));
		pResult_Ext->nEntry = pDirMetaData[dir_idx].nEntries;
		pResult_buf = (char*)pResult_New_Allocated + sizeof(int);
	}
	
	p_nDirEntries = (int*)( pResult_buf );
	pIdxMin = (int*)( pResult_buf + sizeof(int));
	pIdxMax = (int*)( pResult_buf + sizeof(int)*2);
	p_DirEntryOffset = (int*)( pResult_buf + sizeof(int)*3 );
	p_szDirEntryName = (char*)pResult_buf + sizeof(int) * (3 + nEntryToSend);

	*p_nDirEntries = pDirMetaData[dir_idx].nEntries;
	if(offset < nEntry)	{
		*pIdxMin = offset & 0xFFFFFFFF;
	}
	else	{
		*pIdxMin = nEntry + 1;
	}
//	printf("DBG> %d entries under directory %s\n", nEntry, pDirMetaData[dir_idx].szDirName);
	nMaxEntry = pDirMetaData[dir_idx].nMaxEntry;
	elt_list_DirEntry = pDirMetaData[dir_idx].elt_list_DirEntry;
	nEntry_Save = nEntry;

	nEntry = 0;
	for(i=0; i<nMaxEntry; i++)	{
		if(elt_list_DirEntry[i].key[0])	{	// a valid entry. 
			nEntry++;
			if(nEntry > offset)	{
				break;
			}
		}
	}

	nEntry = 0;
	for(; i<nMaxEntry; i++)	{
		if(elt_list_DirEntry[i].key[0])	{	// a valid entry. 
			nBytesEntryName = strlen(elt_list_DirEntry[i].key);
			NewLen = nBytesDirEntryNameAccu + 2 + nBytesEntryName;
			if(NewLen > nBytesDirEntryNameAccuMax)	{	// buffer is full
				break;
			}
			else	{
				strcpy(p_szDirEntryName + nBytesDirEntryNameAccu + 1, elt_list_DirEntry[i].key);
				p_szDirEntryName[nBytesDirEntryNameAccu] = (char)(elt_list_DirEntry[i].value & 0xFF);
				p_DirEntryOffset[nEntry] = nBytesDirEntryNameAccu;
				nBytesDirEntryNameAccu += (nBytesEntryName + 2);
				nEntry++;
				if(nEntry >= nEntryToSend)	break;
			}
		}
	}
	*pIdxMax = offset + nEntry - 1;	// last entry in the list

	nDirEntryListBuffSize = sizeof(int)*(3 + nEntryToSend) + nBytesDirEntryNameAccu;	// the real buff size needed
	if(nDirEntryListBuffSize > (IO_RESULT_BUFFER_SIZE - sizeof(RW_FUNC_RETURN)) )	{	// too large to fit in result buffer (loc_buf)
		pResult_Ext->mr_tmp = Server_qp.IB_RegisterBuf_RW_Local_Remote((void*)pResult_New_Allocated, nDirEntryListBuffSize + sizeof(int));
		assert(pResult_Ext->mr_tmp != NULL);
	}
	else	{
		if(bUseTmpBuff)	memcpy(loc_buf, pResult_buf, nDirEntryListBuffSize);	// copy entry data from tmp buffer to result buffer
	}

	return nDirEntryListBuffSize;	// always larger than zero. 
}

/*
int my_opendir_by_index(int dir_idx, void *loc_buf, long int offset, long int nBuffSize)
{
	int i, nEntry, nEntryToSend, nEntry_Save, nMaxEntry, nEntryCount=0, IdxLast=-1, NewLen;
	size_t nDirEntryListBuffSize;
	int *p_nDirEntries, *p_DirEntryOffset, nBytesDirEntryNameAccu=0, nBytesDirEntryNameAccuMax, nBytesEntryName, *p_nEntryNameOffset, *pIdxMin, *pIdxMax;
	char *p_szDirEntryName, *pResult_buf, *pResult_New_Allocated=NULL;
	struct elt_CharEntry *elt_list_DirEntry;
	RW_FUNC_RETURN_EXT *pResult_Ext;
	
	nEntry = pDirMetaData[dir_idx].nEntries;
	nEntryToSend = MIN(MAX_NUM_DIR_ENTRY_PER_REQUEST, MAX(0, nEntry - offset) );	// limit the number of entries

	nDirEntryListBuffSize = pDirMetaData[dir_idx].nLenAllEntries + sizeof(int)*(3 + nEntryToSend);
	nDirEntryListBuffSize = MIN(MIN(nDirEntryListBuffSize, MAX_SIZE_DIR_ENTRY_BUFF_PER_REQUEST), nBuffSize);	// limit the size of buffer
	nBytesDirEntryNameAccuMax = nDirEntryListBuffSize - sizeof(int)*(3 + nEntryToSend);
	
	if(nDirEntryListBuffSize > (IO_RESULT_BUFFER_SIZE - sizeof(RW_FUNC_RETURN)) )	{	// too large to fit in result buffer (loc_buf)
		pResult_New_Allocated = (char *)ncx_slab_alloc(sp_OpenDirEntryBuff, nDirEntryListBuffSize + sizeof(int));
		assert(pResult_New_Allocated != NULL);
		pResult_Ext = (RW_FUNC_RETURN_EXT *)((long int)loc_buf + sizeof(int) - sizeof(RW_FUNC_RETURN));
		pResult_Ext->mr_tmp = Server_qp.IB_RegisterBuf_RW_Local_Remote((void*)pResult_New_Allocated, nDirEntryListBuffSize + sizeof(int));
		assert(pResult_Ext->mr_tmp != NULL);
		pResult_Ext->nEntry = pDirMetaData[dir_idx].nEntries;

		pResult_buf = (char*)pResult_New_Allocated + sizeof(int);
	}
	else	{
		pResult_buf = (char*)loc_buf;
	}

	p_nDirEntries = (int*)( pResult_buf );
	pIdxMin = (int*)( pResult_buf + sizeof(int));
	pIdxMax = (int*)( pResult_buf + sizeof(int)*2);
	p_DirEntryOffset = (int*)( pResult_buf + sizeof(int)*3 );
	p_szDirEntryName = (char*)pResult_buf + sizeof(int) * (3 + nEntry);

	*p_nDirEntries = pDirMetaData[dir_idx].nEntries;
	if(offset < nEntry)	{
		*pIdxMin = offset & 0xFFFFFFFF;
	}
	else	{
		*pIdxMin = nEntry + 1;
	}
//	printf("DBG> %d entries under directory %s\n", nEntry, pDirMetaData[dir_idx].szDirName);
	nMaxEntry = pDirMetaData[dir_idx].nMaxEntry;
	elt_list_DirEntry = pDirMetaData[dir_idx].elt_list_DirEntry;
	nEntry_Save = nEntry;

	nEntry = 0;
	for(i=0; i<nMaxEntry; i++)	{
		if(elt_list_DirEntry[i].key[0])	{	// a valid entry. 
			nEntry++;
			if(nEntry > offset)	{
				break;
			}
		}
	}

	nEntry = 0;
	for(; i<nMaxEntry; i++)	{
		if(elt_list_DirEntry[i].key[0])	{	// a valid entry. 
			nBytesEntryName = strlen(elt_list_DirEntry[i].key);
			NewLen = nBytesDirEntryNameAccu + 2 + nBytesEntryName;
			if(NewLen > nBytesDirEntryNameAccuMax)	{	// buffer is full
				break;
			}
			else	{
				strcpy(p_szDirEntryName + nBytesDirEntryNameAccu + 1, elt_list_DirEntry[i].key);
				p_szDirEntryName[nBytesDirEntryNameAccu] = (char)(elt_list_DirEntry[i].value & 0xFF);
				p_DirEntryOffset[nEntry] = nBytesDirEntryNameAccu;
				nBytesDirEntryNameAccu += (nBytesEntryName + 2);
				nEntry++;
				if(nEntry >= nEntryToSend)	break;
			}
		}
	}
	*pIdxMax = offset + nEntry - 1;	// last entry in the list

	return (sizeof(int)*(3 + nEntryToSend) + nBytesDirEntryNameAccu);	// always larger than zero. 
}
*/

/*
int my_opendir_by_index(int dir_idx, void *loc_buf)
{
	int i, nEntry, nEntry_Save, nMaxEntry;
	size_t nDirEntryListBuffSize;
//	unsigned char *szDirEntryBuff=NULL;// entry data buffer
	int *p_nDirEntries, *p_DirEntryOffset, nBytesDirEntryNameAccu=0, nBytesEntryName, *p_nEntryNameOffset;
	char *p_szDirEntryName, *pResult_buf, *pResult_New_Allocated=NULL;
	struct elt_CharEntry *elt_list_DirEntry;
	RW_FUNC_RETURN_EXT *pResult_Ext;
	
	nEntry = pDirMetaData[dir_idx].nEntries;
	nDirEntryListBuffSize = pDirMetaData[dir_idx].nLenAllEntries + sizeof(int)*(1 + nEntry);
	if(nDirEntryListBuffSize > (IO_RESULT_BUFFER_SIZE - sizeof(RW_FUNC_RETURN)) )	{	// too large to fit in result buffer (loc_buf)
		pResult_New_Allocated = (char *)ncx_slab_alloc(sp_OpenDirEntryBuff, nDirEntryListBuffSize + sizeof(int));
		assert(pResult_New_Allocated != NULL);
		pResult_Ext = (RW_FUNC_RETURN_EXT *)((long int)loc_buf + sizeof(int) - sizeof(RW_FUNC_RETURN));
		pResult_Ext->mr_tmp = Server_qp.IB_RegisterBuf_RW_Local_Remote((void*)pResult_New_Allocated, nDirEntryListBuffSize + sizeof(int));
		assert(pResult_Ext->mr_tmp != NULL);
		pResult_Ext->nEntry = pDirMetaData[dir_idx].nEntries;

		pResult_buf = (char*)pResult_New_Allocated + sizeof(int);
	}
	else	{
		pResult_buf = (char*)loc_buf;
	}

	p_szDirEntryName = (char*)pResult_buf + sizeof(int) * (1 + nEntry);
	p_DirEntryOffset = (int*)( pResult_buf + sizeof(int)*1 );
	p_nDirEntries = (int*)( pResult_buf );
	*p_nDirEntries = pDirMetaData[dir_idx].nEntries;
//	printf("DBG> %d entries under directory %s\n", nEntry, pDirMetaData[dir_idx].szDirName);
	nMaxEntry = pDirMetaData[dir_idx].nMaxEntry;
	elt_list_DirEntry = pDirMetaData[dir_idx].elt_list_DirEntry;
	nEntry_Save = nEntry;
	nEntry = 0;
	for(i=0; i<nMaxEntry; i++)	{
		if(elt_list_DirEntry[i].key[0])	{	// a valid entry. 
			nBytesEntryName = strlen(elt_list_DirEntry[i].key);
			strcpy(p_szDirEntryName + nBytesDirEntryNameAccu + 1, elt_list_DirEntry[i].key);
			p_szDirEntryName[nBytesDirEntryNameAccu] = (char)(elt_list_DirEntry[i].value & 0xFF);
//			printf("DBG> %d entry, %s\n", nEntry, p_szDirEntryName + nBytesDirEntryNameAccu + 1);
			p_DirEntryOffset[nEntry] = nBytesDirEntryNameAccu;
			nBytesDirEntryNameAccu += (nBytesEntryName + 2);
			nEntry++;
			if(nEntry >= nEntry_Save)	break;
		}
	}
	
	return (nDirEntryListBuffSize);	// always larger than zero. 
}
*/


int my_opendir(char szDirName[], void *loc_buf)
{
	int dir_idx;
	unsigned long long fn_hash=0;

	dir_idx = p_Hash_Dir->DictSearch(szDirName, &elt_list_dir, &ht_table_dir, &fn_hash);
	if(dir_idx < 0)	{
		errno = ENOENT;
		return (-1);	// fail
	}
	return my_opendir_by_index(dir_idx, loc_buf, 0, MAX_SIZE_DIR_ENTRY_BUFF_PER_REQUEST);
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
	size_t DataReturn[2];

//	printf("DBG> Before my_mkdir(). sp_DirEntryName\n");
//	ncx_slab_stat(sp_DirEntryName, &ncx_stat);
//	printf("DBG> Before my_mkdir(). sp_DirEntryNameOffset\n");
//	ncx_slab_stat(sp_DirEntryNameOffset, &ncx_stat);

	my_mkdir("/myfs/tmp", S_IRWXU | S_IRWXG | S_IRWXO, my_uid, my_gid);

//	printf("DBG> After my_mkdir(). sp_DirEntryName\n");
//	ncx_slab_stat(sp_DirEntryName, &ncx_stat);
//	printf("DBG> After my_mkdir(). sp_DirEntryNameOffset\n");
//	ncx_slab_stat(sp_DirEntryNameOffset, &ncx_stat);

	my_rmdir("/myfs/tmp");

//	my_mkdir("/myfs/tmp_0");

//	printf("DBG> After my_rmdir(). sp_DirEntryName\n");
//	ncx_slab_stat(sp_DirEntryName, &ncx_stat);
//	printf("DBG> After my_rmdir(). sp_DirEntryNameOffset\n");
//	ncx_slab_stat(sp_DirEntryNameOffset, &ncx_stat);


	Readin_All_Dir();
	Readin_All_File();

	fd = my_openfile(DataReturn, "/myfs/3/k.rnd", O_RDONLY);
	my_close(fd, NULL);

//	printf("DBG> After my_mkdir(). sp_DirEntryName\n");
//	ncx_slab_stat(sp_DirEntryName, &ncx_stat);
//	printf("DBG> After my_mkdir(). sp_DirEntryNameOffset\n");
//	ncx_slab_stat(sp_DirEntryNameOffset, &ncx_stat);

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
			nBytes = MIN(nBytes, nBytesTotal);
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
			nBytes = MIN(nBytes, nBytesTotal);
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
	size_t DataReturn[2];

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
			fd_out = my_openfile(DataReturn, szNameOut, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR);
			if(fd_out == -1)	{
				if( ( (file_stat.st_mode & S_IFMT) == S_IFDIR) )	{	// Existing dir. Expected behavior.
				}
				else	{
					printf("DBG>Fail to open file %s\nQuit\n", szNameOut);
					exit(1);
				}
			}
			else	{
//				nWriteBytes = my_write(fd_out, pBuff, nBytes_to_Read, 0);	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				
				if(nWriteBytes != nBytes_to_Read)	{
					printf("Error in writing file for %s. nWriteBytes (%d) != nBytes_to_Read (%d)\nQuit\n", szNameOut, nWriteBytes, nBytes_to_Read);
					exit(1);
				}
				my_close(fd_out, NULL);
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

void my_statfs(FS_STAT *pFS_Stat)
{
	pFS_Stat->fs_nblocks = _NPAGES;
	pFS_Stat->fs_bfreeblocks = pMem_Allocator->Get_Num_Free_Page();

	pFS_Stat->fs_ninode = MAX_NUM_FILE;
	pFS_Stat->fs_nfreeinode = MAX_NUM_FILE - nFile;

	pFS_Stat->f_namelen = DEFAULT_FULL_FILE_NAME_LEN;
}
