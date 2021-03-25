#include <sys/stat.h>
#include <pthread.h>

#include "io_ops.h"
#include "qp.h"
#include "dict.h"
#include "myfs.h"
#include "utility.h"
#include "ncx_slab.h"
#include "unique_thread.h"

extern int nFile, nDir;
extern ncx_slab_pool_t *sp_DirEntryName, *sp_DirEntryNameOffset;
extern pthread_attr_t thread_attr;
extern CCreatedUniqueThread Unique_Thread;

extern SERVER_QUEUEPAIR Server_qp;

extern CHASHTABLE_CHAR *p_Hash_File;
extern struct elt_Char *elt_list_file;
extern int *ht_table_file;

extern CHASHTABLE_CHAR *p_Hash_Dir;
extern struct elt_Char *elt_list_dir;
extern int *ht_table_dir;

extern META_INFO *pMetaData;
extern DIR_META_INFO *pDirMetaData;

extern ACTIVEFILE fd_List[MAX_FD_ACTIVE];

void RW_Open(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_shm_global;
	void *rem_buff;
	unsigned int rkey;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	idx_qp = pRF_Op_Msg->idx_qp;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_qp.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);
	pResult->ret_value = my_openfile(pRF_Op_Msg->szName, pRF_Op_Msg->flag);
	pResult->myerrno = errno;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pResult->Tag_End = (pResult->Tag_Ini) ^ tag_magic;

	Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, sizeof(RW_FUNC_RETURN));
}

void RW_Close(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_shm_global;
	void *rem_buff;
	unsigned int rkey;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	idx_qp = pRF_Op_Msg->idx_qp;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_qp.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);
	pResult->ret_value = my_close(pRF_Op_Msg->fd);
	pResult->myerrno = errno;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pResult->Tag_End = (pResult->Tag_Ini) ^ tag_magic;

	Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, sizeof(RW_FUNC_RETURN));
}

void RW_Stat(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic, file_idx, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_shm_global;
	void *rem_buff;
	unsigned int rkey;
	unsigned long long fn_hash;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	idx_qp = pRF_Op_Msg->idx_qp;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_qp.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN) + sizeof(struct stat);

	file_idx = p_Hash_File->DictSearch(pRF_Op_Msg->szName, &elt_list_file, &ht_table_file, &(pRF_Op_Msg->file_hash));
	if(file_idx < 0)	{
		pResult->myerrno = ENOENT;
		pResult->ret_value = -1;
	}
	else	{
		memcpy((char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int), &(pMetaData[file_idx].st_dev), sizeof(struct stat));
		pResult->ret_value = 0;
	}
	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
}

void RW_Opendir(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_shm_global;
	void *rem_buff;
	unsigned int rkey;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	idx_qp = pRF_Op_Msg->idx_qp;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_qp.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->ret_value = my_opendir(pRF_Op_Msg->szName, (void*)((char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int)));
	pResult->myerrno = errno;
	pResult->nDataSize = sizeof(RW_FUNC_RETURN) + pResult->ret_value;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
}

void RW_Read(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_shm_global;
	void *rem_buff;
	unsigned int rkey;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	idx_qp = pRF_Op_Msg->idx_qp;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_qp.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

//	Determine_Index_StorageBlock_for_Offset(pRF_Op_Msg->fd, pRF_Op_Msg->offset);

	if(pRF_Op_Msg->nLen <= DATA_COPY_THRESHOLD_SIZE)	{
		if(pRF_Op_Msg->offset >= pMetaData[fd_List[pRF_Op_Msg->fd].idx_file].st_size)	{	// out of range
			pResult->ret_value = 0;
		}
		else	{
			pResult->ret_value = my_read(pRF_Op_Msg->fd, (char*)pResult+sizeof(RW_FUNC_RETURN)-sizeof(int), pRF_Op_Msg->nLen, pRF_Op_Msg->offset);
			pResult->myerrno = errno;
		}
		pResult->nDataSize = (pResult->ret_value>0) ? (sizeof(RW_FUNC_RETURN) + pResult->ret_value) : (sizeof(RW_FUNC_RETURN));

		pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
		pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
		*pTag_End = (pResult->Tag_Ini) ^ tag_magic;
		Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
	}
	else	{
		if(pRF_Op_Msg->offset >= pMetaData[fd_List[pRF_Op_Msg->fd].idx_file].st_size)   {       // out of range
			pResult->ret_value = 0;
		}
		else    {
			pResult->ret_value = my_read_RDMA(pRF_Op_Msg->fd, idx_qp, (void*)((char*)pResult+sizeof(RW_FUNC_RETURN)), mr_shm_global->lkey, rem_buff, rkey, pRF_Op_Msg->nLen, pRF_Op_Msg->offset);
			pResult->myerrno = errno;
		}
		
		rem_buff = (void*)(Server_qp.pQP_Data[idx_qp].rem_addr);
		rkey = Server_qp.pQP_Data[idx_qp].rem_key;
		pResult->nDataSize = sizeof(RW_FUNC_RETURN);	// different!!!

		pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
		pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
		*pTag_End = (pResult->Tag_Ini) ^ tag_magic;
		Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].rem_addr), Server_qp.pQP_Data[idx_qp].rem_key, pResult->nDataSize);
	}
}


void RW_Write(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_shm_global;
	void *rem_buff;
	unsigned int rkey;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	idx_qp = pRF_Op_Msg->idx_qp;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_qp.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

//	Determine_Index_StorageBlock_for_Offset(pRF_Op_Msg->fd, pRF_Op_Msg->offset);

	if(pRF_Op_Msg->nLen <= DATA_COPY_THRESHOLD_SIZE)	{
		Server_qp.IB_Get(idx_qp, (void*)((char*)pResult+sizeof(RW_FUNC_RETURN)), mr_shm_global->lkey, (void*)((char*)rem_buff+sizeof(RW_FUNC_RETURN)), rkey, pRF_Op_Msg->nLen);
		pResult->ret_value = my_write(pRF_Op_Msg->fd, (char*)pResult+sizeof(RW_FUNC_RETURN), pRF_Op_Msg->nLen, pRF_Op_Msg->offset);
	}
	else	{
		pResult->ret_value = my_write_RDMA(pRF_Op_Msg->fd, idx_qp, (void*)((char*)pResult+sizeof(RW_FUNC_RETURN)), mr_shm_global->lkey, rem_buff, rkey, pRF_Op_Msg->nLen, pRF_Op_Msg->offset);
	}
	pResult->nDataSize = (sizeof(RW_FUNC_RETURN));
	pResult->myerrno = errno;
	
	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;
	
	Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].rem_addr), Server_qp.pQP_Data[idx_qp].rem_key, pResult->nDataSize);
}

void RW_PRead(IO_CMD_MSG *pRF_Op_Msg)
{
}

void RW_Seek(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	void *rem_buff;
	unsigned int rkey;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	idx_qp = pRF_Op_Msg->idx_qp;
	tag_magic = pRF_Op_Msg->tag_magic;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);
	pResult->ret_value = my_lseek(pRF_Op_Msg->fd, pRF_Op_Msg->offset, pRF_Op_Msg->flag);
	pResult->myerrno = errno;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pResult->Tag_End = (pResult->Tag_Ini) ^ tag_magic;

	Server_qp.IB_Put(idx_qp, (void*)pResult, Server_qp.mr_shm_global->lkey, rem_buff, rkey, sizeof(RW_FUNC_RETURN));
}

void RW_LStat(IO_CMD_MSG *pRF_Op_Msg)
{
}

void RW_FStat(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic, file_idx, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_shm_global;
	void *rem_buff;
	unsigned int rkey;
	unsigned long long fn_hash;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	idx_qp = pRF_Op_Msg->idx_qp;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_qp.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN) + sizeof(struct stat);

	file_idx = fd_List[pRF_Op_Msg->fd].idx_file ;
	memcpy((char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int), &(pMetaData[file_idx].st_dev), sizeof(struct stat));
	pResult->ret_value = 0;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
}

void RW_Dir_Exist(IO_CMD_MSG *pRF_Op_Msg)
{
}

void RW_Unlink(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_shm_global;
	void *rem_buff;
	unsigned int rkey;
	unsigned long long fn_hash;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	idx_qp = pRF_Op_Msg->idx_qp;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_qp.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);

	pResult->ret_value = my_unlink(pRF_Op_Msg->szName);
	if(pResult->ret_value < 0)	pResult->myerrno = errno;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
}

void RW_Remove_Dir(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_shm_global;
	void *rem_buff;
	unsigned int rkey;
	unsigned long long fn_hash;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	idx_qp = pRF_Op_Msg->idx_qp;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_qp.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);

	pResult->ret_value = my_rmdir(pRF_Op_Msg->szName);
	if(pResult->ret_value < 0)	pResult->myerrno = errno;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
}

void RW_Truncate(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic, *pTag_End, file_idx;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_shm_global;
	void *rem_buff;
	unsigned int rkey;
	unsigned long long fn_hash;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	idx_qp = pRF_Op_Msg->idx_qp;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_qp.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);

	file_idx = p_Hash_File->DictSearch(pRF_Op_Msg->szName, &elt_list_file, &ht_table_file, &fn_hash);
	if(file_idx < 0)	{
		pResult->myerrno = ENOENT;
		pResult->ret_value = -1;
	}
	else	{
		pResult->ret_value = Truncate_File(file_idx, pRF_Op_Msg->nLen);
		if(pResult->ret_value < 0)	pResult->myerrno = errno;
	}

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
}

void RW_Ftruncate(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_shm_global;
	void *rem_buff;
	unsigned int rkey;
	unsigned long long fn_hash;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	idx_qp = pRF_Op_Msg->idx_qp;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_qp.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);

	pResult->ret_value = Truncate_File(fd_List[pRF_Op_Msg->fd].idx_file, pRF_Op_Msg->nLen);
	if(pResult->ret_value < 0)	pResult->myerrno = errno;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
}

void RW_Utimes(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic, *pTag_End, file_idx;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_shm_global;
	void *rem_buff;
	unsigned int rkey;
	unsigned long long fn_hash;
	struct timespec *pTimes;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	idx_qp = pRF_Op_Msg->idx_qp;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_qp.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);

	file_idx = p_Hash_File->DictSearch(pRF_Op_Msg->szName, &elt_list_file, &ht_table_file, &fn_hash);
	if(file_idx < 0)	{
		pResult->myerrno = ENOENT;
		pResult->ret_value = -1;
	}
	else	{
		pTimes = (struct timespec *)(&(pRF_Op_Msg->offset));

		pMetaData[file_idx].st_atim.tv_sec = pTimes[0].tv_sec;	// update time stamp
		pMetaData[file_idx].st_atim.tv_nsec = pTimes[0].tv_nsec;

		pMetaData[file_idx].st_mtim.tv_sec = pTimes[1].tv_sec;
		pMetaData[file_idx].st_mtim.tv_nsec = pTimes[1].tv_nsec;

		pResult->ret_value = 0;
	}

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
}

void RW_Futimens(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic, *pTag_End, file_idx;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_shm_global;
	void *rem_buff;
	unsigned int rkey;
	unsigned long long fn_hash;
	struct timespec *pTimes;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	idx_qp = pRF_Op_Msg->idx_qp;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_qp.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);

	file_idx = fd_List[pRF_Op_Msg->fd].idx_file;
	if(file_idx < 0)	{
		pResult->myerrno = ENOENT;
		pResult->ret_value = -1;
	}
	else	{
		pTimes = (struct timespec *)(&(pRF_Op_Msg->offset));

		pMetaData[file_idx].st_atim.tv_sec = pTimes[0].tv_sec;	// update time stamp
		pMetaData[file_idx].st_atim.tv_nsec = pTimes[0].tv_nsec;

		pMetaData[file_idx].st_mtim.tv_sec = pTimes[1].tv_sec;
		pMetaData[file_idx].st_mtim.tv_nsec = pTimes[1].tv_nsec;

		pResult->ret_value = 0;
	}

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
}

void RW_Mkdir(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic, file_idx, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_shm_global;
	void *rem_buff;
	unsigned int rkey;
	unsigned long long fn_hash;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	idx_qp = pRF_Op_Msg->idx_qp;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_qp.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);
	pResult->ret_value = my_mkdir(pRF_Op_Msg->szName, pRF_Op_Msg->mode, Server_qp.pQP_Data[idx_qp].cuid, Server_qp.pQP_Data[idx_qp].cgid);	// mode is NOT used yet!!!
	if(pResult->ret_value < 0)	pResult->myerrno = errno;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
}

//void sigusr1_handler(int signum)
void RW_Print_Mem(void)
{
	ncx_slab_stat_t ncx_stat;
	ncx_slab_stat(sp_DirEntryName, &ncx_stat);
	ncx_slab_stat(sp_DirEntryNameOffset, &ncx_stat);
	printf("DBG> nFile = %d nDir = %d\n", nFile, nDir);
}

void* Func_thread_Disconnect_QP(void *pParam)	// close a QP
{
	long int Parameter;
	int idx_qp, nToken, *pParamInt;

	Parameter = (long int)pParam;
	pParamInt = (int*)(&Parameter);
	idx_qp = pParamInt[0];
	nToken = pParamInt[1];

	if(Unique_Thread.Redeem_A_Token(nToken))	{
//		printf("DBG> Going to destroy %d QP.\n", idx_qp);
		Server_qp.Destroy_A_QueuePair(idx_qp);
	}

	return NULL;
}

void RW_Disconnect_QP(IO_CMD_MSG *pRF_Op_Msg)
{
	pthread_t pthread_Disconnect_QP;
	long int Param;
	int *pParamInt;

	pParamInt = (int*)(&Param);
	pParamInt[0] = pRF_Op_Msg->idx_qp;
	pParamInt[1] = Unique_Thread.Apply_A_Token();

	// Just destroy QP. No need to return any result. 
	if( pthread_create(&pthread_Disconnect_QP, &thread_attr, Func_thread_Disconnect_QP, (void*)Param) ) {
		fprintf(stderr, "Error creating thread in RW_Disconnect_QP().\n");
		return;
	}
}


