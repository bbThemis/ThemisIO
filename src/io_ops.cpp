#include <sys/stat.h>
#include <pthread.h>


#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <uct/api/uct.h>
// extern "C" {
// #include <ucp/core/ucp_mm.h> 
// }
#include "io_ops.h"
// #include "qp.h"
#include "ucx_rma.h"
#include "dict.h"
#include "myfs.h"
#include "utility.h"
#include "ncx_slab.h"
#include "unique_thread.h"

extern int mpi_rank, nFSServer;
extern int nFile, nDir;
// extern long int nSizeReg;
extern long int nSizeUCXReg;
//extern ncx_slab_pool_t *sp_DirEntryName, *sp_DirEntryNameOffset;
extern pthread_attr_t thread_attr;
extern CCreatedUniqueThread Unique_Thread;

// extern SERVER_QUEUEPAIR Server_qp;
extern SERVER_RDMA Server_ucx;

extern CHASHTABLE_CHAR *p_Hash_File;
extern struct elt_Char *elt_list_file;
extern int *ht_table_file;

extern CHASHTABLE_CHAR *p_Hash_Dir;
extern struct elt_Char *elt_list_dir;
extern int *ht_table_dir;

extern META_INFO *pMetaData;
extern DIR_META_INFO *pDirMetaData;

extern ACTIVEFILE fd_List[MAX_FD_ACTIVE];

extern int IO_Msg_Size_op;

extern ncx_slab_pool_t *sp_OpenDirEntryBuff;


void RW_Open(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	size_t *pInode_and_FileSize;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
	pResult->nDataSize = sizeof(RW_FUNC_RETURN) + 2*sizeof(size_t);	// add inode info and file size info!!!
	pInode_and_FileSize = (size_t *)( (char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int) );

	pResult->ret_value = my_openfile(pInode_and_FileSize, pRF_Op_Msg->szName, pRF_Op_Msg->flag);
	pResult->myerrno = errno;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
//	pResult->Tag_End = (pResult->Tag_Ini) ^ tag_magic;
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;
	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	ucp_rkey_destroy(rkey);
}

void RW_Close(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	META_DATA_ON_CLOSE *pMetaData_OnClose;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
	pResult->nDataSize = sizeof(RW_FUNC_RETURN);
	pMetaData_OnClose = (META_DATA_ON_CLOSE *)(pRF_Op_Msg->szName);
	pResult->ret_value = my_close(pRF_Op_Msg->fd, pMetaData_OnClose);
	pResult->myerrno = errno;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pResult->Tag_End = (pResult->Tag_Ini) ^ tag_magic;

	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, sizeof(RW_FUNC_RETURN));
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, sizeof(RW_FUNC_RETURN));
	ucp_rkey_destroy(rkey);
}

void RW_Stat(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, file_idx, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	unsigned long long fn_hash;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN) + sizeof(struct stat);
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
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

	// Server_qp.IB_Put(idx_ucx, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	ucp_rkey_destroy(rkey);
}

void RW_Opendir(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, *pTag_End, *pDoneCopy;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	RW_FUNC_RETURN_EXT *pResult_Ext;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->ret_value = my_opendir(pRF_Op_Msg->szName, (void*)((char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int)));
	pResult->myerrno = errno;
	pResult->nDataSize = (pResult->ret_value >= 0) ? (sizeof(RW_FUNC_RETURN)+pResult->ret_value) : (sizeof(RW_FUNC_RETURN));
	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);

	if(pResult->nDataSize <= IO_RESULT_BUFFER_SIZE)	{
		pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
		*pTag_End = (pResult->Tag_Ini) ^ tag_magic;
		// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
		Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	}
	else	{
		pResult_Ext = (RW_FUNC_RETURN_EXT *)pResult;
		// pDoneCopy = (int *)(ucp_memh_address(pResult_Ext->mr_tmp));
		pDoneCopy = (int *)(pResult_Ext->mr_tmp_addr);
		*pDoneCopy = 0;

		// pResult_Ext->addr = (long int)(ucp_memh_address(pResult_Ext->mr_tmp));
		pResult_Ext->addr = (long int)pResult_Ext->mr_tmp_addr;
		// pResult_Ext->rkey = (long int)(pResult_Ext->mr_tmp->rkey);
		Server_ucx.UCX_Pack_Rkey(pResult_Ext->mr_tmp, ((void*)pResult_Ext->rkey_buffer));
		pTag_End = (int*)( (char*)pResult + sizeof(RW_FUNC_RETURN_EXT) - sizeof(int) );
		*pTag_End = (pResult->Tag_Ini) ^ tag_magic;
		// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, sizeof(RW_FUNC_RETURN_EXT));
		Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, sizeof(RW_FUNC_RETURN_EXT));
		while( (*pDoneCopy) == 0 )	{	// wait until client finishes data transfer
		}
		// free buffer
		// nSizeReg -= pResult_Ext->mr_tmp->length;
		nSizeUCXReg -= pResult_Ext->mr_tmp_length;
		// ibv_dereg_mr(pResult_Ext->mr_tmp);
		ucp_mem_unmap(Server_ucx.ucp_main_context, pResult_Ext->mr_tmp);
		ncx_slab_free(sp_OpenDirEntryBuff, (void*)(pResult_Ext->addr));
	}
	ucp_rkey_destroy(rkey);
}

void RW_Read_Dir_Entries(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, *pTag_End, *pDoneCopy, dir_idx;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	RW_FUNC_RETURN_EXT *pResult_Ext;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	unsigned long long fn_hash=0;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
	dir_idx = p_Hash_Dir->DictSearch(pRF_Op_Msg->szName, &elt_list_dir, &ht_table_dir, &fn_hash);
	if(dir_idx < 0)	{
		pResult->myerrno = ENOENT;
		pResult->ret_value = -1;	// fail
	}
	else	{
		pResult->myerrno = errno;
		pResult->ret_value = my_opendir_by_index(dir_idx, (void*)((char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int)), pRF_Op_Msg->offset, pRF_Op_Msg->nLen);
	}
	pResult->nDataSize = (pResult->ret_value >= 0) ? (sizeof(RW_FUNC_RETURN)+pResult->ret_value) : (sizeof(RW_FUNC_RETURN));
	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);

	if(pResult->nDataSize <= IO_RESULT_BUFFER_SIZE)	{
		pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
		*pTag_End = (pResult->Tag_Ini) ^ tag_magic;
		// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
		Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	}
	else	{
		// pResult_Ext = (RW_FUNC_RETURN_EXT *)pResult;
		// pDoneCopy = (int *)(pResult_Ext->mr_tmp->addr);
		// *pDoneCopy = 0;
		pResult_Ext = (RW_FUNC_RETURN_EXT *)pResult;
		pDoneCopy = (int *)(pResult_Ext->mr_tmp_addr);
		*pDoneCopy = 0;
		// pResult_Ext->addr = (long int)(pResult_Ext->mr_tmp->addr);
		// pResult_Ext->rkey = (long int)(pResult_Ext->mr_tmp->rkey);
		pResult_Ext->addr = (long int)(pResult_Ext->mr_tmp_addr);
		Server_ucx.UCX_Pack_Rkey(pResult_Ext->mr_tmp, ((void*)pResult_Ext->rkey_buffer));
		pTag_End = (int*)( (char*)pResult + sizeof(RW_FUNC_RETURN_EXT) - sizeof(int) );
		*pTag_End = (pResult->Tag_Ini) ^ tag_magic;
		// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, sizeof(RW_FUNC_RETURN_EXT));
		Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, sizeof(RW_FUNC_RETURN_EXT));
		while( (*pDoneCopy) == 0 )	{	// wait until client finishes data transfer
		}
		// free buffer
		// nSizeReg -= pResult_Ext->mr_tmp->length;
		// ibv_dereg_mr(pResult_Ext->mr_tmp);
		// ncx_slab_free(sp_OpenDirEntryBuff, (void*)(pResult_Ext->addr));
		nSizeUCXReg -= pResult_Ext->mr_tmp_length;
		ucp_mem_unmap(Server_ucx.ucp_main_context, pResult_Ext->mr_tmp);
		ncx_slab_free(sp_OpenDirEntryBuff, (void*)(pResult_Ext->addr));
	}
	ucp_rkey_destroy(rkey);
}

void RW_Read(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
//	Determine_Index_StorageBlock_for_Offset(pRF_Op_Msg->fd, pRF_Op_Msg->offset);

	if(pRF_Op_Msg->nLen <= DATA_COPY_THRESHOLD_SIZE)	{
//		if(pRF_Op_Msg->offset >= pMetaData[fd_List[pRF_Op_Msg->fd].idx_file].st_size)	{	// out of range
//			pResult->ret_value = 0;
//		}
//		else	{
//			pResult->ret_value = my_read(pRF_Op_Msg->fd, (char*)pResult+sizeof(RW_FUNC_RETURN)-sizeof(int), pRF_Op_Msg->nLen, pRF_Op_Msg->offset);
			pResult->ret_value = my_read_stripe(pRF_Op_Msg->fd, pRF_Op_Msg->szName, pRF_Op_Msg->mode, (char*)pResult+sizeof(RW_FUNC_RETURN)-sizeof(int), pRF_Op_Msg->nLen, pRF_Op_Msg->offset);
			pResult->myerrno = errno;
//		}
		pResult->nDataSize = (pResult->ret_value>0) ? (sizeof(RW_FUNC_RETURN) + pResult->ret_value) : (sizeof(RW_FUNC_RETURN));

		pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
		pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
		*pTag_End = (pResult->Tag_Ini) ^ tag_magic;
		// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
		Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	}
	else	{
//		if(pRF_Op_Msg->offset >= pMetaData[fd_List[pRF_Op_Msg->fd].idx_file].st_size)   {       // out of range
//			pResult->ret_value = 0;
//		}
//		else    {
//			pResult->ret_value = my_read_RDMA(pRF_Op_Msg->fd, idx_qp, (void*)((char*)pResult+sizeof(RW_FUNC_RETURN)), mr_shm_global->lkey, rem_buff, rkey, pRF_Op_Msg->nLen, pRF_Op_Msg->offset);
//			pResult->ret_value = my_read_stripe(pRF_Op_Msg->fd, pRF_Op_Msg->szName, (char*)pResult+sizeof(RW_FUNC_RETURN)-sizeof(int), pRF_Op_Msg->nLen, pRF_Op_Msg->offset);
			pResult->ret_value = my_read_stripe_RDMA(pRF_Op_Msg->fd, pRF_Op_Msg->szName, pRF_Op_Msg->mode, idx_ucx, (void*)((char*)pResult+sizeof(RW_FUNC_RETURN)), rem_buff, rkey, pRF_Op_Msg->nLen, pRF_Op_Msg->offset);
			pResult->myerrno = errno;
//		}
		
		// rem_buff = (void*)(Server_qp.pQP_Data[idx_qp].rem_addr);
		// rkey = Server_qp.pQP_Data[idx_qp].rem_key;
		rem_buff = (void*)(Server_ucx.pUCX_Data[idx_ucx].rem_addr);
		rkey = Server_ucx.pUCX_Data[idx_ucx].rkey;
		pResult->nDataSize = sizeof(RW_FUNC_RETURN);	// different!!!

		pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
		pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
		*pTag_End = (pResult->Tag_Ini) ^ tag_magic;
		// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].rem_addr), Server_qp.pQP_Data[idx_qp].rem_key, pResult->nDataSize);
		Server_ucx.UCX_Put(idx_ucx, (void*)pResult, (void*)(Server_ucx.pUCX_Data[idx_ucx].rem_addr), Server_ucx.pUCX_Data[idx_ucx].rkey, pResult->nDataSize);
	}
	ucp_rkey_destroy(rkey);
}


void RW_Write(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = (void*)pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

//	Determine_Index_StorageBlock_for_Offset(pRF_Op_Msg->fd, pRF_Op_Msg->offset);
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
	if(pRF_Op_Msg->nLen <= DATA_COPY_THRESHOLD_SIZE)	{
		// Server_qp.IB_Get(idx_qp, (void*)((char*)pResult+sizeof(RW_FUNC_RETURN)), mr_shm_global->lkey, (void*)((char*)rem_buff+sizeof(RW_FUNC_RETURN)), rkey, pRF_Op_Msg->nLen);
		Server_ucx.UCX_Get(idx_ucx, (void*)((char*)pResult+sizeof(RW_FUNC_RETURN)), (void*)((char*)rem_buff+sizeof(RW_FUNC_RETURN)), rkey, pRF_Op_Msg->nLen);
		// pResult->ret_value = my_write_stripe(pRF_Op_Msg->fd, pRF_Op_Msg->szName, pRF_Op_Msg->mode, (const void *)((char*)pResult+sizeof(RW_FUNC_RETURN)), pRF_Op_Msg->nLen, pRF_Op_Msg->offset);
		pResult->ret_value = my_write_stripe(pRF_Op_Msg->fd, pRF_Op_Msg->szName, pRF_Op_Msg->mode, (const void *)((char*)pResult+sizeof(RW_FUNC_RETURN)), pRF_Op_Msg->nLen, pRF_Op_Msg->offset);
//		pResult->ret_value = my_write(pRF_Op_Msg->fd, (char*)pResult+sizeof(RW_FUNC_RETURN), pRF_Op_Msg->nLen, pRF_Op_Msg->offset);
	}
	else	{
		// pResult->ret_value = my_write_stripe_RDMA(pRF_Op_Msg->fd, pRF_Op_Msg->szName, pRF_Op_Msg->mode, idx_ucx, (void*)((char*)pResult+sizeof(RW_FUNC_RETURN)), mr_shm_global->lkey, rem_buff, rkey, pRF_Op_Msg->nLen, pRF_Op_Msg->offset);
		pResult->ret_value = my_write_stripe_RDMA(pRF_Op_Msg->fd, pRF_Op_Msg->szName, pRF_Op_Msg->mode, idx_ucx, (void*)((char*)pResult+sizeof(RW_FUNC_RETURN)), rem_buff, rkey, pRF_Op_Msg->nLen, pRF_Op_Msg->offset);
	}
	pResult->nDataSize = (sizeof(RW_FUNC_RETURN));
	pResult->myerrno = errno;
	
	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;
	
	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, (void*)(Server_qp.pQP_Data[idx_qp].rem_addr), Server_qp.pQP_Data[idx_qp].rem_key, pResult->nDataSize);
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, (void*)(Server_ucx.pUCX_Data[idx_ucx].rem_addr), Server_ucx.pUCX_Data[idx_ucx].rkey, pResult->nDataSize);
	ucp_rkey_destroy(rkey);
}

//void RW_PRead(IO_CMD_MSG *pRF_Op_Msg)
//{
//}

/*
void RW_Seek(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	void *rem_buff;
	unsigned int rkey;

	rkey = pRF_Op_Msg->rkey;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_qp = pRF_Op_Msg->idx_qp;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);
	pResult->ret_value = my_lseek(pRF_Op_Msg->fd, pRF_Op_Msg->offset, pRF_Op_Msg->flag);
	pResult->myerrno = errno;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pResult->Tag_End = (pResult->Tag_Ini) ^ tag_magic;

	Server_qp.IB_Put(idx_qp, (void*)pResult, Server_qp.mr_shm_global->lkey, rem_buff, rkey, sizeof(RW_FUNC_RETURN));
}
*/

void RW_LStat(IO_CMD_MSG *pRF_Op_Msg)
{
}

void RW_FStat(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, file_idx, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	unsigned long long fn_hash;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = (void*)pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN) + sizeof(struct stat);
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
	file_idx = fd_List[pRF_Op_Msg->fd].idx_file ;
	memcpy((char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int), &(pMetaData[file_idx].st_dev), sizeof(struct stat));
	pResult->ret_value = 0;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	ucp_rkey_destroy(rkey);
}

void RW_Dir_Exist(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, *pTag_End, dir_idx;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	unsigned long long fn_hash;
	PARENTDIR_FUNC_RETURN *pReturnResult_Dir_Exist;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = (void*)pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);
	pReturnResult_Dir_Exist = (PARENTDIR_FUNC_RETURN *)((char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int));

	pResult->nDataSize = sizeof(RW_FUNC_RETURN) + sizeof(PARENTDIR_FUNC_RETURN);
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
	dir_idx = p_Hash_Dir->DictSearch(pRF_Op_Msg->szName, &elt_list_dir, &ht_table_dir, &(pRF_Op_Msg->file_hash));
	if(dir_idx < 0)	{
//		pResult->myerrno = ENOENT;
		pResult->ret_value = 0;
	}
	else	{
		pReturnResult_Dir_Exist->idx_Parent_Dir = dir_idx;
		pResult->ret_value = 1;	// existing!
	}

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	ucp_rkey_destroy(rkey);
}

void RW_Unlink(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, *pTag_End, idx_Server_Shift, nExtStripeServers, IdxServer;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	unsigned long long fn_hash;
	size_t nFileSize, Offset;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = (void*)pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);
	
	pResult->ret_value = my_unlink(pRF_Op_Msg->szName, &nFileSize);

	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
	if(pResult->ret_value < 0)	pResult->myerrno = errno;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	if(nFileSize > FILE_STRIPE_SIZE)	{	// more than one strip. Send requests to other following servers
		if(nFSServer > 1)	{
			nExtStripeServers = MIN( (nFileSize + FILE_STRIPE_SIZE -1) / FILE_STRIPE_SIZE, nFSServer - 1);
			idx_Server_Shift = 1;
			while(idx_Server_Shift <= nExtStripeServers)	{
				Request_Free_Stripe_Data( (mpi_rank + idx_Server_Shift)%nFSServer, pRF_Op_Msg->szName);
				idx_Server_Shift++;
			}
		}
	}

	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	// Let the client keep going. Server needs to send requests to other server to release stripe storage if necessary. 
	ucp_rkey_destroy(rkey);
}

void RW_Free_Stripe_Data(IO_CMD_MSG *pRF_Op_Msg)
{
	my_Free_Stripe_Data_Ext_Server(pRF_Op_Msg->szName);	// Just need to release storage. No need to return result/data. 
}

void RW_Remove_Dir(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	unsigned long long fn_hash;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = (void*)pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);

	pResult->ret_value = my_rmdir(pRF_Op_Msg->szName);
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);

	if(pResult->ret_value < 0)	pResult->myerrno = errno;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	ucp_rkey_destroy(rkey);
}

void RW_Truncate(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, *pTag_End, file_idx;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	unsigned long long fn_hash;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = (void*)pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
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

	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	ucp_rkey_destroy(rkey);
}

void RW_Ftruncate(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	unsigned long long fn_hash;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = (void*)pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);

	pResult->ret_value = Truncate_File(fd_List[pRF_Op_Msg->fd].idx_file, pRF_Op_Msg->nLen);

	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
	if(pResult->ret_value < 0)	pResult->myerrno = errno;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	ucp_rkey_destroy(rkey);
}

void RW_Utimes(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, *pTag_End, file_idx;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	unsigned long long fn_hash;
	struct timespec *pTimes;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = (void*)pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
	file_idx = p_Hash_File->DictSearch(pRF_Op_Msg->szName, &elt_list_file, &ht_table_file, &fn_hash);
	if(file_idx < 0)	{
		pResult->myerrno = ENOENT;
		pResult->ret_value = -1;
	}
	else	{
		pTimes = (struct timespec *)(&(pRF_Op_Msg->nLen_FileName));

		pMetaData[file_idx].st_atim.tv_sec = pTimes[0].tv_sec;	// update time stamp
		pMetaData[file_idx].st_atim.tv_nsec = pTimes[0].tv_nsec;

		pMetaData[file_idx].st_mtim.tv_sec = pTimes[1].tv_sec;
		pMetaData[file_idx].st_mtim.tv_nsec = pTimes[1].tv_nsec;

		pResult->ret_value = 0;
	}

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	ucp_rkey_destroy(rkey);
}

void RW_Futimens(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, *pTag_End, file_idx;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	unsigned long long fn_hash;
	struct timespec *pTimes;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = (void*)pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
	file_idx = fd_List[pRF_Op_Msg->fd].idx_file;
	if(file_idx < 0)	{
		pResult->myerrno = ENOENT;
		pResult->ret_value = -1;
	}
	else	{
		pTimes = (struct timespec *)(&(pRF_Op_Msg->nLen_FileName));

		pMetaData[file_idx].st_atim.tv_sec = pTimes[0].tv_sec;	// update time stamp
		pMetaData[file_idx].st_atim.tv_nsec = pTimes[0].tv_nsec;

		pMetaData[file_idx].st_mtim.tv_sec = pTimes[1].tv_sec;
		pMetaData[file_idx].st_mtim.tv_nsec = pTimes[1].tv_nsec;

		pResult->ret_value = 0;
	}

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	ucp_rkey_destroy(rkey);
}

void RW_Mkdir(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, file_idx, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	unsigned long long fn_hash;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = (void*)pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);
	pResult->ret_value = my_mkdir(pRF_Op_Msg->szName, pRF_Op_Msg->mode, Server_ucx.pUCX_Data[idx_ucx].cuid, Server_ucx.pUCX_Data[idx_ucx].cgid);	// mode is NOT used yet!!!
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
	if(pResult->ret_value < 0)	pResult->myerrno = errno;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	ucp_rkey_destroy(rkey);
}

//void sigusr1_handler(int signum)
void RW_Print_Mem(void)
{
	ncx_slab_stat_t ncx_stat;
//	ncx_slab_stat(sp_DirEntryName, &ncx_stat);
//	ncx_slab_stat(sp_DirEntryNameOffset, &ncx_stat);
	printf("DBG> nFile = %d nDir = %d\n", nFile, nDir);
}

void* Func_thread_Disconnect_QP(void *pParam)	// close a QP
{
	long int Parameter;
	int idx_ucx, nToken, *pParamInt;

	Parameter = (long int)pParam;
	pParamInt = (int*)(&Parameter);
	idx_ucx = pParamInt[0];
	nToken = pParamInt[1];

	if(Unique_Thread.Redeem_A_Token(nToken))	{
//		printf("DBG> Going to destroy %d QP.\n", idx_qp);
		// Server_qp.Destroy_A_QueuePair(idx_qp);
		Server_ucx.Destroy_A_UCPWorker(idx_ucx);
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

void RW_Hello(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;

	idx_ucx = pRF_Op_Msg->idx_qp;
	if(idx_ucx < nFSServer)	{	// from other server!
		printf("INFO> On server %d RW_Hello(): Hello from server %d.\n", mpi_rank, idx_ucx);
	}
	else	{	// from normal client
		printf("INFO> On server %d RW_Hello(): Hello from client. idx_ucx = %d.\n", mpi_rank, idx_ucx);
	}

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = (void*)pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN);
	pResult->ret_value = 123456 + mpi_rank;
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pResult->Tag_End = (pResult->Tag_Ini) ^ tag_magic;

	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, sizeof(RW_FUNC_RETURN));
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, sizeof(RW_FUNC_RETURN));
	ucp_rkey_destroy(rkey);
}

void RW_StatFS(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, *pTag_End;
	char *szBuff;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	FS_STAT *pFS_Stat;

	idx_ucx = pRF_Op_Msg->idx_qp;
	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = (void*)pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);

	pResult->nDataSize = sizeof(RW_FUNC_RETURN) + sizeof(FS_STAT);
	pResult->ret_value = 0;
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	pFS_Stat = (FS_STAT *)( (char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int) );
	my_statfs(pFS_Stat);

	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	ucp_rkey_destroy(rkey);
}

void RW_File_AddEntry_ParentDir(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, *pTag_End;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	PARENTDIR_FUNC_RETURN *pReturnResult_AddEntry_ParentDir;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = (void*)pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);
	pReturnResult_AddEntry_ParentDir = (PARENTDIR_FUNC_RETURN *)((char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int));
	pResult->nDataSize = sizeof(RW_FUNC_RETURN) + sizeof(PARENTDIR_FUNC_RETURN);

	pResult->ret_value = my_AddEntryInfo_Remote_Request(pRF_Op_Msg->szName, pRF_Op_Msg->nLen_Parent_Dir_Name, pRF_Op_Msg->flag, &(pReturnResult_AddEntry_ParentDir->idx_Parent_Dir));
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);
	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	ucp_rkey_destroy(rkey);
}

void RW_File_RemoveEntry_ParentDir(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_ucx, tag_magic, *pTag_End, *pIntParam;
	RW_FUNC_RETURN *pResult;
	// struct ibv_mr *mr_shm_global;
	ucp_mem_h mr_shm_global;
	void *rem_buff;
	// unsigned int rkey;
	void* rkey_buffer;
	ucp_rkey_h rkey;
	PARENTDIR_FUNC_RETURN *pReturnResult_AddEntry_ParentDir;

	// rkey = pRF_Op_Msg->rkey;
	rkey_buffer = (void*)pRF_Op_Msg->rkey_buffer;
	rem_buff = pRF_Op_Msg->rem_buff;
	pIntParam = (int*)(pRF_Op_Msg->szName);
	tag_magic = pRF_Op_Msg->tag_magic;
	idx_ucx = pRF_Op_Msg->idx_qp;
	mr_shm_global = Server_ucx.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_ucx.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);
	pReturnResult_AddEntry_ParentDir = (PARENTDIR_FUNC_RETURN *)((char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int));
	pResult->nDataSize = sizeof(RW_FUNC_RETURN);
	Server_ucx.UCX_Unpack_Rkey(idx_ucx, rkey_buffer, &rkey);

	my_RemoveEntryInfo_Remote_Request((char*)(&(pIntParam[2])), pIntParam[0], pIntParam[1]);
	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;
	pResult->ret_value = 0;	// always success

	// Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
	Server_ucx.UCX_Put(idx_ucx, (void*)pResult, rem_buff, rkey, pResult->nDataSize);
	ucp_rkey_destroy(rkey);
}
/*
void RW_File_UpdateEntry_ParentDir_EntryIdx(IO_CMD_MSG *pRF_Op_Msg)
{
	int idx_qp, tag_magic, *pTag_End;
	RW_FUNC_RETURN *pResult;
	struct ibv_mr *mr_shm_global;
	void *rem_buff;
	unsigned int rkey;
	PARENTDIR_FUNC_RETURN *pReturnResult_AddEntry_ParentDir;

	idx_qp = pRF_Op_Msg->idx_qp;

	rem_buff = pRF_Op_Msg->rem_buff;
	rkey = pRF_Op_Msg->rkey;
	tag_magic = pRF_Op_Msg->tag_magic;
	mr_shm_global = Server_qp.mr_shm_global;
	pResult = (RW_FUNC_RETURN *)( (char*)(Server_qp.p_shm_IO_Result) + pRF_Op_Msg->tid*IO_RESULT_BUFFER_SIZE);
	pReturnResult_AddEntry_ParentDir = (PARENTDIR_FUNC_RETURN *)((char*)pResult + sizeof(RW_FUNC_RETURN) - sizeof(int));
	pResult->nDataSize = sizeof(RW_FUNC_RETURN);

	my_UpdateEntryIndex_in_ParentDir(pRF_Op_Msg->szName, pRF_Op_Msg->flag);	// use pRF_Op_Msg->flag to store NewIdxEntry_in_Dir. 
//	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
//	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
//	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;
//	pResult->ret_value = 0;	// always success

	// no need to return anything
//	Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
}
*/

