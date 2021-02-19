#include "io_ops.h"
#include "qp.h"
#include "myfs.h"
#include "utility.h"

extern SERVER_QUEUEPAIR Server_qp;


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

void RW_Opendir(IO_CMD_MSG *pOP_Msg)
{
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

	if(pRF_Op_Msg->nLen <= DATA_COPY_THRESHOLD_SIZE)	pResult->ret_value = my_read(pRF_Op_Msg->fd, (char*)pResult+sizeof(RW_FUNC_RETURN)-sizeof(int), pRF_Op_Msg->nLen);
	pResult->nDataSize = (pResult->ret_value>0) ? (sizeof(RW_FUNC_RETURN) + pResult->ret_value) : (sizeof(RW_FUNC_RETURN));
	pResult->myerrno = errno;

	pResult->Tag_Ini = (int)((long int)rem_buff & 0xFFFFFFFF);
	pTag_End = (int*)( (char*)pResult + pResult->nDataSize - sizeof(int) );
//	pResult->Tag_End = (pResult->Tag_Ini) ^ tag_magic;
	*pTag_End = (pResult->Tag_Ini) ^ tag_magic;

	Server_qp.IB_Put(idx_qp, (void*)pResult, mr_shm_global->lkey, rem_buff, rkey, pResult->nDataSize);
}

void RW_PRead(IO_CMD_MSG *pOP_Msg)
{
}

void RW_Seek(IO_CMD_MSG *pOP_Msg)
{
}

void RW_Stat(IO_CMD_MSG *pOP_Msg)
{
}

void RW_LStat(IO_CMD_MSG *pOP_Msg)
{
}

void RW_FStat(IO_CMD_MSG *pOP_Msg)
{
}

void RW_Dir_Exist(IO_CMD_MSG *pOP_Msg)
{
}

