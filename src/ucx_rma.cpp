#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <string.h>
#include <errno.h>

#include "ucx_rma.h"
#include "myfs.h"
#include "fixed_mem_allocator.h"

extern CFIXEDSIZE_MEM_ALLOCATOR CFixedSizeMemAllcator;
extern int mpi_rank, nFSServer;	// rank and size of MPI


SERVER_RDMA::SERVER_RDMA(void)
{
    if(pthread_mutex_init(&process_lock, NULL) != 0) { 
        printf("\n mutex process_lock init failed\n"); 
        exit(1);
    }
}

SERVER_RDMA::~SERVER_RDMA(void)
{
	pthread_mutex_destroy(&process_lock);

    ucp_cleanup(ucp_main_context);
}


int SERVER_RDMA::Init_Context(ucp_context_h *ucp_context, ucp_worker_h *ucp_worker)
{
	/* UCP objects */
    ucp_params_t ucp_params;
    ucs_status_t status;
    int ret = 0;

    memset(&ucp_params, 0, sizeof(ucp_params));

    /* UCP initialization */
    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES | UCP_PARAM_FIELD_NAME | UCP_PARAM_FIELD_MT_WORKERS_SHARED;
    ucp_params.name       = "ucp_themisio_server";
    ucp_params.mt_workers_shared = 1;
    ucp_params.features = UCP_FEATURE_RMA;
    status = ucp_init(&ucp_params, NULL, ucp_context);
    if (status != UCS_OK) {
        fprintf(stderr, "failed to ucp_init (%s)\n", ucs_status_string(status));
        ret = -1;
        goto err;
    }
    ret = Init_Worker(*ucp_context, ucp_worker);
    if (ret != 0) {
        goto err_cleanup;
    }

    return ret;
err_cleanup:
    ucp_cleanup(*ucp_context);
err:
    return ret;
}

int SERVER_RDMA::Init_Worker(ucp_context_h ucp_context, ucp_worker_h *ucp_worker) {
    ucp_worker_params_t worker_params;
    ucs_status_t status;
    int ret = 0;

    memset(&worker_params, 0, sizeof(worker_params));

    worker_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    worker_params.thread_mode = UCS_THREAD_MODE_MULTI;

    status = ucp_worker_create(ucp_context, &worker_params, ucp_worker);
    if (status != UCS_OK) {
        fprintf(stderr, "failed to ucp_worker_create (%s)\n", ucs_status_string(status));
        ret = -1;
    }
    return ret;
}

void SERVER_RDMA::Init_Server_Memory(int max_num_qp) {
    int i, nSizeofNewMsgFlag, nSizeofHeartBeat, nSizeofIOCmdMsg, nSizeofIOResult, nSizeofIOResult_Recv, nSizePerCallReturnBlock, nSizeofReturnBuffer;
	int offset;
	char *p_CallReturnBuff;

    max_qp = max_num_qp;
	p_Hash_socket_fd = (CHASHTABLE_INT *)malloc(CHASHTABLE_INT::GetStorageSize(max_num_qp*2));
	p_Hash_socket_fd->DictCreate(max_num_qp*2, &elt_list_socket_fd, &ht_table_socket_fd);	// init hash table

    nSizeofNewMsgFlag = sizeof(char)*max_qp;
	nSizeofHeartBeat = sizeof(time_t)*max_qp;
	nSizeofIOCmdMsg = sizeof(IO_CMD_MSG)*max_qp;
	nSizeofIOResult = sizeof(char)*IO_RESULT_BUFFER_SIZE*NUM_THREAD_IO_WORKER;	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	nSizeofIOResult_Recv = sizeof(char)*IO_RESULT_BUFFER_SIZE;	// the size of buffer to recv results from other servers. 

	nSizePerCallReturnBlock = (SIZE_FOR_NEW_MSG + sizeof(IO_CMD_MSG) + sizeof(RW_FUNC_RETURN) + 1*sizeof(int) + 64) & 0xFFFFFFC0;
	nSizeofReturnBuffer = CFixedSizeMemAllcator.Query_MemSize(nSizePerCallReturnBlock, MAX_NUM_RETURN_BUFF);

	if(mpi_rank == 0)	{
		nSizeshm_Global = nSizeofNewMsgFlag + nSizeofHeartBeat + nSizeofIOCmdMsg + nSizeofIOResult + nSizeofIOResult_Recv + nSizeofReturnBuffer 
			+ sizeof(JOB_SCALE_LIST) + sizeof(JOB_OP_SEND)*nFSServer;
	}
	else	{
		nSizeshm_Global = nSizeofNewMsgFlag + nSizeofHeartBeat + nSizeofIOCmdMsg + nSizeofIOResult + nSizeofIOResult_Recv + nSizeofReturnBuffer 
			+ sizeof(JOB_SCALE_LIST) + sizeof(JOB_OP_SEND);
	}
	p_shm_Global = memalign( 4096, nSizeshm_Global);
	assert(p_shm_Global != NULL);
	memset(p_shm_Global, 0, nSizeshm_Global);

	offset = 0;
	p_shm_NewMsgFlag = (unsigned char *)p_shm_Global;
	offset += nSizeofNewMsgFlag;

	p_shm_TimeHeartBeat = (time_t *)((char*)p_shm_Global + offset);
	offset += nSizeofHeartBeat;

	p_shm_IO_Cmd_Msg = (IO_CMD_MSG *)((char*)p_shm_Global + offset);
	offset += nSizeofIOCmdMsg;

	p_shm_IO_Result = (char*)((char*)p_shm_Global + offset);
	offset += nSizeofIOResult;

	p_shm_IO_Result_Recv = (char*)((char*)p_shm_Global + offset);
	offset += nSizeofIOResult_Recv;

	p_CallReturnBuff = (char*)((char*)p_shm_Global + offset );
	offset += nSizeofReturnBuffer;

	pJobScale_Local = (JOB_SCALE_LIST *)((char*)p_shm_Global + offset);
	offset += sizeof(JOB_SCALE_LIST);

	pJob_OP_Send = (JOB_OP_SEND *)((char*)p_shm_Global + offset);

    if(mpi_rank == 0)	{
		pJob_OP_Recv = pJob_OP_Send;
		pJobScale_Remote = pJobScale_Local;
	}
	// Other rank will get the value of pJobScale_Remote and pJob_OP_Recv with bcast!!!

	CFixedSizeMemAllcator.Init_Memory_Pool(p_CallReturnBuff);
    ucs_status_t status;
    status = RegisterBuf_RW_Local_Remote(p_shm_Global, nSizeshm_Global, &mr_shm_global);
	assert(mr_shm_global != NULL);

    rem_buff = (unsigned char*)p_shm_Global;
    rem_buff_size = nSizeshm_Global;
	status = ucp_rkey_pack(ucp_main_context, mr_shm_global, &rkey_buffer, &rkey_buffer_size);
    assert(rkey_buffer != NULL);
}

ucs_status_t SERVER_RDMA::RegisterBuf_RW_Local_Remote(void* buf, size_t len, ucp_mem_h* memh) {
    uct_allocated_memory_t alloc_mem;
    ucp_mem_map_params_t mem_map_params;
    mem_map_params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH;
    mem_map_params.length = len;
    mem_map_params.address = buf;
    ucs_status_t status = ucp_mem_map(ucp_main_context, &mem_map_params, memh);
    return status;
}