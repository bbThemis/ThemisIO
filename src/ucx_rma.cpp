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
pthread_t pthread_IO_Worker_UCX[NUM_THREAD_IO_WORKER];

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
    free(rkey_buffer);
    ucp_worker_destroy(ucp_main_worker);
    ucp_cleanup(ucp_main_context);
}

int SERVER_RDMA::Get_IO_Worker_Index_from_UCX_Index(int idx_ucx)
{
	int nNumQueuePerWorker, idx_Queue, nUCX_InterServer;

	nUCX_InterServer = nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER;
	if(idx_ucx < nUCX_InterServer)	return (idx_ucx % NUM_THREAD_IO_WORKER_INTER_SERVER);

//	idx_Queue = ( ((idx_ucx-NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER + (idx_ucx-NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER/MAX_NUM_QUEUE_M1 ) % MAX_NUM_QUEUE_M1) + NUM_THREAD_IO_WORKER_INTER_SERVER;
//	idx_Queue = ( ((idx_ucx-nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER + (idx_ucx-nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER/MAX_NUM_QUEUE_MX ) % MAX_NUM_QUEUE_MX) + nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER;
	idx_Queue = ( ((idx_ucx - nUCX_InterServer)*NUM_QUEUE_PER_WORKER + (idx_ucx - nUCX_InterServer)*NUM_QUEUE_PER_WORKER/MAX_NUM_QUEUE_MX ) % MAX_NUM_QUEUE_MX) + NUM_THREAD_IO_WORKER_INTER_SERVER;

	if( ( ( MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER ) % ( NUM_THREAD_IO_WORKER - NUM_THREAD_IO_WORKER_INTER_SERVER ) ) == 0 )	{
		nNumQueuePerWorker = ( MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER ) / ( NUM_THREAD_IO_WORKER - NUM_THREAD_IO_WORKER_INTER_SERVER ) ;
	}
	else	{
		nNumQueuePerWorker = ( MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER ) / ( NUM_THREAD_IO_WORKER - NUM_THREAD_IO_WORKER_INTER_SERVER ) + 1;
	}

//	if(idx_ucx < nUCX_InterServer)	{	// inter-server qp
//		return pQP_Data[idx_ucx].idx_queue;
//	}
//	else	{
		return (( (idx_Queue-NUM_THREAD_IO_WORKER_INTER_SERVER)/nNumQueuePerWorker ) + NUM_THREAD_IO_WORKER_INTER_SERVER);
//	}
}

void SERVER_RDMA::Init_Server_UCX_Env(int remote_buff_size) {
    ucp_main_context = NULL;
    ucp_main_worker = NULL;
    Init_Context(&ucp_main_context, &ucp_main_worker);
    if(!ucp_main_context) {
        fprintf(stderr, "SERVER_RDMA Failure: No HCA can use.\n");
		exit(1);
    }
    pIO_Cmd_ToSend_Other_Server = (IO_CMD_MSG *)memalign(64, DATA_COPY_THRESHOLD_SIZE + 4096);
	assert(pIO_Cmd_ToSend_Other_Server != NULL);
	RegisterBuf_RW_Local_Remote(pIO_Cmd_ToSend_Other_Server, DATA_COPY_THRESHOLD_SIZE + 4096, &mr_loc);
	assert(mr_loc != NULL);

	for(int i=0; i<NUM_THREAD_IO_WORKER; i++)	{
        Init_Worker(ucp_main_context, &ucp_data_worker[i]);
		assert(ucp_data_worker[i] != NULL);
	}
    
}

void SERVER_RDMA::Clean_UCX_Env(void) {
    for(int i=0; i<NUM_THREAD_IO_WORKER; i++) {
        if(ucp_data_worker[i]) {
            ucp_worker_destroy(ucp_data_worker[i]);
            ucp_data_worker[i] = NULL;
        }
    }
}

ucs_status_t SERVER_RDMA::server_create_ep(ucp_worker_h data_worker,
                                     ucp_address_t* peer_address,
                                     ucp_ep_h *peer_ep) {
    ucp_ep_params_t ep_params;
    ucs_status_t    status;

    /* Server creates an ep to the client on the data worker.
     * This is not the worker the listener was created on.
     * The client side should have initiated the connection, leading
     * to this ep's creation */
    ep_params.field_mask      = UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
    ep_params.address    = peer_address;
    ep_params.err_handler.cb  = err_cb;
    ep_params.err_handler.arg = NULL;

    status = ucp_ep_create(data_worker, &ep_params, peer_ep);
    if (status != UCS_OK) {
        fprintf(stderr, "failed to create an endpoint on the server: (%s)\n",
                ucs_status_string(status));
    }

    return status;
}

void SERVER_RDMA::Server_Loop() {
    while(1) {}
}

void SERVER_RDMA::AllocateUCPDataWorker(int idx) {
    UCX_DATA *pUCX;
	int idx_io_worker;
	
	pUCX = &(pUCX_Data[idx]);
    pUCX_Data[idx].bServerReady = 0;
	idx_io_worker = Get_IO_Worker_Index_from_UCX_Index(idx);

    pUCX->ucp_data_worker = ucp_data_worker[idx_io_worker];

    if(pthread_mutex_init(&(pUCX->qp_lock), NULL) != 0) {
		perror("pthread_mutex_init");
		exit(1);
	}

    ucs_status_t status;
    status = ucp_worker_get_address(pUCX->ucp_data_worker, &pUCX->address_p, &pUCX->address_length);
    assert(pUCX->address_length <= MAX_UCP_ADDR_LEN);
    if(status != UCS_OK) {
        fprintf(stderr, "ucp_worker_get_address Failure: %s.\n", ucs_status_string(status));
		exit(1);
    }
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

    pUCX_Data = (UCX_DATA *)malloc(sizeof(UCX_DATA) * max_num_qp);
    assert(pUCX_Data != NULL);

    nSizeofNewMsgFlag = sizeof(char)*max_qp;
	nSizeofHeartBeat = sizeof(time_t)*max_qp;
	nSizeofIOCmdMsg = sizeof(IO_CMD_MSG)*max_qp;
	nSizeofIOResult = sizeof(char)*IO_RESULT_BUFFER_SIZE*NUM_THREAD_IO_WORKER;	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	nSizeofIOResult_Recv = sizeof(char)*IO_RESULT_BUFFER_SIZE;	// the size of buffer to recv results from other servers. 

	nSizePerCallReturnBlock = (SIZE_FOR_NEW_MSG + sizeof(IO_CMD_MSG) + sizeof(RW_FUNC_RETURN) + 1*sizeof(int) + 64) & 0xFFFFFFC0;
    // UCX_TEST
	// nSizeofReturnBuffer = CFixedSizeMemAllcator.Query_MemSize(nSizePerCallReturnBlock, MAX_NUM_RETURN_BUFF);
    nSizeofReturnBuffer = 0;
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
    // UCX_TEST
	// CFixedSizeMemAllcator.Init_Memory_Pool(p_CallReturnBuff);
    ucs_status_t status;
    status = RegisterBuf_RW_Local_Remote(p_shm_Global, nSizeshm_Global, &mr_shm_global);
	assert(mr_shm_global != NULL);

    rem_buff = (unsigned char*)p_shm_Global;
    rem_buff_size = nSizeshm_Global;
	status = ucp_rkey_pack(ucp_main_context, mr_shm_global, &rkey_buffer, &rkey_buffer_size);
    assert(rkey_buffer != NULL);
    assert(rkey_buffer_size <= MAX_UCP_RKEY_SIZE);
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

void SERVER_RDMA::UCX_Put(int idx, void* loc_buff, void* rem_buf, size_t len) {
    long int t1_ms, t2_ms;
    struct timeval tm1, tm2;
    int bTimeOut=0;	// the flag of time out in PUT. 
    if(pUCX_Data[idx].bTimeout)	{	// Something wrong with this QP. Client may disconnect or die...
		printf("WARNING> QP %d got timeout in previous Put(). Ignore all OPs for this QP. HostName = %s tid = %d\n", 
			idx, pUCX_Data[idx].szClientHostName, pUCX_Data[idx].ctid);
		return;
	}
    ucp_ep_h peer_ep = pUCX_Data[idx].peer_ep;
    ucp_rkey_h rkey = pUCX_Data[idx].rkey;
    ucp_worker_h ucp_data_worker = pUCX_Data[idx].ucp_data_worker;
retry:
    ucp_request_param_t param;
    param.op_attr_mask = 0;
    ucs_status_ptr_t req = ucp_put_nbx(peer_ep, loc_buff, len, (uint64_t)rem_buff, rkey, &param);
    ucs_status_t status = ucp_request_check_status(req);
    if(UCS_PTR_IS_ERR(req)) {
        fprintf(stderr, "Error occured at %s:L%d. Failure: ucp_put_nbx in Put(). tid= %d nConnectionAccu = %d Server() Client(%s)\n", 
			__FILE__, __LINE__, nConnectionAccu, pUCX_Data[idx].ctid, pUCX_Data[idx].szClientHostName, pUCX_Data[idx].szClientExeName);
		exit(1);
    }
    pUCX_Data[idx].nPut_Get++;
    if( (pUCX_Data[idx].nPut_Get - pUCX_Data[idx].nPut_Get_Done) >= UCX_QUEUE_SIZE ) {
        gettimeofday(&tm1, NULL);
		t1_ms = (tm1.tv_sec * 1000) + (tm1.tv_usec / 1000);
        while(1) {
            ucp_worker_progress(ucp_data_worker);
            gettimeofday(&tm2, NULL);
			t2_ms = (tm2.tv_sec * 1000) + (tm2.tv_usec / 1000);
            ucs_status_t status = ucp_request_check_status(req);
            if(status == UCS_OK) {
                pUCX_Data[idx].nPut_Get_Done +=1;
            }
            else if(status == UCS_INPROGRESS) {
                if( (t2_ms - t1_ms) > UCX_PUT_TIMEOUT_MS )	{
					bTimeOut = 1;
					goto retry;
				}
            }
            else {
                fprintf(stderr, "ucp_put_nbx failed %s\n", ucs_status_string(status));
//				pthread_mutex_unlock(&(pUCX_Data[idx].qp_lock));
				exit(1);
				return;
            }
        }
    }
}

void SERVER_RDMA::UCX_Get(int idx, void* loc_buff, void* rem_buf, size_t len) {
    long int t1_ms, t2_ms;
    struct timeval tm1, tm2;
    int bTimeOut=0;	// the flag of time out in PUT. 
    if(pUCX_Data[idx].bTimeout)	{	// Something wrong with this QP. Client may disconnect or die...
		printf("WARNING> QP %d got timeout in previous Put(). Ignore all OPs for this QP. HostName = %s tid = %d\n", 
			idx, pUCX_Data[idx].szClientHostName, pUCX_Data[idx].ctid);
		return;
	}
    ucp_ep_h peer_ep = pUCX_Data[idx].peer_ep;
    ucp_rkey_h rkey = pUCX_Data[idx].rkey;
    ucp_worker_h ucp_data_worker = pUCX_Data[idx].ucp_data_worker;
retry:
    ucp_request_param_t param;
    param.op_attr_mask = 0;
    ucs_status_ptr_t req = ucp_get_nbx(peer_ep, loc_buff, len, (uint64_t)rem_buff, rkey, &param);
    ucs_status_t status = ucp_request_check_status(req);
    if(UCS_PTR_IS_ERR(req)) {
        fprintf(stderr, "Error occured at %s:L%d. Failure: ucp_get_nbx in Put(). tid= %d nConnectionAccu = %d Server() Client(%s)\n", 
			__FILE__, __LINE__, nConnectionAccu, pUCX_Data[idx].ctid, pUCX_Data[idx].szClientHostName, pUCX_Data[idx].szClientExeName);
		exit(1);
    }
    pUCX_Data[idx].nPut_Get++;
    if( (pUCX_Data[idx].nPut_Get - pUCX_Data[idx].nPut_Get_Done) >= UCX_QUEUE_SIZE ) {
        gettimeofday(&tm1, NULL);
		t1_ms = (tm1.tv_sec * 1000) + (tm1.tv_usec / 1000);
        while(1) {
            ucp_worker_progress(ucp_data_worker);
            gettimeofday(&tm2, NULL);
			t2_ms = (tm2.tv_sec * 1000) + (tm2.tv_usec / 1000);
            ucs_status_t status = ucp_request_check_status(req);
            if(status == UCS_OK) {
                pUCX_Data[idx].nPut_Get_Done +=1;
            }
            else if(status == UCS_INPROGRESS) {
                if( (t2_ms - t1_ms) > UCX_PUT_TIMEOUT_MS )	{
					bTimeOut = 1;
					goto retry;
				}
            }
            else {
                fprintf(stderr, "ucp_get_nbx failed %s\n", ucs_status_string(status));
//				pthread_mutex_unlock(&(pQP_Data[idx].qp_lock));
				exit(1);
				return;
            }
        }
    }
}