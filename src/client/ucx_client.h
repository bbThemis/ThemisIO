#ifndef __UCX_CLIENT
#define __UCX_CLIENT

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

//export SLURM_JOBID="12345"
//export SLURM_NNODES=1

// SLURM_JOBID=2339541. 4 bytes
// IP. 4 bytes
// tid. 4 bytes

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <assert.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <signal.h>
#include <sys/syscall.h>
#include<signal.h>
#include <sys/time.h>
#include <malloc.h>
#include <netinet/tcp.h>

#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <uct/api/uct.h>

#include "client_common.h"
#include "utility.h"
#include "dict.h"
#include "ucx_rma_common.h"
#include "io_ops_common.h"

static pthread_mutex_t ucx_process_lock; // for this process
static pthread_mutex_t ht_ucx_lock;
static CHASHTABLE_INT *pHT_ucx=NULL;
static struct elt_Int *elt_list_ucx = NULL;
static int *ht_table_ucx=NULL;

#define UCX_PORT 12589

static __thread unsigned char *ucx_rem_buff=NULL, *ucx_loc_buff=NULL; 
static __thread ucp_mem_h ucx_mr_rem = NULL, ucx_mr_loc = NULL;

// file server info is stored at /dev/shm/ucx_myfs. Use bcast_dir to share this file across nodes. 
FSSERVERLIST *pUCXFileServerList, UCXFileServerListLocal;	// pUCXFileServerList in shared memory

class CLIENT_UCX {
private:
	UCX_EXCH_DATA ucx_my_data, ucx_pal_data;
	UCX_IB_MEM_DATA my_remote_mem;
	uint64_t nPut=0, nPut_Done=0;
	uint64_t nGet=0, nGet_Done=0;
	pthread_mutex_t ucx_put_get_lock;
	int sock = 0;

	void server_create_ep(void);
	void AllocateUCPDataWorker(void);
	void Setup_Socket(char szServerIP[]);

	static int Init_Context(ucp_context_h *ucp_context);
	int Init_Worker(ucp_context_h ucp_context, ucp_worker_h *ucp_worker);

public:
	static ucp_context_h ucp_main_context;
	static int Done_UCX_Init;
	static void Init_UCX_Env(void);

	ucp_ep_h server_ep;
	UCX_IB_MEM_DATA pal_remote_mem;

	ucp_mem_h mr_rem_thread = NULL, mr_loc_thread = NULL;
	ucp_mem_h mr_loc_ucx_Obj = NULL;
	ucp_worker_h  ucp_worker = NULL;

	time_t qp_heart_beat_t = 0;
	uint64_t remote_addr_new_msg;	// the address of remote buffer to notify a new message
	uint64_t remote_addr_heart_beat;	// the address of remote buffer to write heart beat time info.
	uint64_t remote_addr_IO_CMD;	// the address of remote buffer to IO requests.
	int ucx_put_get_locked;
	int tid = 0;
	int Idx_fs = -1;

	void CloseUCPDataWorker(void);
	void Setup_UCP_Connection(int IdxServer, char loc_buff[], size_t size_loc_buff, char rem_buff[], size_t size_rem_buff);
	static ucs_status_t RegisterBuf_RW_Local_Remote(void* buf, size_t len, ucp_mem_h* memh);
	int UCX_Put(void* loc_buff, void* rem_buf, ucp_rkey_h rkey, size_t len);
    int UCX_Get(void* loc_buff, void* rem_buf, ucp_rkey_h rkey, size_t len);
};

static __thread CLIENT_UCX *pClient_ucx[MAX_FS_UCX_SERVER];
static CLIENT_UCX *pClient_ucx_List[MAX_UCX_PER_PROCESS];


inline void Allocate_ucx_loc_rem_buff(void)
{
	int i;

	if(ucx_rem_buff == NULL)	{
		ucx_rem_buff = (unsigned char *)memalign(64, 2*(IO_RESULT_BUFFER_SIZE + 4096));
		assert(ucx_rem_buff != NULL);
		CLIENT_UCX::RegisterBuf_RW_Local_Remote(ucx_rem_buff, IO_RESULT_BUFFER_SIZE + 4096, &ucx_mr_rem);
		assert(ucx_mr_rem != NULL);
		ucx_loc_buff = ucx_rem_buff + IO_RESULT_BUFFER_SIZE + 4096;
		CLIENT_UCX::RegisterBuf_RW_Local_Remote(ucx_loc_buff, IO_RESULT_BUFFER_SIZE + 4096, &ucx_mr_loc);
		assert(ucx_mr_loc != NULL);

		for(i=0; i<MAX_FS_UCX_SERVER; i++)	{
			if(pClient_ucx[i])	{
				pClient_ucx[i]->mr_loc_thread = ucx_mr_loc;
				pClient_ucx[i]->mr_rem_thread = ucx_mr_rem;
			}
		}
	}
}

void CLIENT_UCX::server_create_ep() {
	ucp_ep_params_t ep_params;
    ucs_status_t    status;

    /* Server creates an ep to the client on the data worker.
     * This is not the worker the listener was created on.
     * The client side should have initiated the connection, leading
     * to this ep's creation */
    ep_params.field_mask      = UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
	ep_params.address    = (ucp_address_t*)ucx_pal_data.peer_address;
    ep_params.err_handler.cb  = err_cb;
    ep_params.err_handler.arg = NULL;
	status = ucp_ep_create(ucp_worker, &ep_params, &server_ep);
    if (status != UCS_OK) {
        fprintf(stderr, "Error occured at %s:L%d. Failure: ucp_ep_create.\n", __FILE__, __LINE__);
		exit(1);
    } /*else {
        fprintf(stdout, "mpi_rank %d succeed to create an endpoint on the server: (%s)\n", mpi_rank,
                ucs_status_string(status));
    }*/
}

void CLIENT_UCX::AllocateUCPDataWorker() {
	int ret = Init_Worker(ucp_main_context, &ucp_worker);
	if(ret != 0 || ucp_worker == NULL) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ucp_worker_create.\n", __FILE__, __LINE__);
		exit(1);
	}

	ucs_status_t status;
	ucp_address_t *addr;
	size_t addr_len;
    status = ucp_worker_get_address(ucp_worker, &addr, &addr_len);
    assert(addr_len <= MAX_UCP_ADDR_LEN);
	memcpy(ucx_my_data.peer_address, addr, addr_len);
	ucx_my_data.peer_address_length = addr_len;
	
}

int CLIENT_UCX::Init_Worker(ucp_context_h ucp_context, ucp_worker_h *ucp_worker) {
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

void CLIENT_UCX::Setup_Socket(char szServerIP[])
{
    struct sockaddr_in serv_addr; 
	int one = 1;
	struct timeval tm1, tm2;	// tm1.tv_sec
	unsigned long long t;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) { 
        printf("\n Socket creation error \n"); 
        return; 
    } 

    if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)	perror("setsockopt(2) error");
	
    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(UCX_PORT); 
	
    // Convert IPv4 and IPv6 addresses from text to binary form 
    if(inet_pton(AF_INET, szServerIP, &serv_addr.sin_addr)<=0)  { 
        printf("\nInvalid address/ Address not supported \n"); 
        return; 
    }
	
//	gettimeofday(&tm1, NULL);
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) { 
        printf("\nConnection Failed \n"); 
        return;
    }
//	gettimeofday(&tm2, NULL);
//	t = 1000000 * (tm2.tv_sec - tm1.tv_sec) + (tm2.tv_usec - tm1.tv_usec);
//	printf("DBG> Rank = %d t_connect = %lld\n", mpi_rank, t);

    if (setsockopt(sock, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)	perror("setsockopt(2) error");
}

int CLIENT_UCX::Init_Context(ucp_context_h *ucp_context) {
	ucp_params_t ucp_params;
    ucs_status_t status;
    int ret = 0;

    memset(&ucp_params, 0, sizeof(ucp_params));

    /* UCP initialization */
    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES | UCP_PARAM_FIELD_NAME | UCP_PARAM_FIELD_MT_WORKERS_SHARED;
    ucp_params.name       = "ucp_themisio_client";
    ucp_params.mt_workers_shared = 1;
    ucp_params.features = UCP_FEATURE_RMA;
    status = ucp_init(&ucp_params, NULL, ucp_context);
    if (status != UCS_OK) {
        fprintf(stderr, "failed to ucp_init (%s)\n", ucs_status_string(status));
        ret = -1;
    }
	return ret;
}

ucp_context_h CLIENT_UCX::ucp_main_context=NULL;
int CLIENT_UCX::Done_UCX_Init = 0;

void CLIENT_UCX::Init_UCX_Env() {
	int ret, Found_IB=0;
	int nDevices;
	struct ibv_device_attr device_attr;
	struct timeval tm1, tm2;	// tm1.tv_sec
	unsigned long long t;
	pthread_mutex_lock(&ucx_process_lock);

	if(Done_UCX_Init == 0) {
		Init_Context(&ucp_main_context);
		Done_UCX_Init = 1;
	}
	if(!ucp_main_context) {
		fprintf(stderr, "CLIENT_UCX Failure: No HCA can use.\n");
		exit(1);
	}
	pthread_mutex_unlock(&process_lock);
}

void CLIENT_UCX::Setup_UCP_Connection(int IdxServer, char loc_buff[], size_t size_loc_buff, char rem_buff[], size_t size_rem_buff) {
	int idx;
//	GLOBAL_ADDR_DATA Global_Addr_Data;
	unsigned long long t;
	struct timeval tm1, tm2;
//	JOB_INFO_DATA JobInfo;
	UCX_DATA_SEND_BY_SERVER data_to_recv;
	UCX_DATA_SEND_BY_CLIENT data_to_send;
	char szHostName[64];
	#ifdef SYS_gettid
	tid = syscall(SYS_gettid);
#else
	tid = gettid();
#endif
	Idx_fs = IdxServer;
	ucp_worker = NULL;

	nPut = nPut_Done = 0;
	nGet = nGet_Done = 0;
	sock = 0;
	mr_loc_ucx_Obj = NULL;
	qp_heart_beat_t = 0;

	if(pthread_mutex_init(&ucx_put_get_lock, NULL) != 0) { 
        printf("\n mutex ucx_put_get_lock init failed\n"); 
        exit(1);
    }
	ucx_put_get_locked = 0;
	Setup_Socket(UCXFileServerListLocal.FS_List[IdxServer].szIP);
	AllocateUCPDataWorker();
	const char *fake_user_id = 0;
	gethostname(szHostName, 63);
	Take_ShortName(szHostName);
	data_to_send.JobInfo.comm_tag = TAG_SUBMIT_JOB_INFO;
	data_to_send.JobInfo.nnode = nnode_this_job;
	data_to_send.JobInfo.jobid = jobid;
	data_to_send.JobInfo.cip = pFileServerList->myip;
	data_to_send.JobInfo.ctid = tid;
	data_to_send.JobInfo.cuid = getuid();
	fake_user_id = getenv("THEMIS_FAKE_USERID");
	if (fake_user_id)
		sscanf(fake_user_id, "%u", &data_to_send.JobInfo.cuid);
	data_to_send.JobInfo.cgid = getgid();
	memcpy(data_to_send.JobInfo.szClientHostName, szHostName, UCX_MAX_HOSTNAME_LEN);
	memcpy(data_to_send.JobInfo.szClientExeName, szExeName, UCX_MAX_EXENAME_LEN);

	data_to_send.ucx.comm_tag = TAG_EXCH_UCX_INFO;
	memcpy(data_to_send.ucx.peer_address, ucx_my_data.peer_address, ucx_my_data.peer_address_length);
	data_to_send.ucx.peer_address_length = ucx_my_data.peer_address_length;

	RegisterBuf_RW_Local_Remote((void*)this, sizeof(CLIENT_UCX), &mr_loc_ucx_Obj);
	if(ucx_loc_buff == NULL)	Allocate_ucx_loc_rem_buff();
	mr_loc_thread = ucx_mr_loc;
	mr_rem_thread = ucx_mr_rem;

	my_remote_mem.addr = (uint64_t)ucx_rem_buff;
	void* rkey_buffer;
    size_t rkey_buffer_size = 0;
	ucs_status_t status = ucp_rkey_pack(ucp_main_context, ucx_mr_rem, &rkey_buffer, &rkey_buffer_size);
    assert(rkey_buffer != NULL);
    assert(rkey_buffer_size <= MAX_UCP_RKEY_SIZE);
    assert(status == UCS_OK);
	memcpy(my_remote_mem.rkey_buffer, rkey_buffer, rkey_buffer_size);
	my_remote_mem.rkey_buffer_size = rkey_buffer_size;
	
	data_to_send.ib_mem.comm_tag = TAG_EXCH_MEM_INFO;
	data_to_send.ib_mem.addr = my_remote_mem.addr;
	memcpy(data_to_send.ib_mem.rkey_buffer, my_remote_mem.rkey_buffer, rkey_buffer_size);
	data_to_send.ib_mem.rkey_buffer_size = rkey_buffer_size;

	write(sock, &(data_to_send), sizeof(UCX_DATA_SEND_BY_CLIENT));	// submit job info
	read(sock, &(data_to_recv), sizeof(UCX_DATA_SEND_BY_SERVER));

	remote_addr_new_msg = data_to_recv.global_addr.addr_NewMsgFlag;
	remote_addr_heart_beat = data_to_recv.global_addr.addr_TimeHeartBeat;
	remote_addr_IO_CMD = data_to_recv.global_addr.addr_IO_Cmd_Msg;

	ucx_pal_data.comm_tag = data_to_recv.ucx.comm_tag;
	memcpy(ucx_pal_data.peer_address, data_to_recv.ucx.peer_address, data_to_recv.ucx.peer_address_length);
	ucx_pal_data.peer_address_length = data_to_recv.ucx.peer_address_length;

	server_create_ep();

	pal_remote_mem.comm_tag = data_to_recv.ib_mem.comm_tag;
	pal_remote_mem.addr = data_to_recv.ib_mem.addr;
	status = ucp_ep_rkey_unpack(server_ep, data_to_recv.ib_mem.rkey_buffer, &pal_remote_mem.rkey);
    assert(status == UCS_OK);
	if(bDebug)	printf("INFO> tid = %d Client Setup_UCP_Connection with server\n", tid);
	close(sock);

	pthread_mutex_lock(&ht_ucx_lock);
	fetch_and_add(&(pUCXFileServerList->FS_List[IdxServer].nQP), 1);

	idx = pHT_ucx->DictInsertAuto(tid, &elt_list_ucx, &ht_table_ucx);
	pClient_ucx_List[idx] = this;
	pClient_ucx[IdxServer] = this;
	
	nPut = 0;
	nGet = 0;
	nPut_Done = 0;
	nGet_Done = 0;
		
	pthread_mutex_unlock(&ht_ucx_lock);
}

ucs_status_t CLIENT_UCX::RegisterBuf_RW_Local_Remote(void* buf, size_t len, ucp_mem_h* memh) {
    uct_allocated_memory_t alloc_mem;
    ucp_mem_map_params_t mem_map_params;
    mem_map_params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH;
    mem_map_params.length = len;
    mem_map_params.address = buf;
    ucs_status_t status = ucp_mem_map(ucp_main_context, &mem_map_params, memh);
    return status;
}

int CLIENT_UCX::UCX_Put(void* loc_buf, void* rem_buf, ucp_rkey_h rkey, size_t len) {
	int ne, ret;
	
	if(ucp_worker == NULL)	return 1;
	pthread_mutex_lock(&ucx_put_get_lock);
	ucx_put_get_locked = 1;
	ucp_request_param_t param;
    memset(&param, 0, sizeof(ucp_request_param_t));
    param.op_attr_mask = 0;
    ucs_status_ptr_t req = ucp_put_nbx(server_ep, loc_buf, len, (uint64_t)rem_buf, rkey, &param);
	if(UCS_PTR_IS_ERR(req)) {
        fprintf(stderr, "Error occured at %s:L%d. Failure: ucp_put_nbx in Put(). ret = %d\n", __FILE__, __LINE__, ret);
		exit(1);
    }
	nPut++;
	if( (nPut - nPut_Done) >= UCX_QUEUE_SIZE ) {
        while(1) {
            ucp_worker_progress(ucp_worker);
            ucs_status_t status = ucp_request_check_status(req);
            if(status == UCS_OK) {
                nPut_Done +=1;
                break;
            }
            else if(status == UCS_INPROGRESS) {
            }
            else {
                fprintf(stderr, "ucp_put_nbx failed %s\n", ucs_status_string(status));
//				pthread_mutex_unlock(&(pQP_Data[idx].qp_lock));
				exit(1);
				return 1;
            }
        }
    }
	ucx_put_get_locked = 0;
	pthread_mutex_unlock(&ucx_put_get_lock);
	
	return 0;
}

int CLIENT_UCX::UCX_Get(void* loc_buf, void* rem_buf, ucp_rkey_h rkey, size_t len)  {
	int ne, ret;
	
	if(ucp_worker == NULL)	return 1;
	pthread_mutex_lock(&ucx_put_get_lock);
	ucp_request_param_t param;
    memset(&param, 0, sizeof(ucp_request_param_t));
    param.op_attr_mask = 0;
    ucs_status_ptr_t req = ucp_get_nbx(server_ep, loc_buf, len, (uint64_t)rem_buf, rkey, &param);
	if(UCS_PTR_IS_ERR(req)) {
        fprintf(stderr, "Error occured at %s:L%d. Failure: ucp_get_nbx in Get(). ret = %d\n", __FILE__, __LINE__, ret);
		exit(1);
    }
	nGet++;
	if( (nGet - nGet_Done) >= UCX_QUEUE_SIZE ) {
        while(1) {
            ucp_worker_progress(ucp_worker);
            ucs_status_t status = ucp_request_check_status(req);
            if(status == UCS_OK) {
                nGet_Done +=1;
                break;
            }
            else if(status == UCS_INPROGRESS) {
            }
            else {
                fprintf(stderr, "ucp_get_nbx failed %s\n", ucs_status_string(status));
//				pthread_mutex_unlock(&(pQP_Data[idx].qp_lock));
				exit(1);
				return 1;
            }
        }
    }
	ucx_put_get_locked = 0;
	pthread_mutex_unlock(&ucx_put_get_lock);

	return 0;
}

#endif

