// g++ -march=skylake-avx512 -o server put_get_server.cpp dict.cpp xxhash.cpp -libverbs -lpthread -lrt -Wunused-variable

#ifndef __UCX_RMA
#define __UCX_RMA

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <signal.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <assert.h>
#include <sys/time.h>
#include <malloc.h>
#include <immintrin.h>

#include <stdlib.h>
#include <cstdint>
#include <cstdio>

#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <uct/api/uct.h>

#include "dict.h"
#include "io_queue.h"
#include "ucx_rma_common.h"

#include "ucx_qp_common.h"



extern CIO_QUEUE IO_Queue_List[MAX_NUM_QUEUE];
#define MAX_UCX_NEW_MSG	(1024*16)
typedef struct {
    ucp_worker_h ucp_data_worker;
    int nPut_Get, nPut_Get_Done;
    int jobid, idx_queue;	// jobid and the index of queue that handles this jobid
	int idx_JobRec, cuid, cgid, ctid, bTimeout, bServerReady;
    // These are only needed between file servers. Not needed for the pairs with regular compute node (file server clients). 
	uint64_t remote_addr_new_msg;	// the address of remote buffer to notify a new message
	uint64_t remote_addr_heart_beat;	// the address of remote buffer to write heart beat time info.
	uint64_t remote_addr_IO_CMD;	// the address of remote buffer to IO requests.

    ucp_address_t *address_p = NULL;
    size_t address_length       = 0;
    
    // char peer_address[MAX_UCP_ADDR_LEN];
	// size_t peer_address_length = 0;
	// char rkey_buffer[MAX_UCP_RKEY_SIZE];
    // size_t rkey_buffer_size = 0;

    ucp_ep_h  peer_ep;
    ucp_rkey_h rkey;

    unsigned long int	rem_addr;
    pthread_mutex_t	ucx_lock;
    char szClientHostName[UCX_MAX_HOSTNAME_LEN];
	char szClientExeName[UCX_MAX_EXENAME_LEN];

}UCX_DATA, *PUCX_DATA;

class SERVER_RDMA {
public:
    ucp_context_h ucp_main_context;
    ucp_worker_h  ucp_main_worker;

	in_addr_t sock_addr;    // local IP or INADDR_ANY
	int sock_port;          // local port to listen on
	int sock_fd;            // listener descriptor
	int sock_signal_fd;     // used to receive signals
	int sock_epoll_fd;      // used for all notification

    ucp_mem_h mr_rem, mr_loc, mr_shm_global = NULL;
    void* rkey_buffer;
    size_t rkey_buffer_size = 0;
    // ucp_address_t *address_p = NULL;
    // size_t address_length       = 0;

    int nConnectionAccu = 0;
    int max_qp, nQP, IdxLastQP, IdxLastQP64, FirstAV_QP;	// IdxLastQP64 is 64 aligned for IdxLastQP
    int nSizeshm_Global;
    FairnessMode fairness_mode;
pthread_mutex_t process_lock;	// for this process

    CHASHTABLE_INT *p_Hash_socket_fd = NULL;
	struct elt_Int *elt_list_socket_fd = NULL;
	int *ht_table_socket_fd=NULL;

    UCX_DATA *pUCX_Data = NULL;
    void *p_shm_Global = NULL;	// NewMsgFlag[], Time_HeartBeat[], IO_Msg[]
	unsigned char *p_shm_NewMsgFlag = NULL;
	time_t *p_shm_TimeHeartBeat = NULL;
	IO_CMD_MSG *p_shm_IO_Cmd_Msg = NULL;
	char *p_shm_IO_Result = NULL;
	char *p_shm_IO_Result_Recv = NULL;
	IO_CMD_MSG *pIO_Cmd_ToSend_Other_Server=NULL;

    long int T_Start_us = 0;
	JOB_SCALE_LIST *pJobScale_Local=NULL, *pJobScale_Remote=NULL;
	JOB_OP_SEND *pJob_OP_Recv=NULL;	// only allocate memory on rank 0
	JOB_OP_SEND *pJob_OP_Send=NULL;

    ucp_worker_h ucp_data_worker[NUM_THREAD_IO_WORKER];

    unsigned char		*rem_buff = NULL;
	int					rem_buff_size;

    SERVER_RDMA(void);
	~SERVER_RDMA(void);
    void Init_Server_Memory(int max_num_qp, int port);
    void Socket_Server_Loop(); // Socket_Server_Loop
    int Add_Epoll(int events, int fd);
	int Del_Epoll(int fd);
	int Setup_Listener(void);
	int Accept_Client();
	void Drain_Client(const int fd);

    void Init_Server_UCX_Env(int remote_buff_size);
    void Clean_UCX_Env(void);
    void AllocateUCPDataWorker(int idx); // IB_CreateQueuePair
    ucs_status_t server_create_ep(ucp_worker_h data_worker,
                                     ucp_address_t* peer_address,
                                     ucp_ep_h *server_ep);
    ucs_status_t RegisterBuf_RW_Local_Remote(void* buf, size_t len, ucp_mem_h* memh);
    
    void UCX_Pack_Rkey(ucp_mem_h memh, void *rkey_buffer);
    void UCX_Unpack_Rkey(int idx, void* rkey_buffer, ucp_rkey_h* rkey_p);
    void UCX_Put(int idx, void* loc_buff, void* rem_buf, void* rkey_buffer, size_t len);
    void UCX_Get(int idx, void* loc_buff, void* rem_buf, void* rkey_buffer, size_t len);
    void UCX_Put(int idx, void* loc_buff, void* rem_buf, ucp_rkey_h rkey, size_t len);
    void UCX_Get(int idx, void* loc_buff, void* rem_buf, ucp_rkey_h rkey, size_t len);
    int UCX_Flush(ucp_worker_h ucp_worker);

    int FindFirstAvailableQP(void);
    void ScanLostUCX();
    void ScanNewMsg();
    void Destroy_A_UCPWorker(int idx);
private:

    int nAllUCXNewMsg;
    int nPreAllUCXNewMsg;
    int Init_Context(ucp_context_h *ucp_context, ucp_worker_h *ucp_worker);
    int Init_Worker(ucp_context_h ucp_context, ucp_worker_h *ucp_worker);
    int Get_IO_Worker_Index_from_UCX_Index(int idx_ucx);
    ucs_status_t server_create_ep(ucp_worker_h data_worker,
                                     ucp_conn_request_h conn_request,
                                     ucp_ep_h *server_ep);
    
};

#endif

