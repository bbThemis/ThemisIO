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


class SERVER_RDMA {
public:
    ucp_context_h ucp_main_context;
    ucp_worker_h  ucp_main_worker;
    ucp_mem_h mr_rem, mr_loc, mr_shm_global = NULL;
    void* rkey_buffer = NULL;
    size_t rkey_buffer_size = 0;


    int max_qp, nQP, IdxLastQP, IdxLastQP64, FirstAV_QP;	// IdxLastQP64 is 64 aligned for IdxLastQP
    int nSizeshm_Global;
pthread_mutex_t process_lock;	// for this process

    CHASHTABLE_INT *p_Hash_socket_fd = NULL;
	struct elt_Int *elt_list_socket_fd = NULL;
	int *ht_table_socket_fd=NULL;

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

    unsigned char		*rem_buff = NULL;
	int					rem_buff_size;

    SERVER_RDMA(void);
	~SERVER_RDMA(void);
    int Init_Context(ucp_context_h *ucp_context, ucp_worker_h *ucp_worker);
    int Init_Worker(ucp_context_h ucp_context, ucp_worker_h *ucp_worker);
    void Init_Server_Memory(int max_num_qp);
    ucs_status_t RegisterBuf_RW_Local_Remote(void* buf, size_t len, ucp_mem_h* memh);
};

#endif

