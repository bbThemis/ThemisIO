// g++ -march=skylake-avx512 -o server put_get_server.cpp dict.cpp xxhash.cpp -libverbs -lpthread -lrt -Wunused-variable

#ifndef __IBVERBS_WRPPER
#define __IBVERBS_WRPPER

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
#include "dict.h"
#include "qp_common.h"
#include "io_queue.h"

//#define N_THREAD_PREALLOCATE_QP	(1)
#define N_THREAD_PREALLOCATE_QP	(16)
#define N_THREAD_ADD_PREALLOCATE_QP	(4)

#define NUM_QP_PREALLOCATED		(128)
#define NUM_QP_TRIGGER_PREALLOCATED		(64)	// a half of NUM_QP_PREALLOCATED
//#define NUM_QP_PREALLOCATED		(2048)	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Important for RESNET50. It creates new QPs very frequently. 
//#define NUM_QP_TRIGGER_PREALLOCATED		(1024)	// a half of NUM_QP_PREALLOCATED. !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Important for RESNET50. It creates new QPs very frequently. 

#define DEFAULT_REM_BUFF_SIZE	(4096)

#define MAX_NEW_MSG	(1024*16)

#define DEF_CQ_MOD	(100)
//#define QUEUE_SIZE	(128)
#define IB_QUEUE_SIZE	(1)
#define CTX_POLL_BATCH		(16)


enum FairnessMode {
	// tradition first in first out
	FIFO, 
	// Priority based on jobs such that the throughput of each job is proportional
	// to the number of nodes in the job.
	SIZE_FAIR,

	// Priority based on jobs such that each job gets equal throughput.
	JOB_FAIR,

	// Priority based on users such that each user gets equal throughput.
	USER_FAIR, 

	// user-then-size-fair
	USER_SIZE_FAIR, 

	// user-then-job-fair
	USER_JOB_FAIR,

	// group-user-size
	GROUP_USER_SIZE_FAIR
};


typedef	struct	{
	int ready;
	unsigned int psn;
	struct ibv_qp *queue_pair;
	struct ibv_cq *send_complete_queue, *recv_complete_queue;
}QUEUE_PAIR_PREALLOCATED, *PQUEUE_PAIR_PREALLOCATED;

extern CIO_QUEUE IO_Queue_List[MAX_NUM_QUEUE];

inline int Align64_Int(int a)
{
	// return ( (a & 0x3F) ? (64 + (a & 0xFFFFFFC0) ) : (a) );

	// branch not needed
	return (a + 63) & ~63;
}

void Init_PreAllocated_QueuePair_List(void);
void* Func_thread_PreAllocate_QueuePair(void *pParam);
void CreateQueuePair(struct ibv_context* context, struct ibv_pd* pd, struct ibv_cq **pSend_complete_queue, struct ibv_cq **pRecv_complete_queue, struct ibv_qp **pQueue_pair, unsigned int *pPsn);

typedef	struct	{
	struct ibv_qp *queue_pair;
	struct ibv_cq *send_complete_queue, *recv_complete_queue;

	int nPut_Get, nPut_Get_Done;
	int jobid, idx_queue;	// jobid and the index of queue that handles this jobid
	int idx_JobRec, cuid, cgid, ctid, bTimeout, bServerReady;
    float rule_rate; // Rule_Lustre param

	// These are only needed between file servers. Not needed for the pairs with regular compute node (file server clients). 
	uint64_t remote_addr_new_msg;	// the address of remote buffer to notify a new message
	uint64_t remote_addr_heart_beat;	// the address of remote buffer to write heart beat time info.
	uint64_t remote_addr_IO_CMD;	// the address of remote buffer to IO requests.

	unsigned int tag_ib_me;
	unsigned int ib_my_lid;
	unsigned int ib_my_qpn;
	unsigned int ib_my_psn;

	unsigned int tag1;
	unsigned int ib_pal_lid;
	unsigned int ib_pal_qpn;
	unsigned int ib_pal_psn;

	unsigned int		tag_mem;
	int					rem_key;
	unsigned long int	rem_addr;
	pthread_mutex_t	qp_lock;
	char szClientHostName[MAX_HOSTNAME_LEN];
	char szClientExeName[MAX_EXENAME_LEN];
}QP_DATA, *PQP_DATA;

//typedef	struct	{
//	unsigned char msg[256];	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! NEED changes!!!!!!!!!!!!!!!!!!!!
//}IO_CMD_MSG, *PIO_CMD_MSG;


class SERVER_QUEUEPAIR {
public:
	in_addr_t sock_addr;    // local IP or INADDR_ANY
	int sock_port;          // local port to listen on
	int sock_fd;            // listener descriptor
	int sock_signal_fd;     // used to receive signals
	int sock_epoll_fd;      // used for all notification

	int nConnectionAccu = 0;
	int max_qp, nQP, IdxLastQP, IdxLastQP64, FirstAV_QP;	// IdxLastQP64 is 64 aligned for IdxLastQP
	int nSizeshm_Global;
	FairnessMode fairness_mode;
pthread_mutex_t process_lock;	// for this process

	CHASHTABLE_INT *p_Hash_socket_fd = NULL;
	struct elt_Int *elt_list_socket_fd = NULL;
	int *ht_table_socket_fd=NULL;

	QP_DATA *pQP_Data = NULL;
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

	struct ibv_device** dev_list_ = NULL;
	struct ibv_context* context_ = NULL;
	struct ibv_pd* pd_ = NULL;
	struct ibv_port_attr port_attr_ = {};
	struct ibv_mr *mr_rem, *mr_loc, *mr_shm_global = NULL;
	struct ibv_cq *send_complete_queue[NUM_THREAD_IO_WORKER];

	int					tag_mem;
	int					rem_key;
	unsigned char		*rem_buff = NULL;
	int					rem_buff_size;

	SERVER_QUEUEPAIR(void);
	~SERVER_QUEUEPAIR(void);
	void Init_Server_Socket(int max_num_qp, int port);
	void Socket_Server_Loop(void);
	int Add_Epoll(int events, int fd);
	int Del_Epoll(int fd);
	int Setup_Listener(void);
	int Accept_Client();
	void Drain_Client(const int fd);

	void Init_Server_IB_Env(int remote_buff_size);
	void Clean_IB_Env(void);
	void IB_CreateQueuePair(int idx);
	void Get_A_PreAllocated_QueuePair(int idx);
	void IB_Modify_QP(struct ibv_qp* qp, uint32_t src_psn, uint16_t dest_lid, uint32_t dest_pqn, uint32_t dest_psn);
	struct ibv_mr* IB_RegisterBuf_RW_Local_Remote(void* buf, size_t len);
	void IB_Put(int idx, void* loc_buf, uint32_t lkey, void* rem_buf, uint32_t rkey, size_t len);
	void IB_Get(int idx, void* loc_buf, uint32_t lkey, void* rem_buf, uint32_t rkey, size_t len);

	int FindFirstAvailableQP(void);
	void ScanLostQueuePairs(void);
	void ScanNewMsg(void);				// one thread scanning new message from clients
//	void ScanNewMsgInterServer(void);	// one thread scanning new message inter servers
	void Destroy_A_QueuePair(int idx);
	void Refill_PreAllocated_QP_Pool(void);
	int Get_IO_Worker_Index_from_QP_Index(int idx_qp);
};

typedef struct	{
	SERVER_QUEUEPAIR *pServer_qp;
	int t_rank, nthread;
	int nToken;	// the token to make sure only one thread is executed. 
}PARAM_PREALLOCATE_QP;


/* Handle options that can be set on the command line.
   Currently that's just the fairness mode.
*/


class ServerOptions {
public:
	ServerOptions() : fairness_mode(getDefaultFairnessMode()) {}

	// Returns true iff the command line arguments are successfully parsed.
	// defined in put_get_server.cpp
	bool parseCommandLineArgs(int argc, char **argv);

	// Prints help message explaining command line usage.
	// defined in put_get_server.cpp
	static void printHelp();

	// FairnessMode is defined in qp.h
	FairnessMode getFairnessMode() {return fairness_mode;}

	// use this to change the default fairness mode
	static FairnessMode getDefaultFairnessMode() {return FIFO;};

	static const char *fairnessModeToString(FairnessMode fairness_mode) {
		static char szFairnessModeString[16][64]={"fifo", "size-fair", "job-fair", "user-fair", "user-size-fair", "user-job-fair", "group-user-size-fair"};

		return szFairnessModeString[(int)fairness_mode];
//		return fairness_mode == SIZE_FAIR ? "size-fair"
//			: fairness_mode == JOB_FAIR ? "job-fair"
//			: "user-fair";
	}

private:
	FairnessMode fairness_mode;
};


#endif

