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
//#include "dict.h"
#include "qp_common.h"
#include "io_queue.h"

//#define N_THREAD_PREALLOCATE_QP	(1)
#define N_THREAD_PREALLOCATE_QP	(16)
#define N_THREAD_ADD_PREALLOCATE_QP	(4)

#define NUM_QP_PREALLOCATED		(512)
#define NUM_QP_TRIGGER_PREALLOCATED		(256)	// a half of NUM_QP_PREALLOCATED

#define DEFAULT_REM_BUFF_SIZE	(4096)

#define MAX_NEW_MSG	(1024*16)

#define DEF_CQ_MOD	(100)
//#define QUEUE_SIZE	(128)
#define IB_QUEUE_SIZE	(1)
#define CTX_POLL_BATCH		(16)

extern int mpi_rank, nFSServer;	// rank and size of MPI
pthread_mutex_t lock_preallocate_qp;

//typedef void (*org_sighandler)(int sig, siginfo_t *siginfo, void *ptr);
//static org_sighandler org_segv=NULL, org_term=NULL, org_int=NULL;

QPAIR_EXCH_DATA qp_my_data, qp_pal_data;

typedef	struct	{
	int ready;
	unsigned int psn;
	struct ibv_qp *queue_pair;
	struct ibv_cq *send_complete_queue, *recv_complete_queue;
}QUEUE_PAIR_PREALLOCATED, *PQUEUE_PAIR_PREALLOCATED;

char Is_PreAllocated_QP_Ready[NUM_QP_PREALLOCATED];
int nQueuePairPreAllocated=0;
QUEUE_PAIR_PREALLOCATED List_of_QueuePair_PreAllocated[NUM_QP_PREALLOCATED];
pthread_t pthread_preallocate[N_THREAD_PREALLOCATE_QP];

extern IO_QUEUE IO_Queue_List[MAX_NUM_QUEUE];


void Init_PreAllocated_QueuePair_List(void);
void* Func_thread_PreAllocate_QueuePair(void *pParam);
void CreateQueuePair(struct ibv_context* context, struct ibv_pd* pd, struct ibv_cq **pSend_complete_queue, struct ibv_cq **pRecv_complete_queue, struct ibv_qp **pQueue_pair, unsigned int *pPsn);


typedef	struct	{
	struct ibv_qp *queue_pair;
	struct ibv_cq *send_complete_queue, *recv_complete_queue;

	int nPut_Get, nPut_Get_Done;
	int jobid, idx_queue;	// jobid and the index of queue that handles this jobid

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
}QP_DATA, *PQP_DATA;

typedef	struct	{
	unsigned char msg[256];	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! NEED changes!!!!!!!!!!!!!!!!!!!!
}IO_CMD_MSG, *PIO_CMD_MSG;

IB_MEM_DATA my_local_mem, my_remote_mem;
IB_MEM_DATA pal_local_mem, pal_remote_mem;

int nNewMsg, NewMsgList[MAX_NEW_MSG];


inline int Align64_Int(int a)
{
	return ( (a & 0x3F) ? (64 + (a & 0xFFFFFFC0) ) : (a) );
}

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
	pthread_mutex_t process_lock;	// for this process

//	CHASHTABLE_INT *p_Hash_socket_fd = NULL;
//	struct elt_Int *elt_list_socket_fd = NULL;
//	int *ht_table_socket_fd=NULL;

	QP_DATA *pQP_Data = NULL;
	void *p_shm_Global = NULL;	// NewMsgFlag[], Time_HeartBeat[], IO_Msg[]
	unsigned char *p_shm_NewMsgFlag = NULL;
	time_t *p_shm_TimeHeartBeat = NULL;
	IO_CMD_MSG *p_shm_IO_Cmd_Msg = NULL;

	struct ibv_device** dev_list_ = NULL;
	struct ibv_context* context_ = NULL;
	struct ibv_pd* pd_ = NULL;
	struct ibv_port_attr port_attr_ = {};
	struct ibv_mr *mr_rem, *mr_loc, *mr_shm_global = NULL;

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
	void ScanNewMsg(void);
	void Destroy_A_QueuePair(int idx);
	void Refill_PreAllocated_QP_Pool(void);
};

typedef struct	{
	SERVER_QUEUEPAIR *pServer_qp;
	int t_rank, nthread;
}PARAM_PREALLOCATE_QP;

PARAM_PREALLOCATE_QP pParam_PreAllocate[N_THREAD_PREALLOCATE_QP];

void* Func_thread_PreAllocate_QueuePair(void *pParam)
{
	PARAM_PREALLOCATE_QP *pParamPreAlloccate;
	SERVER_QUEUEPAIR *pServer_qp;
	QUEUE_PAIR_PREALLOCATED *pPreAllocateQP;
	int i, t_rank, nthread;	// thread rank
	
	pParamPreAlloccate = (PARAM_PREALLOCATE_QP *)pParam;
	pServer_qp = pParamPreAlloccate->pServer_qp;
	t_rank = pParamPreAlloccate->t_rank;
	nthread = pParamPreAlloccate->nthread;

	if(t_rank == 0)	{
		printf("DBG> nthread = %d\n", nthread);
	}

	for(i=t_rank; i<NUM_QP_PREALLOCATED; i+= nthread)	{
		pPreAllocateQP = &(List_of_QueuePair_PreAllocated[i]);

		pthread_mutex_lock(&lock_preallocate_qp);
		if(Is_PreAllocated_QP_Ready[i] == 0)	{
			pthread_mutex_unlock(&lock_preallocate_qp);

			CreateQueuePair(pServer_qp->context_, pServer_qp->pd_, &(pPreAllocateQP->send_complete_queue), &(pPreAllocateQP->recv_complete_queue), &(pPreAllocateQP->queue_pair), &(pPreAllocateQP->psn));
			pPreAllocateQP->ready = 1;
			pthread_mutex_lock(&lock_preallocate_qp);
			Is_PreAllocated_QP_Ready[i] = 1;
			nQueuePairPreAllocated++;
			pthread_mutex_unlock(&lock_preallocate_qp);
		}
		else	pthread_mutex_unlock(&lock_preallocate_qp);
	}

	return NULL;
}

void SERVER_QUEUEPAIR::Refill_PreAllocated_QP_Pool(void)
{
	int i;

	for(i=0; i<N_THREAD_PREALLOCATE_QP; i++)	{
//		pParam_PreAllocate[i].pServer_qp = pServer_qp;
//		pParam_PreAllocate[i].t_rank = i;
		pParam_PreAllocate[i].nthread = N_THREAD_ADD_PREALLOCATE_QP;	// smaller number of threads for refilling
		if(pthread_create(&(pthread_preallocate[i]), NULL, Func_thread_PreAllocate_QueuePair, &(pParam_PreAllocate[i]))) {
			fprintf(stderr, "Error creating thread\n");
			return;
		}
	}
}


void Init_PreAllocated_QueuePair_List(void)
{
	nQueuePairPreAllocated=0;
	memset(List_of_QueuePair_PreAllocated, 0, sizeof(QUEUE_PAIR_PREALLOCATED)*NUM_QP_PREALLOCATED);
	memset(Is_PreAllocated_QP_Ready, 0, sizeof(char)*NUM_QP_PREALLOCATED);

    if(pthread_mutex_init(&lock_preallocate_qp, NULL) != 0) { 
        printf("\n mutex lock_preallocate_qp init failed\n"); 
        exit(1);
    }
}

void SERVER_QUEUEPAIR::Destroy_A_QueuePair(int idx)
{
	int j, IdxLastQP_Save;
	
	p_shm_NewMsgFlag[idx] = 0;
	p_shm_TimeHeartBeat[idx] = 0;

    if (pQP_Data[idx].recv_complete_queue != NULL)
		ibv_destroy_cq(pQP_Data[idx].recv_complete_queue);
    if (pQP_Data[idx].send_complete_queue != NULL)
		ibv_destroy_cq(pQP_Data[idx].send_complete_queue);
	if (pQP_Data[idx].queue_pair)	{
		ibv_destroy_qp(pQP_Data[idx].queue_pair);
		pQP_Data[idx].queue_pair = NULL;
	}

	pthread_mutex_lock(&process_lock);
	// to update the first available entry!!!
	if(idx < FirstAV_QP)	{
		FirstAV_QP = idx;
	}
	nQP--;
	if(idx == IdxLastQP)	{	// Is removing the last record? To find the new last record. 
		IdxLastQP_Save = IdxLastQP;
		IdxLastQP = -1;

		for(j=IdxLastQP_Save-1; j>mpi_rank; j--)	{
			if(pQP_Data[j].queue_pair)	{	// a valid queue pair
				IdxLastQP = j;
				break;
			}
		}
		if(IdxLastQP == -1)	{
			for(j=mpi_rank-1; j>=0; j--)	{	// skip itself. mpi_rank
				if(pQP_Data[j].queue_pair)	{	// a valid queue pair
					IdxLastQP = j;
					break;
				}
			}
		}

		IdxLastQP64 = Align64_Int(IdxLastQP+1);	// +1 is needed since IdxLastQP is included!
	}
	pthread_mutex_unlock(&process_lock);
}

void SERVER_QUEUEPAIR::ScanLostQueuePairs(void)
{
	int j;
	struct timeval tm1;	// tm1.tv_sec

	gettimeofday(&tm1, NULL);

	for(j=nFSServer; j<=IdxLastQP; j++)	{	// skip the clients on other servers
//	for(j=0; j<=IdxLastQP; j++)	{
		if(p_shm_TimeHeartBeat[j])	{
			if( (tm1.tv_sec - p_shm_TimeHeartBeat[j]) > (T_FREQ_ALARM_HB + 3) )	{	// out of dated heart beat. Lost connection??? Destroy the queue pair. 
				Destroy_A_QueuePair(j);
				printf("Destroy queue pair %d due to lost connection.\n", j);
			}
		}
	}
}

SERVER_QUEUEPAIR::SERVER_QUEUEPAIR(void)
{
    if(pthread_mutex_init(&process_lock, NULL) != 0) { 
        printf("\n mutex process_lock init failed\n"); 
        exit(1);
    }
}

SERVER_QUEUEPAIR::~SERVER_QUEUEPAIR(void)
{
	pthread_mutex_destroy(&process_lock);
	pthread_mutex_destroy(&lock_preallocate_qp);
}

void SERVER_QUEUEPAIR::ScanNewMsg(void)	// scan all queue pairs for make a list of queue pairs with new msg
{
	int i, k, LastQPLocal;
	__m512i Data;
	unsigned long int cmpMask;

	nNewMsg = 0;
	if(p_shm_NewMsgFlag == NULL)	return;
	LastQPLocal = IdxLastQP + 1;

	if(IdxLastQP64 <=192)	{	// simple version
		for(i=0; i<LastQPLocal; i++)	{
			if(p_shm_NewMsgFlag[i])	{
				NewMsgList[nNewMsg] = i;
				printf("DBG> Rank = %d. Found new msg for qp %d.\n", mpi_rank, i);
				nNewMsg++;
				p_shm_NewMsgFlag[i] = 0;
			}
		}
	}
	else	{	// AVX512 version
        for(i=0; i< (IdxLastQP64-64); i+=64)        {
			Data = *( volatile __m512i *)(& p_shm_NewMsgFlag[i]);
			cmpMask = _mm512_movepi8_mask(Data);
			
			if ( cmpMask != 0 ) {
				for(k=0; k<64; k++)	{
					if(cmpMask & 1LL)	{
						NewMsgList[nNewMsg] = i + k;
						printf("DBG> Rank = %d. Found new msg for qp %d.\n", mpi_rank, i+k);
						nNewMsg++;
						p_shm_NewMsgFlag[i+k] = 0;
					}
					cmpMask = cmpMask >> 1;
				}
			}
		}
        for(; i< IdxLastQP64; i+=64)        {	// residue
			Data = *( volatile __m512i *)(& p_shm_NewMsgFlag[i]);
			cmpMask = _mm512_movepi8_mask(Data);
			
			if ( cmpMask != 0 ) {
				for(k=0; k<64; k++)	{
					if(cmpMask & 1LL)	{
						if( (i + k) >= LastQPLocal )	{	// reached the end
							break;
						}
						NewMsgList[nNewMsg] = i + k;
						printf("DBG> Rank = %d. Found new msg for qp %d.\n", mpi_rank, i+k);
						nNewMsg++;
						p_shm_NewMsgFlag[i+k] = 0;
					}
					cmpMask = cmpMask >> 1;
				}
			}
		}
	}
}

int SERVER_QUEUEPAIR::FindFirstAvailableQP(void)
{
	int i, idx = -1, Done=0;

	if(FirstAV_QP < 0)	{
		return FirstAV_QP;
	}
	idx = FirstAV_QP;
	FirstAV_QP = -1;

	for(i=idx+1; i<mpi_rank; i++)	{	// Need to exclude mpi_rank.
		if(pQP_Data[i].queue_pair == NULL)	{
			FirstAV_QP = i;
			Done = 1;
			break;
		}
	}

	if(! Done)	{
		for(i=mpi_rank+1; i<max_qp; i++)	{
			if(pQP_Data[i].queue_pair == NULL)	{
				FirstAV_QP = i;
				break;
			}
		}
	}
	if(FirstAV_QP < 0)	{
		printf("WARNING> All queue pairs are used.\n");
	}

	return idx;
}

void SERVER_QUEUEPAIR::Init_Server_Socket(int max_num_qp, int port)
{
	int i, nSizeofNewMsgFlag, nSizeofHeartBeat, nSizeofIOCmdMsg;

//	void *pHash;
	sock_addr = INADDR_ANY;
	sock_fd = -1;
	sock_signal_fd = -1;
	sock_epoll_fd = -1;
	sock_port = port;
	
	max_qp = max_num_qp;
//	pHash = malloc(CHASHTABLE_INT::GetStorageSize(max_num_qp*2));
//	p_Hash_socket_fd = (CHASHTABLE_INT *)pHash;
///	p_Hash_socket_fd->DictCreate(max_num_qp*2, &elt_list_socket_fd, &ht_table_socket_fd);	// init hash table
	
//	pQP_Data = (QP_DATA *)malloc(sizeof(QP_DATA) * max_num_qp * 2);	
	pQP_Data = (QP_DATA *)malloc(sizeof(QP_DATA) * max_num_qp);
	assert(pQP_Data != NULL);
	for(i=0; i<max_qp; i++)	{
		pQP_Data[i].queue_pair = NULL;
	}

	nSizeofNewMsgFlag = sizeof(char)*max_qp;
	nSizeofHeartBeat = sizeof(time_t)*max_qp;
	nSizeofIOCmdMsg = sizeof(IO_CMD_MSG)*max_qp;
	nSizeshm_Global = nSizeofNewMsgFlag + nSizeofHeartBeat + nSizeofIOCmdMsg;
	p_shm_Global = memalign( 4096, nSizeshm_Global);
	assert(p_shm_Global != NULL);
	memset(p_shm_Global, 0, nSizeshm_Global);
	p_shm_NewMsgFlag = (unsigned char *)p_shm_Global;
	p_shm_TimeHeartBeat = (time_t *)((char*)p_shm_Global + nSizeofNewMsgFlag);
	p_shm_IO_Cmd_Msg = (IO_CMD_MSG *)((char*)p_shm_Global + nSizeofNewMsgFlag + nSizeofHeartBeat);
	FirstAV_QP = 0;
	nQP = 0;
	IdxLastQP = -1;
	IdxLastQP64 = -1;
	mr_shm_global = IB_RegisterBuf_RW_Local_Remote(p_shm_Global, nSizeshm_Global);
	assert(mr_shm_global != NULL);

//	Init_Server_IB_Env(DEFAULT_REM_BUFF_SIZE);
}

int SERVER_QUEUEPAIR::Add_Epoll(int events, int fd)
{
	int rc;
	struct epoll_event ev;
	
	memset(&ev,0,sizeof(ev)); // placate valgrind
	ev.events = events;
	ev.data.fd= fd;
	printf("INFO> Adding fd %d to epoll\n", fd);
	rc = epoll_ctl(sock_epoll_fd, EPOLL_CTL_ADD, fd, &ev);
	if (rc == -1) {
		fprintf(stderr, "ERROR> epoll_ctl: %s\n", strerror(errno));
	}
	return rc;
}

int SERVER_QUEUEPAIR::Del_Epoll(int fd)
{
	int rc;
	struct epoll_event ev;
	rc = epoll_ctl(sock_epoll_fd, EPOLL_CTL_DEL, fd, &ev);
	if (rc == -1) {
		fprintf(stderr, "ERROR> epoll_ctl: %s\n", strerror(errno));
	}
	return rc;
}

int SERVER_QUEUEPAIR::Setup_Listener(void)
{
	int rc = -1, one=1;
	
	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd == -1) {
		fprintf(stderr, "ERROR> socket: %s\n", strerror(errno));
	}
	else	{
		// internet socket address structure: our address and port
		struct sockaddr_in sin;
		sin.sin_family = AF_INET;
		sin.sin_addr.s_addr = sock_addr;
		sin.sin_port = htons(sock_port);
		
		// bind socket to address and port 
		setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
		if (bind(fd, (struct sockaddr*)&sin, sizeof(sin)) == -1) {
			fprintf(stderr, "ERROR> bind: %s\n", strerror(errno));
		}
		else	{
			// put socket into listening state
			if (listen(fd,1) == -1) {
				fprintf(stderr, "listen: %s\n", strerror(errno));
			}
			else	{
				sock_fd = fd;
				rc=0;
			}
		}
	}
	
	if ((rc < 0) && (fd != -1)) close(fd);
	return rc;
}

void SERVER_QUEUEPAIR::Get_A_PreAllocated_QueuePair(int idx)
{
	int i;
	QP_DATA *pQP;
	QUEUE_PAIR_PREALLOCATED *pPreAllocatedQP;

	pQP = &(pQP_Data[idx]);

	pthread_mutex_lock(&lock_preallocate_qp);
	for(i=0; i<NUM_QP_PREALLOCATED; i++)	{
		if(Is_PreAllocated_QP_Ready[i])	{
			pPreAllocatedQP = &(List_of_QueuePair_PreAllocated[i]);
			pQP->send_complete_queue = pPreAllocatedQP->send_complete_queue;
			pQP->recv_complete_queue = pPreAllocatedQP->recv_complete_queue;
			pQP->queue_pair = pPreAllocatedQP->queue_pair;
			pQP->ib_my_psn = pPreAllocatedQP->psn;
			pQP->ib_my_lid = port_attr_.lid;
			pQP->ib_my_qpn = pQP->queue_pair->qp_num;
			Is_PreAllocated_QP_Ready[i] = 0;
			nQueuePairPreAllocated--;
			break;
		}
	}
	pthread_mutex_unlock(&lock_preallocate_qp);
	if(i >= NUM_QP_PREALLOCATED)	{	// no free queue pair available. Create one now!!!
		printf("Warning> No preallocated queue pair available. Create a new queue pair now!\n");
		IB_CreateQueuePair(idx);
	}
	else if(i>= NUM_QP_TRIGGER_PREALLOCATED)	{	// The number of available queue pair is running low. Need to preallocate more queuepairs!!!
		Refill_PreAllocated_QP_Pool();
	}
}

//extern int new_socket;
int SERVER_QUEUEPAIR::Accept_Client()
{
	int fd, nBytes, idx, idx_Queue;
	unsigned int *p_token;
	struct sockaddr_in in;
	socklen_t sz = sizeof(in);
	char szBuff[128];
	GLOBAL_ADDR_DATA Global_Addr_Data;
	JOB_INFO_DATA JobInfo;
	
	fd = accept(sock_fd,(struct sockaddr*)&in, &sz);
	if (fd == -1) {
		printf("INFO> accept: %s\n", strerror(errno)); 
	}
	else	{
		if (sizeof(in) == sz) {
			printf("INFO> connection fd %d from %s:%d\n", fd, inet_ntoa(in.sin_addr), (int)ntohs(in.sin_port));
		}
		
		p_token = (unsigned int*)szBuff;
		nBytes = read(fd, szBuff, 4*sizeof(int));
		assert(p_token[0] == TAG_EXCH_QP_INFO);
		if( nBytes != 16 )	{	// 4 int
			printf("Error> nBytes = %d. Unexpected value.\n", nBytes);
		}
		
		nConnectionAccu++;
//		idx = p_Hash_socket_fd->DictInsertAuto(fd, &elt_list_socket_fd, &ht_table_socket_fd);
		pthread_mutex_lock(&process_lock);
		idx = FindFirstAvailableQP();
		assert(idx >= 0);
		nQP++;
//		printf("DBG> QP is allocated at %d. nQP = %d\n", idx, nQP);
		if(IdxLastQP < idx)	{
			IdxLastQP = idx;
			IdxLastQP64 = Align64_Int(IdxLastQP+1);	// +1 is needed since IdxLastQP is included!
		}
		pthread_mutex_unlock(&process_lock);

		Get_A_PreAllocated_QueuePair(idx);
//		IB_CreateQueuePair(idx);	// DBG> IB_CreateQueuePair() 3345 us. Slow process! We can do pre-allocation to save time. When the number of pre-allocated QP is 
		// Get a queue pair from preallocated list of qps

		// not large enough, start a thread and do pre-allocation! 
		pQP_Data[idx].ib_pal_lid = p_token[1];
		pQP_Data[idx].ib_pal_qpn = p_token[2];
		pQP_Data[idx].ib_pal_psn = p_token[3];

		pQP_Data[idx].nPut_Get = 0;
		pQP_Data[idx].nPut_Get_Done = 0;
//		printf("INFO> Inserted a hash entry at %d\n", idx);
//		printf(" Pal token (%d, %d, %d) My token (%d, %d, %d)\n", p_token[1], p_token[2], p_token[3], pQP_Data[idx].ib_my_lid, pQP_Data[idx].ib_my_qpn, pQP_Data[idx].ib_my_psn);
		
		pQP_Data[idx].tag_ib_me = TAG_EXCH_QP_INFO;
		write(fd, &(pQP_Data[idx].tag_ib_me), 4*sizeof(int));
//		write(fd, &(pQP_Data[idx].ib_my_lid), 4*sizeof(int));
		// DBG> IB_Modify_QP() 1273 us
		IB_Modify_QP(pQP_Data[idx].queue_pair, pQP_Data[idx].ib_my_psn, (uint16_t)(pQP_Data[idx].ib_pal_lid), pQP_Data[idx].ib_pal_qpn, pQP_Data[idx].ib_pal_psn);

		nBytes = read(fd, &(pQP_Data[idx].tag_mem), 2*sizeof(int)+sizeof(long int));
//		nBytes = read(fd, &(pQP_Data[idx].rem_addr), 2*sizeof(int)+sizeof(long int));
		if( nBytes != (2*sizeof(int)+sizeof(long int)) )	{	// 4 int
			printf("Error> nBytes = %d. Unexpected value.\n", nBytes);
		}
		assert(pQP_Data[idx].tag_mem == TAG_EXCH_MEM_INFO);
		
//		mr_rem = IB_RegisterBuf_RW_Local_Remote(rem_buff, rem_buff_size);
//		rem_buff = mr_rem->addr;	// we know that address
//		rem_key = mr_rem->rkey;
		rem_buff = (unsigned char*)mr_shm_global->addr;
		rem_key = mr_shm_global->rkey;
		rem_buff_size = nSizeshm_Global;

//		printf("DBG> rem_buff_size = %d rem_buff at %p\n", rem_buff_size, rem_buff);
//		printf("INFO> Mine (%lx, %d) Remote (%lx, %d)\n", mr_rem->addr, mr_rem->rkey, pQP_Data[idx].rem_addr, pQP_Data[idx].rem_key);
//		printf("INFO> Mine (%lx, %d) Remote (%lx, %d)\n", mr_shm_global->addr, mr_shm_global->rkey, pQP_Data[idx].rem_addr, pQP_Data[idx].rem_key);

		tag_mem = TAG_EXCH_MEM_INFO;
//		write(fd, &rem_buff, 2*sizeof(int)+sizeof(long int));
		write(fd, &tag_mem, 2*sizeof(int)+sizeof(long int));

		read(fd, &JobInfo, sizeof(JOB_INFO_DATA));
		assert(JobInfo.comm_tag == TAG_SUBMIT_JOB_INFO);
		printf("INFO> jobid = %d nnode = %d\n", JobInfo.jobid, JobInfo.nnode);
		idx_Queue = Query_Jobid_In_Queue(JobInfo.jobid);
		if(idx_Queue > 0)	{	// a queue created already
			IO_Queue_List[idx_Queue].nQP ++;
		}
		else	{
			idx_Queue = Create_A_Queue(JobInfo.jobid);
			IO_Queue_List[idx_Queue].nnode = JobInfo.nnode;
		}
		pQP_Data[idx].idx_queue = idx_Queue;

		Global_Addr_Data.comm_tag = TAG_GLOBAL_ADDR_INFO;
		Global_Addr_Data.addr_NewMsgFlag = (uint64_t)p_shm_NewMsgFlag + sizeof(char)*idx;
		Global_Addr_Data.addr_TimeHeartBeat = (uint64_t)p_shm_TimeHeartBeat + sizeof(time_t)*idx;
		Global_Addr_Data.addr_IO_Cmd_Msg = (uint64_t)p_shm_IO_Cmd_Msg + sizeof(IO_CMD_MSG)*idx;
		write(fd, &Global_Addr_Data, sizeof(GLOBAL_ADDR_DATA));

		int tag;
		read(fd, &tag, sizeof(int));
		if(tag == TAG_DONE)	{
			write(fd, &tag, sizeof(int));
			printf("INFO> Socket will be closed soon.\n");
		}
		else	{
			printf("ERROR> Unexpected value = %x\n", tag);
		}

		if (Add_Epoll(EPOLLIN, fd) == -1) {
			close(fd);
			fd = -1;
		}
	}
//	new_socket = fd;

	return fd;
}

void SERVER_QUEUEPAIR::Drain_Client(const int fd)
{
	int rc;
	char buf[1024];
	
	rc = read(fd, buf, 128);
	switch(rc) { 
	default: printf("INFO> received %d bytes\n", rc);         break;
	case  0: printf("INFO> fd %d closed\n", fd);              break;
	case -1: printf("INFO> recv: %s\n", strerror(errno));    break;
	}
	
	if (rc != 0) return;
	
	// client closed. log it, tell epoll to forget it, close it.
	printf("INFO> client %d has closed\n", fd);
	Del_Epoll(fd);
	close(fd);
	
//	idx = p_Hash_socket_fd->DictSearchOrg(fd, &elt_list_socket_fd, &ht_table_socket_fd);
//	if(idx >= 0)	{
///		p_Hash_socket_fd->DictDelete(fd, &elt_list_socket_fd, &ht_table_socket_fd);	// !!!!!!!!!!!!!!!!!!!!!!!!!! only for test!!!!
//		Del_Epoll(fd);
//		close(fd);
//	}
//	else	{
//		printf("Error: failed to find fd %d from hash table.\n", fd);
//	}
}

void SERVER_QUEUEPAIR::Socket_Server_Loop(void)
{
	int i, epoll_ret;
	struct epoll_event ev;
	struct signalfd_siginfo info;
	
	// block all signals. we take signals synchronously via signalfd
	sigset_t all;
	// signals that we'll accept synchronously via signalfd */
	int sigs[] = {SIGIO,SIGHUP,SIGTERM,SIGINT,SIGQUIT,SIGALRM};
	
	sigfillset(&all);
	sigprocmask(SIG_SETMASK,&all,NULL);
	
	// a few signals we'll accept via our signalfd
	sigset_t sw;
	sigemptyset(&sw);
	for(i=0; i < sizeof(sigs)/sizeof(*sigs); i++) sigaddset(&sw, sigs[i]);
	
	if (Setup_Listener()) goto done;
	
	/* create the signalfd for receiving signals */
	sock_signal_fd = signalfd(-1, &sw, 0);
	if (sock_signal_fd == -1) {
		fprintf(stderr,"signalfd: %s\n", strerror(errno));
		goto done;
	}
	
	// set up the epoll instance
	sock_epoll_fd = epoll_create(1); 
	if (sock_epoll_fd == -1) {
		fprintf(stderr,"epoll: %s\n", strerror(errno));
		goto done;
	}
	
	// add descriptors of interest
	if (Add_Epoll(EPOLLIN, sock_fd))        goto done; // listening socket
	if (Add_Epoll(EPOLLIN, sock_signal_fd)) goto done; // signal socket
	
	while ( 1 ) {
		epoll_ret = epoll_wait(sock_epoll_fd, &ev, 1, -1);
		if(epoll_ret <= 0)	{
			if(errno == EINTR)	continue;
			else break;
		}

		// if a signal was sent to us, read its signalfd_siginfo
		if (ev.data.fd == sock_signal_fd) { 
			if (read(sock_signal_fd, &info, sizeof(info)) != sizeof(info)) {
				fprintf(stderr,"ERROR> failed to read signal fd buffer\n");
				continue;
			}
			else if(info.ssi_signo == SIGTERM)	{
				//				write(STDERR_FILENO,"Got signal %d (SIGTERM)\n", info.ssi_signo);
			}
			else	{
				write(STDERR_FILENO,"Got signal %d\n", info.ssi_signo);  
			}
			goto done;
		}
		
		/* regular POLLIN. handle the particular descriptor that's ready */
		assert(ev.events & EPOLLIN);
		fprintf(stderr,"INFO> handle POLLIN on fd %d\n", ev.data.fd);
		if (ev.data.fd == sock_fd) Accept_Client();
		else Drain_Client(ev.data.fd);
	}
	
	fprintf(stderr, "epoll_wait: %s\n", strerror(errno));
	
done:   /* we get here if we got a signal like Ctrl-C */
/*
	for(i=0; i<p_Hash_socket_fd->size; i++)	{
		if(elt_list_socket_fd[i].key >= 0)	{
			close(elt_list_socket_fd[i].key);
			Del_Epoll(elt_list_socket_fd[i].key);
		}
	}
	if(p_Hash_socket_fd)	{
		free((void*)p_Hash_socket_fd);
		p_Hash_socket_fd = NULL;
	}
*/
//	if(pQP_Data)	{
//		free(pQP_Data);
//		pQP_Data = NULL;
//	}
	
	if (sock_epoll_fd != -1) close(sock_epoll_fd);
	if (sock_signal_fd != -1) close(sock_signal_fd);
}

void SERVER_QUEUEPAIR::Init_Server_IB_Env(int remote_buff_size)
{
	int ret;
	int devices;
	
	dev_list_ = ibv_get_device_list(&devices);
	
	if (!dev_list_) {
		int errno_backup = errno;
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_get_device_list (errno=%d).\n", __FILE__, __LINE__, errno_backup);
		exit(1);
	}
	
	for (int i = 0; i < devices; i++) {
		ibv_device* device = dev_list_[i];
		
		if (!device) {
			continue;
		}
		
		context_ = ibv_open_device(device);
		
		if (!context_) {
			continue;
		}
	}
	
	struct ibv_device_attr device_attr;
	
	if (!context_) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: No HCA can use.\n", __FILE__, __LINE__);
		exit(1);
	}
	
	ret = ibv_query_device(context_, &device_attr);
	printf("max_qp = %d max_qp_wr = %d\n", device_attr.max_qp, device_attr.max_qp_wr);
	
	ret = ibv_query_port(context_, 1, &port_attr_);
	
	if (ret != 0 || port_attr_.lid == 0) {
		// error handling
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_query_port.\n", __FILE__, __LINE__);
		exit(1);
	}
	
	pd_ = ibv_alloc_pd(context_);
	
	if (!pd_) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_alloc_pd.\n", __FILE__, __LINE__);
		exit(1);
	}

//	rem_buff_size = remote_buff_size;
//	rem_buff = (unsigned char *)malloc(rem_buff_size);
//	if(rem_buff == NULL)	{
//		fprintf(stderr, "ERROR> Error to allocate %d bytes for rem_buff.\n");
//		exit(1);
//	}
}

void SERVER_QUEUEPAIR::Clean_IB_Env(void)
{
	// release queues
//	if (queue_pair)
//		ibv_destroy_qp(queue_pair);
//    if (recv_complete_queue != NULL)
//		ibv_destroy_cq(recv_complete_queue);
//    if (send_complete_queue != NULL)
//		ibv_destroy_cq(send_complete_queue);
	
	// release memory region which is nonblocking-io but not freed.
//	ibv_dereg_mr(mr_rem);
//	ibv_dereg_mr(mr_loc);
	
	if (pd_ != NULL) ibv_dealloc_pd(pd_);
	if (context_ != NULL)	ibv_close_device(context_);
	if (dev_list_ != NULL)	ibv_free_device_list(dev_list_);
}

void SERVER_QUEUEPAIR::IB_Modify_QP(struct ibv_qp* qp, uint32_t src_psn, uint16_t dest_lid, uint32_t dest_pqn, uint32_t dest_psn)
{
  int ret;

  struct ibv_qp_attr init_attr = {};
  init_attr.qp_state = IBV_QPS_INIT;
  init_attr.port_num = 1;
//  init_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE;
  init_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;

  ret = ibv_modify_qp(
      qp, &init_attr,
      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
  if (ret != 0) {
    fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_modify_qp(1).\n", __FILE__, __LINE__);
    exit(1);
  }

  struct ibv_qp_attr rtr_attr = {};
  rtr_attr.qp_state = IBV_QPS_RTR;
  rtr_attr.path_mtu = IBV_MTU_4096;
  rtr_attr.dest_qp_num = dest_pqn;
  rtr_attr.rq_psn = dest_psn;
  rtr_attr.max_dest_rd_atomic = 16;	// Ref to ctx_set_out_reads() in simple.c!!!!!!!!!!!!!!!!

  // retry_speed faster
  rtr_attr.min_rnr_timer = 12;	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

  // retry_speed slower
  // rtr_attr.min_rnr_timer = 0;

  rtr_attr.ah_attr.is_global = 0;
  rtr_attr.ah_attr.dlid = dest_lid;
  rtr_attr.ah_attr.sl = 0;
  rtr_attr.ah_attr.src_path_bits = 0;
  rtr_attr.ah_attr.port_num = 1;

  ret = ibv_modify_qp(qp, &rtr_attr,
                      IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                          IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                          IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
  if (ret != 0) {
    fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_modify_qp(2).\n", __FILE__, __LINE__);
    exit(1);
  }

  struct ibv_qp_attr rts_attr = {};
  rts_attr.qp_state = IBV_QPS_RTS;
  rts_attr.path_mtu = IBV_MTU_4096;	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  rts_attr.timeout = 14;		// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  rts_attr.retry_cnt = 7;
  rts_attr.rnr_retry = 7;
  rts_attr.sq_psn = src_psn;
  rts_attr.max_rd_atomic = 16;	// Ref to ctx_set_out_reads() in simple.c!!!!!!!!!!!!!!!!
  rts_attr.ah_attr.dlid = dest_lid; // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

  ret = ibv_modify_qp(qp, &rts_attr,
                      IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                          IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                          IBV_QP_MAX_QP_RD_ATOMIC);
  if (ret != 0) {
    fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_modify_qp(3).\n", __FILE__, __LINE__);
    exit(1);
  }
}

void CreateQueuePair(struct ibv_context* context, struct ibv_pd* pd, struct ibv_cq **pSend_complete_queue, struct ibv_cq **pRecv_complete_queue, struct ibv_qp **pQueue_pair, unsigned int *pPsn)
{
	*pSend_complete_queue = ibv_create_cq(context, IB_QUEUE_SIZE, NULL, NULL, 0);
	*pRecv_complete_queue = ibv_create_cq(context, IB_QUEUE_SIZE, NULL, NULL, 0);
	
	if (!pSend_complete_queue) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_create_cq of send cq.\n", __FILE__, __LINE__);
		exit(1);
	}
	
	if (!pRecv_complete_queue) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_create_cq of recv cq.\n", __FILE__, __LINE__);
		exit(1);
	}
	
	*pPsn = random() % 0xFFFFFF;
	
	struct ibv_qp_init_attr qp_init_attr = {};
	qp_init_attr.qp_type = IBV_QPT_RC;
	qp_init_attr.send_cq = *pSend_complete_queue;
	qp_init_attr.recv_cq = *pRecv_complete_queue;
	
	qp_init_attr.cap.max_send_wr = IB_QUEUE_SIZE;
	qp_init_attr.cap.max_recv_wr = 8192;
	qp_init_attr.cap.max_send_sge = 1;
	qp_init_attr.cap.max_recv_sge = 1;
	qp_init_attr.sq_sig_all = 1;
	//  qp_init_attr.sq_sig_all = 0;	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	//  qp_init_attr.sq_sig_all = 1;	// 0 - In every Work Request submitted to the Send Queue, the user must decide whether to generate a Work Completion for successful 
	//     completions or not
	// 1 - All Work Requests that will be submitted to the Send Queue will always generate a Work Completion !!!!!!!!!!
	qp_init_attr.cap.max_inline_data = 0;
	
	*pQueue_pair = ibv_create_qp(pd, &qp_init_attr);
	
	if (!pQueue_pair) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_create_qp().\n", __FILE__, __LINE__);
		exit(1);
	}
}

void SERVER_QUEUEPAIR::IB_CreateQueuePair(int idx)
{
	QP_DATA *pQP;
	
	pQP = &(pQP_Data[idx]);
	
	CreateQueuePair(context_, pd_, &(pQP->send_complete_queue), &(pQP->recv_complete_queue), &(pQP->queue_pair), &(pQP->ib_my_psn));
	
	pQP->ib_my_lid = port_attr_.lid;
	pQP->ib_my_qpn = pQP->queue_pair->qp_num;
}

struct ibv_mr* SERVER_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote(void* buf, size_t len)
{
  struct ibv_mr* mr_buf = ibv_reg_mr(pd_, buf, len, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
  if (mr_buf == 0) {
      fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_reg_mr on RW_Local_Remote.\n", __FILE__, __LINE__);
      exit(1);
  }

  return mr_buf;
}

void SERVER_QUEUEPAIR::IB_Put(int idx, void* loc_buf, uint32_t lkey, void* rem_buf, uint32_t rkey, size_t len)
{
	int ne, ret;
	struct ibv_sge sge = {};
	sge.addr = (uint64_t)(uintptr_t)loc_buf;
	sge.length = len;
	sge.lkey = lkey;
	
	struct ibv_send_wr write_wr = {};
	struct ibv_wc wc = {};

	memset(&write_wr, 0, sizeof(struct ibv_send_wr));
	write_wr.wr_id = 0;
	write_wr.sg_list = &sge;
	write_wr.num_sge = 1;
	write_wr.opcode = IBV_WR_RDMA_WRITE;
	write_wr.wr.rdma.rkey = rkey;
	write_wr.wr.rdma.remote_addr = (uint64_t)rem_buf;

	write_wr.send_flags = 2;
	struct ibv_send_wr* bad_wr;
	ret = ibv_post_send(pQP_Data[idx].queue_pair, &write_wr, &bad_wr);
	if (ret != 0) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_post_send in Put(). ret = %d\n", __FILE__, __LINE__, ret);
		exit(1);
	}

	pQP_Data[idx].nPut_Get++;

	if ( (pQP_Data[idx].nPut_Get%DEF_CQ_MOD) == (DEF_CQ_MOD - 1)) {
		write_wr.send_flags |= IBV_SEND_SIGNALED;
	}

	if( (pQP_Data[idx].nPut_Get - pQP_Data[idx].nPut_Get_Done) >= IB_QUEUE_SIZE ) {
		memset(&wc, 0, sizeof(struct ibv_wc));

		while(1)	{
			ne = ibv_poll_cq(pQP_Data[idx].send_complete_queue, CTX_POLL_BATCH, &wc);
			if (ne == 1) {
				pQP_Data[idx].nPut_Get_Done +=1;
//				nPut_Done += DEF_CQ_MOD;
				break;
			}
			else if(ne == 0)	{	// equal 0. Message not sent out yet!!!!!!!!!!!!!
			}
			else if (ne < 0) {
				fprintf(stderr, "poll CQ failed %d\n",ne);
				exit(1);
				return;
			}
			else	{
				fprintf(stderr, "ibv_poll_cq() return code is %d\n",ne);
				exit(1);
				return;
			}
		}
	}
}

void SERVER_QUEUEPAIR::IB_Get(int idx, void* loc_buf, uint32_t lkey, void* rem_buf, uint32_t rkey, size_t len)
{
	int ne, ret;

	struct ibv_sge sge = {};
	sge.addr = (uint64_t)(uintptr_t)loc_buf;
	sge.length = len;
	sge.lkey = lkey;
	
	struct ibv_send_wr write_wr = {};
	struct ibv_wc wc[CTX_POLL_BATCH];

	memset(&write_wr, 0, sizeof(struct ibv_send_wr));
	write_wr.wr_id = 0;
	write_wr.sg_list = &sge;
	write_wr.num_sge = 1;
	write_wr.opcode = IBV_WR_RDMA_READ;
	write_wr.wr.rdma.rkey = rkey;
	write_wr.wr.rdma.remote_addr = (uint64_t)rem_buf;

//	if ( nPut_Get_Done % DEF_CQ_MOD == 0 ) {
//		write_wr.send_flags = 0;
//	}
//	if ( (nPut_Get % DEF_CQ_MOD) == (DEF_CQ_MOD - 1)) {
		write_wr.send_flags = 2;
//	}

	struct ibv_send_wr* bad_wr;
	ret = ibv_post_send(pQP_Data[idx].queue_pair, &write_wr, &bad_wr);
	if (ret != 0) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_post_send in Get(). ret = %d\n", __FILE__, __LINE__, ret);
		exit(1);
	}

	pQP_Data[idx].nPut_Get++;

	if ( (pQP_Data[idx].nPut_Get%DEF_CQ_MOD) == (DEF_CQ_MOD - 1)) {
		write_wr.send_flags |= IBV_SEND_SIGNALED;
	}

	if( (pQP_Data[idx].nPut_Get - pQP_Data[idx].nPut_Get_Done) >= IB_QUEUE_SIZE ) {
		memset(wc, 0, sizeof(struct ibv_wc)*CTX_POLL_BATCH);

		while(1)	{
			ne = ibv_poll_cq(pQP_Data[idx].send_complete_queue, CTX_POLL_BATCH, wc);
			if (ne == 1) {
//				pQP_Data[idx].nPut_Get_Done += DEF_CQ_MOD;
				pQP_Data[idx].nPut_Get_Done += 1;
				break;
				
			}
			else if(ne == 0)	{	// equal 0. Message not sent out yet!!!!!!!!!!!!!
			}
			else if (ne < 0) {
				fprintf(stderr, "poll CQ failed %d\n",ne);
				exit(1);
				return;
			}
			else	{
				fprintf(stderr, "ibv_poll_cq() return code is %d\n",ne);
				exit(1);
				return;
			}

		}
	}

}
/*
class	FS_CLIENT_QUEUEPAIR	{	// the clients for other file servers
private:
	struct ibv_cq *send_complete_queue = NULL;
	struct ibv_cq *recv_complete_queue = NULL;
	
	QPAIR_EXCH_DATA qp_my_data, qp_pal_data;
	IB_MEM_DATA my_remote_mem;
	
	uint64_t nPut=0, nPut_Done=0;
	uint64_t nGet=0, nGet_Done=0;
	pthread_mutex_t qp_put_get_lock;
	int sock = 0;
	
	void Init_IB_Env(void);
	void IB_modify_qp(void);
	void IB_CreateQueuePair(void);
	void Setup_Socket(char szServerIP[]);
	void Try_Closing_Socket(void);
	
public:
	IB_MEM_DATA pal_remote_mem;
	struct ibv_mr *mr_rem = NULL, *mr_loc = NULL;
	struct ibv_mr *mr_loc_qp_Obj = NULL;
	struct ibv_qp *queue_pair = NULL;
//	time_t qp_heart_beat_t = 0;
	uint64_t remote_addr_new_msg;	// the address of remote buffer to notify a new message
	uint64_t remote_addr_heart_beat;	// the address of remote buffer to write heart beat time info.
	uint64_t remote_addr_IO_CMD;	// the address of remote buffer to IO requests.
//	int qp_put_get_locked;
//	int tid = 0;
	int Idx_fs = -1;
	
	void Close_QueuePair(void);
	void Setup_QueuePair(int IdxServer, char loc_buff[], size_t size_loc_buff, char rem_buff[], size_t size_rem_buff);
	struct ibv_mr* IB_RegisterBuf_RW_Local_Remote(void* buf, size_t len);
	int IB_Put(void* loc_buf, uint32_t lkey, void* rem_buf, uint32_t rkey, size_t len);
	int IB_Get(void* loc_buf, uint32_t lkey, void* rem_buf, uint32_t rkey, size_t len);
};
*/

/*
static void Finalize_Server(int argc, void *param)
{

}

static void sigsegv_handler(int sig, siginfo_t *siginfo, void *uc)
{
	char szMsg[256];
	Finalize_Server(0, NULL);
	sprintf(szMsg, "Got signal %d (SIGSEGV)\n", siginfo->si_signo);
	write(STDERR_FILENO, szMsg, strlen(szMsg));
	if(org_segv)	org_segv(sig, siginfo, uc);
	else	exit(1);
}

static void sigterm_handler(int sig, siginfo_t *siginfo, void *uc)
{
	char szMsg[256];
	//	if(Client_qp.queue_pair)	Client_qp.Close_QueuePair();
	Finalize_Server(0, NULL);
	sprintf(szMsg, "Got signal %d (SIGTERM)\n", siginfo->si_signo);
	write(STDERR_FILENO, szMsg, strlen(szMsg));
	if(org_term)	org_term(sig, siginfo, uc);
	else	exit(0);
}

static void sigint_handler(int sig, siginfo_t *siginfo, void *uc)
{
	char szMsg[256];
	//	if(Client_qp.queue_pair)	Client_qp.Close_QueuePair();
	Finalize_Server(0, NULL);
	sprintf(szMsg, "Got signal %d (SIGINT)\n", siginfo->si_signo);
	write(STDERR_FILENO, szMsg, strlen(szMsg));
	if(org_int)	org_int(sig, siginfo, uc);
	else	exit(0);
}

static void Setup_Signal_QueuePair(void)
{
	struct sigaction act, old_action;
	
    // Set up sigsegv handler
    memset (&act, 0, sizeof(act));
    act.sa_flags = SA_SIGINFO;
	
    act.sa_sigaction = sigsegv_handler;
    if (sigaction(SIGSEGV, &act, &old_action) == -1) {
        perror("Error: sigaction");
        exit(1);
    }
	if( (old_action.sa_handler != SIG_DFL) && (old_action.sa_handler != SIG_IGN) )	{
		org_segv = old_action.sa_sigaction;
	}
	
    act.sa_sigaction = sigterm_handler;
    if (sigaction(SIGTERM, &act, 0) == -1) {
        perror("Error: sigaction");
        exit(1);
    }
	if( (old_action.sa_handler != SIG_DFL) && (old_action.sa_handler != SIG_IGN) )	{
		org_term = old_action.sa_sigaction;
	}
	
    act.sa_sigaction = sigint_handler;
    if (sigaction(SIGINT, &act, 0) == -1) {
        perror("Error: sigaction");
        exit(1);
    }
	if( (old_action.sa_handler != SIG_DFL) && (old_action.sa_handler != SIG_IGN) )	{
		org_int = old_action.sa_sigaction;
	}
	
	on_exit(Finalize_Server, NULL);
}
*/
#endif

