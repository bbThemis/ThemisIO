#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>


#include "qp.h"
#include "myfs.h"
#include "dict.h"
#include "io_queue.h"
#include "utility.h"
#include "xxhash.h"
#include "corebinding.h"
#include "unique_thread.h"
#include "ncx_slab.h"

long int nSizeReg=0;

extern CORE_BINDING CoreBinding;
extern pthread_attr_t thread_attr;
extern CCreatedUniqueThread Unique_Thread;
extern char *p_CallReturnBuff;
extern ncx_slab_pool_t *sp_CallReturnBuff;

extern int mpi_rank, nFSServer;	// rank and size of MPI
extern CHASHTABLE_INT *pHT_ActiveJobs;
extern struct elt_Int *elt_list_ActiveJobs;
extern int *ht_table_ActiveJobs;
extern int nActiveJob;
extern JOBREC ActiveJobList[MAX_NUM_ACTIVE_JOB];

pthread_mutex_t lock_preallocate_qp;
pthread_mutex_t lock_Modify_ActiveJob_List;

//typedef void (*org_sighandler)(int sig, siginfo_t *siginfo, void *ptr);
//static org_sighandler org_segv=NULL, org_term=NULL, org_int=NULL;

QPAIR_EXCH_DATA qp_my_data, qp_pal_data;

char Is_PreAllocated_QP_Ready[NUM_QP_PREALLOCATED];
int nQueuePairPreAllocated=0;
QUEUE_PAIR_PREALLOCATED List_of_QueuePair_PreAllocated[NUM_QP_PREALLOCATED];
pthread_t pthread_preallocate[N_THREAD_PREALLOCATE_QP];

pthread_t pthread_IO_Worker[NUM_THREAD_IO_WORKER];


IB_MEM_DATA my_local_mem, my_remote_mem;
IB_MEM_DATA pal_local_mem, pal_remote_mem;

int nNewMsg, NewMsgList[MAX_NEW_MSG];

PARAM_PREALLOCATE_QP pParam_PreAllocate[N_THREAD_PREALLOCATE_QP];

int SERVER_QUEUEPAIR::Get_IO_Worker_Index_from_QP_Index(int idx_qp)
{
	int nNumQueuePerWorker, idx_Queue, nQP_InterServer;

	nQP_InterServer = nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER;
	if(idx_qp < nQP_InterServer)	return (idx_qp % NUM_THREAD_IO_WORKER_INTER_SERVER);

//	idx_Queue = ( ((idx_qp-NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER + (idx_qp-NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER/MAX_NUM_QUEUE_M1 ) % MAX_NUM_QUEUE_M1) + NUM_THREAD_IO_WORKER_INTER_SERVER;
//	idx_Queue = ( ((idx_qp-nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER + (idx_qp-nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER/MAX_NUM_QUEUE_MX ) % MAX_NUM_QUEUE_MX) + nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER;
	idx_Queue = ( ((idx_qp - nQP_InterServer)*NUM_QUEUE_PER_WORKER + (idx_qp - nQP_InterServer)*NUM_QUEUE_PER_WORKER/MAX_NUM_QUEUE_MX ) % MAX_NUM_QUEUE_MX) + NUM_THREAD_IO_WORKER_INTER_SERVER;

	if( ( ( MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER ) % ( NUM_THREAD_IO_WORKER - NUM_THREAD_IO_WORKER_INTER_SERVER ) ) == 0 )	{
		nNumQueuePerWorker = ( MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER ) / ( NUM_THREAD_IO_WORKER - NUM_THREAD_IO_WORKER_INTER_SERVER ) ;
	}
	else	{
		nNumQueuePerWorker = ( MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER ) / ( NUM_THREAD_IO_WORKER - NUM_THREAD_IO_WORKER_INTER_SERVER ) + 1;
	}

//	if(idx_qp < nQP_InterServer)	{	// inter-server qp
//		return pQP_Data[idx_qp].idx_queue;
//	}
//	else	{
		return (( (idx_Queue-NUM_THREAD_IO_WORKER_INTER_SERVER)/nNumQueuePerWorker ) + NUM_THREAD_IO_WORKER_INTER_SERVER);
//	}
}

void* Func_thread_PreAllocate_QueuePair(void *pParam)
{
	PARAM_PREALLOCATE_QP *pParamPreAlloccate;
	SERVER_QUEUEPAIR *pServer_qp;
	QUEUE_PAIR_PREALLOCATED *pPreAllocateQP;
	int i, t_rank, nthread;	// thread rank
	
	pParamPreAlloccate = (PARAM_PREALLOCATE_QP *)pParam;
	if(Unique_Thread.Redeem_A_Token(pParamPreAlloccate->nToken) == 0)	return NULL;

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
		pParam_PreAllocate[i].nToken = Unique_Thread.Apply_A_Token();
		if(pthread_create(&(pthread_preallocate[i]), &thread_attr, Func_thread_PreAllocate_QueuePair, &(pParam_PreAllocate[i]))) {
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
	int j, IdxLastQP_Save, nQP_Reserved_M1;

//	printf("DBG> Just destroyed %d QP. Client hostname %s ExeName = %s tid = %d nQP = %d FirstAV_QP = %d IdxLastQP = %d\n", 
//		idx, pQP_Data[idx].szClientHostName, pQP_Data[idx].szClientExeName, pQP_Data[idx].ctid, nQP, FirstAV_QP, IdxLastQP);
	
	nQP_Reserved_M1 = nFSServer * NUM_THREAD_IO_WORKER_INTER_SERVER - 1;
	pthread_mutex_lock(&process_lock);

	p_shm_NewMsgFlag[idx] = 0;
	p_shm_TimeHeartBeat[idx] = 0;

//    if (pQP_Data[idx].recv_complete_queue != NULL)
//		ibv_destroy_cq(pQP_Data[idx].recv_complete_queue);
//    if (pQP_Data[idx].send_complete_queue != NULL)
//		ibv_destroy_cq(pQP_Data[idx].send_complete_queue);

    if (pQP_Data[idx].recv_complete_queue != NULL)	{
		pQP_Data[idx].send_complete_queue = NULL;
		pQP_Data[idx].recv_complete_queue = NULL;
    }

	if (pQP_Data[idx].queue_pair)	{
		ibv_destroy_qp(pQP_Data[idx].queue_pair);
		pQP_Data[idx].queue_pair = NULL;
	}
	else	{
		printf("ERROR> Unexpected!\n");
	}

	fetch_and_add(&(ActiveJobList[pQP_Data[idx].idx_JobRec].nQP), -1);	// Decrese the counter by 1
	pQP_Data[idx].bServerReady = 0;

	// to update the first available entry!!!
	if(idx < FirstAV_QP)	{
		FirstAV_QP = idx;
	}
	nQP--;
	if(idx == IdxLastQP)	{	// Is removing the last record? To find the new last record. 
		IdxLastQP_Save = IdxLastQP;
		IdxLastQP = nQP_Reserved_M1;

		for(j=IdxLastQP_Save-1; j>nQP_Reserved_M1; j--)	{
			if(pQP_Data[j].queue_pair)	{	// a valid queue pair
				IdxLastQP = j;
				break;
			}
		}

		IdxLastQP64 = Align64_Int(IdxLastQP+1);	// +1 is needed since IdxLastQP is included!
	}

	printf("DBG> Rank = %d Destroyed %d QP in job %d (idx %d: %d qps). Client hostname %s ExeName = %s tid = %d nQP = %d FirstAV_QP = %d IdxLastQP = %d\n", 
		mpi_rank, idx, pQP_Data[idx].jobid, pQP_Data[idx].idx_JobRec, ActiveJobList[pQP_Data[idx].idx_JobRec].nQP, pQP_Data[idx].szClientHostName, 
		pQP_Data[idx].szClientExeName, pQP_Data[idx].ctid, nQP, FirstAV_QP, IdxLastQP);

	if( FirstAV_QP > (nQP+NUM_THREAD_IO_WORKER_INTER_SERVER) )	{
		printf("DBG> Something wrong!\n");
	}

	pthread_mutex_unlock(&process_lock);
}

void SERVER_QUEUEPAIR::ScanLostQueuePairs(void)
{
	int j, idx_queue;
	struct timeval tm1;	// tm1.tv_sec

	gettimeofday(&tm1, NULL);

	for(j=nFSServer; j<=IdxLastQP; j++)	{	// skip the clients on other servers
//	for(j=0; j<=IdxLastQP; j++)	{
		if(p_shm_TimeHeartBeat[j])	{
//			if( (tm1.tv_sec - p_shm_TimeHeartBeat[j]) > (T_FREQ_ALARM_HB + 3) )	{	// out of dated heart beat. Lost connection??? Destroy the queue pair. 
			if( (tm1.tv_sec - p_shm_TimeHeartBeat[j]) > (T_FREQ_ALARM_HB + 24000) )	{	// out of dated heart beat. Lost connection??? Destroy the queue pair. !!!!!!!!!!!!!!!!!
				idx_queue = pQP_Data[j].idx_queue;
//				pthread_mutex_lock(&(IO_Queue_List[idx_queue].lock));
//				IO_Queue_List[idx_queue].nQP --;
//				if(IO_Queue_List[idx_queue].nQP == 0)	{	// time to release the queue
//					Free_A_Queue(idx_queue);
//				}
//				pthread_mutex_unlock(&(IO_Queue_List[idx_queue].lock));
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
	int i, k, LastQPLocal, idx_queue, idx_qp, nQP_Server;
	__m512i Data;
	unsigned long int cmpMask, T_Queued;
	struct timeval tm;

	nNewMsg = 0;
	if(p_shm_NewMsgFlag == NULL)	return;
	LastQPLocal = IdxLastQP + 1;

	nQP_Server = NUM_THREAD_IO_WORKER_INTER_SERVER * nFSServer;
	for(i=0; i<nQP_Server; i++)	{	// alway scan new msg from other servers first!!!
		if(p_shm_NewMsgFlag[i])	{
			NewMsgList[nNewMsg] = i;
			nNewMsg++;
			p_shm_NewMsgFlag[i] = 0;
		}
	}
	gettimeofday(&tm, NULL);
	T_Queued = tm.tv_sec * 1000000 + tm.tv_usec;

	for(i=0; i<nNewMsg; i++)	{
		idx_qp = NewMsgList[i];
		idx_queue = pQP_Data[idx_qp].idx_queue;
		p_shm_IO_Cmd_Msg[idx_qp].idx_qp = idx_qp;	// set index of qp. Needed for communication!
		p_shm_IO_Cmd_Msg[idx_qp].idx_JobRec = pQP_Data[idx_qp].idx_JobRec;
		p_shm_IO_Cmd_Msg[idx_qp].T_Queued = T_Queued;

		IO_Queue_List[idx_queue].Enqueue(&(p_shm_IO_Cmd_Msg[idx_qp]));
	}


	nNewMsg = 0;

	if(IdxLastQP64 <=192)	{	// simple version
		for(i=0; i<LastQPLocal; i++)	{
			if(p_shm_NewMsgFlag[i])	{
				if(pQP_Data[i].bServerReady)	{
					NewMsgList[nNewMsg] = i;
	//				printf("DBG> Rank = %d. Found new msg for qp %d.\n", mpi_rank, i);
					nNewMsg++;
					p_shm_NewMsgFlag[i] = 0;
				}
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
						if(pQP_Data[i+k].bServerReady)	{
							NewMsgList[nNewMsg] = i + k;
	//						printf("DBG> Rank = %d. Found new msg for qp %d.\n", mpi_rank, i+k);
							nNewMsg++;
							p_shm_NewMsgFlag[i+k] = 0;
						}
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
						if(pQP_Data[i+k].bServerReady)	{
							NewMsgList[nNewMsg] = i + k;
	//						printf("DBG> Rank = %d. Found new msg for qp %d.\n", mpi_rank, i+k);
							nNewMsg++;
							p_shm_NewMsgFlag[i+k] = 0;
						}
					}
					cmpMask = cmpMask >> 1;
				}
			}
		}
	}

	gettimeofday(&tm, NULL);
	T_Queued = tm.tv_sec * 1000000 + tm.tv_usec;

	for(i=0; i<nNewMsg; i++)	{
		idx_qp = NewMsgList[i];
		idx_queue = pQP_Data[idx_qp].idx_queue;
		p_shm_IO_Cmd_Msg[idx_qp].idx_qp = idx_qp;	// set index of qp. Needed for communication!
		p_shm_IO_Cmd_Msg[idx_qp].idx_JobRec = pQP_Data[idx_qp].idx_JobRec;
		p_shm_IO_Cmd_Msg[idx_qp].T_Queued = T_Queued;

		IO_Queue_List[idx_queue].Enqueue(&(p_shm_IO_Cmd_Msg[idx_qp]));
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

	for(i = max(idx+1,nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER); i<max_qp; i++)	{
		if(pQP_Data[i].queue_pair == NULL)	{
			FirstAV_QP = i;
			break;
		}
	}
	if(FirstAV_QP < 0)	{
		printf("WARNING> All queue pairs are used.\n");
	}

	return idx;
}

void SERVER_QUEUEPAIR::Init_Server_Socket(int max_num_qp, int port)
{
	int i, nSizeofNewMsgFlag, nSizeofHeartBeat, nSizeofIOCmdMsg, nSizeofIOResult, nSizeofIOResult_Recv;

//	void *pHash;
	sock_addr = INADDR_ANY;
	sock_fd = -1;
	sock_signal_fd = -1;
	sock_epoll_fd = -1;
	sock_port = port;
	
	max_qp = max_num_qp;
	p_Hash_socket_fd = (CHASHTABLE_INT *)malloc(CHASHTABLE_INT::GetStorageSize(max_num_qp*2));
	p_Hash_socket_fd->DictCreate(max_num_qp*2, &elt_list_socket_fd, &ht_table_socket_fd);	// init hash table
	
//	pQP_Data = (QP_DATA *)malloc(sizeof(QP_DATA) * max_num_qp * 2);	
	pQP_Data = (QP_DATA *)malloc(sizeof(QP_DATA) * max_num_qp);
	assert(pQP_Data != NULL);
	for(i=0; i<max_qp; i++)	{
		pQP_Data[i].queue_pair = NULL;
	}

	nSizeofNewMsgFlag = sizeof(char)*max_qp;
	nSizeofHeartBeat = sizeof(time_t)*max_qp;
	nSizeofIOCmdMsg = sizeof(IO_CMD_MSG)*max_qp;
	nSizeofIOResult = sizeof(char)*IO_RESULT_BUFFER_SIZE*NUM_THREAD_IO_WORKER;	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	nSizeofIOResult_Recv = sizeof(char)*IO_RESULT_BUFFER_SIZE;	// the size of buffer to recv results from other servers. 
	nSizeshm_Global = nSizeofNewMsgFlag + nSizeofHeartBeat + nSizeofIOCmdMsg + nSizeofIOResult + nSizeofIOResult_Recv + MAX_LEN_RETURN_BUFF;
	p_shm_Global = memalign( 4096, nSizeshm_Global);
	assert(p_shm_Global != NULL);
	memset(p_shm_Global, 0, nSizeshm_Global);
	p_shm_NewMsgFlag = (unsigned char *)p_shm_Global;
	p_shm_TimeHeartBeat = (time_t *)((char*)p_shm_Global + nSizeofNewMsgFlag);
	p_shm_IO_Cmd_Msg = (IO_CMD_MSG *)((char*)p_shm_Global + nSizeofNewMsgFlag + nSizeofHeartBeat);
	p_shm_IO_Result = (char*)((char*)p_shm_Global + nSizeofNewMsgFlag + nSizeofHeartBeat + nSizeofIOCmdMsg);
	p_shm_IO_Result_Recv = (char*)((char*)p_shm_Global + nSizeofNewMsgFlag + nSizeofHeartBeat + nSizeofIOCmdMsg + nSizeofIOResult);
	p_CallReturnBuff = (char*)((char*)p_shm_Global + nSizeofNewMsgFlag + nSizeofHeartBeat + nSizeofIOCmdMsg + nSizeofIOResult + nSizeofIOResult_Recv);
	sp_CallReturnBuff = ncx_slab_init((void*)p_CallReturnBuff, MAX_LEN_RETURN_BUFF);

	FirstAV_QP = 0;
	nQP = 0;
	IdxLastQP = -1;
	IdxLastQP64 = -1;
	mr_shm_global = IB_RegisterBuf_RW_Local_Remote(p_shm_Global, nSizeshm_Global);
	assert(mr_shm_global != NULL);

//	mr_CallReturn = IB_RegisterBuf_RW_Local_Remote(p_CallReturnBuff, MAX_LEN_RETURN_BUFF);
//	assert(mr_CallReturn != NULL);
//	printf("DBG> Registered p_CallReturnBuff. mr_CallReturn = %p\n", mr_CallReturn);

	rem_buff = (unsigned char*)(mr_shm_global->addr);
	rem_key = mr_shm_global->rkey;
	rem_buff_size = nSizeshm_Global;
//	Init_Server_IB_Env(DEFAULT_REM_BUFF_SIZE);
}

int SERVER_QUEUEPAIR::Add_Epoll(int events, int fd)
{
	int rc;
	struct epoll_event ev;
	
	memset(&ev,0,sizeof(ev)); // placate valgrind
	ev.events = events;
	ev.data.fd= fd;
//	printf("INFO> Adding fd %d to epoll\n", fd);
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
			if (listen(fd,800) == -1) {
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
	int i, idx_IO_Worker;
	QP_DATA *pQP;
	QUEUE_PAIR_PREALLOCATED *pPreAllocatedQP;

	pQP = &(pQP_Data[idx]);
	idx_IO_Worker = Get_IO_Worker_Index_from_QP_Index(idx);

	pthread_mutex_lock(&lock_preallocate_qp);
	for(i=0; i<NUM_QP_PREALLOCATED; i++)	{
		if(Is_PreAllocated_QP_Ready[i])	{
			pPreAllocatedQP = &(List_of_QueuePair_PreAllocated[i]);
//			pQP->send_complete_queue = pPreAllocatedQP->send_complete_queue;
//			pQP->recv_complete_queue = pPreAllocatedQP->recv_complete_queue;
			pQP->send_complete_queue = send_complete_queue[idx_IO_Worker];
			pQP->recv_complete_queue = send_complete_queue[idx_IO_Worker];
			pQP->queue_pair = pPreAllocatedQP->queue_pair;
			pQP->ib_my_psn = pPreAllocatedQP->psn;
			pQP->ib_my_lid = port_attr_.lid;
			pQP->ib_my_qpn = pQP->queue_pair->qp_num;
			pPreAllocatedQP->ready = 0;
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
//	else if(i>= NUM_QP_TRIGGER_PREALLOCATED)	{	// The number of available queue pair is running low. Need to preallocate more queuepairs!!!
//		Refill_PreAllocated_QP_Pool();
//	}
}

typedef	struct	{
	int fd;
	int idx;	// idx of QP
	SERVER_QUEUEPAIR *pServer_QP;
	int nToken;	// for creating a unique thread
}QPPARAM, *PQPPARAM;

/*
const uint32_t Prime = 0x01000193; //   16777619
const uint32_t Seed  = 0x811C9DC5; // 2166136261
/// hash a single byte
inline uint32_t fnv1a0(unsigned char oneByte, uint32_t hash = Seed)
{
	return (oneByte ^ hash) * Prime;
}

uint32_t fnv1a(const void* data, size_t numBytes, uint32_t hash = Seed)
{
	assert(data);
	const unsigned char* ptr = (const unsigned char*)data;
	while (numBytes--)
		hash = fnv1a0(*ptr++, hash);
	return hash;
}

inline uint32_t fmix32(uint32_t h)
{
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;

    return h;
}

inline uint32_t rotl32 ( uint32_t x, int8_t r )
{
    return (x << r) | (x >> (32 - r));
}

void MurmurHash3_x86_32(const void *key, int len, uint32_t seed, void *out )
{
    const uint8_t * data = (const uint8_t*)key;
    const int nblocks = len / 4;

    uint32_t h1 = seed;

    uint32_t c1 = 0xcc9e2d51;
    uint32_t c2 = 0x1b873593;

    int i;

    //----------
    // body

    const uint32_t * blocks = (const uint32_t *)(data + nblocks*4);

    for(i = -nblocks; i; i++) {
        uint32_t k1 = blocks[i];

        k1 *= c1;
        k1 = rotl32(k1,15);
        k1 *= c2;

        h1 ^= k1;
        h1 = rotl32(h1,13);
        h1 = h1*5+0xe6546b64;
    }

    //----------
    // tail

    const uint8_t * tail = (const uint8_t*)(data + nblocks*4);

    uint32_t k1 = 0;

    switch(len & 3) {
        case 3: k1 ^= tail[2] << 16;
        case 2: k1 ^= tail[1] << 8;
        case 1: k1 ^= tail[0];
                k1 *= c1; k1 = rotl32(k1,15); k1 *= c2; h1 ^= k1;
    }

    //----------
    // finalization

    h1 ^= len;

    h1 = fmix32(h1);

    *(uint32_t*)out = h1;
}
*/
void* Func_thread_Finish_QP_Setup(void *pParam)
{
	int fd, idx, nBytes, idx_Queue, idx_JobRec;
	QPPARAM *pQPParam;
	SERVER_QUEUEPAIR *pServer_QP;
	unsigned long long jobid_hash, jobid_cip_ctid_hash;	
	GLOBAL_ADDR_DATA Global_Addr_Data;
	DATA_SEND_BY_SERVER *pData_to_send=NULL;
	DATA_SEND_BY_CLIENT *pData_to_recv=NULL;

	pQPParam = (QPPARAM *)pParam;
	if(Unique_Thread.Redeem_A_Token(pQPParam->nToken) == 0)	return NULL;

	fd = pQPParam->fd;
	idx = pQPParam->idx;
	pServer_QP = pQPParam->pServer_QP;

	pData_to_recv = (DATA_SEND_BY_CLIENT *)((char*)pParam + sizeof(QPPARAM));
	pData_to_send = (DATA_SEND_BY_SERVER *)((char*)pParam + sizeof(QPPARAM) + sizeof(DATA_SEND_BY_CLIENT) );


	// DBG> IB_Modify_QP() 1273 us
	pServer_QP->IB_Modify_QP(pServer_QP->pQP_Data[idx].queue_pair, pServer_QP->pQP_Data[idx].ib_my_psn, (uint16_t)(pServer_QP->pQP_Data[idx].ib_pal_lid), pServer_QP->pQP_Data[idx].ib_pal_qpn, pServer_QP->pQP_Data[idx].ib_pal_psn);
	
//	nBytes = read(fd, &(pServer_QP->pQP_Data[idx].tag_mem), 2*sizeof(int)+sizeof(long int));
//	if( nBytes != (2*sizeof(int)+sizeof(long int)) )	{	// 4 int
//		printf("Error> nBytes = %d. Unexpected value.\n", nBytes);
//	}
//	if(pServer_QP->pQP_Data[idx].tag_mem != TAG_EXCH_MEM_INFO)  {
//		printf("DBG> pQP_Data[idx].tag_mem = %x\n", pServer_QP->pQP_Data[idx].tag_mem);
//		fflush(stdout);
//	}
	
//	assert(pServer_QP->pQP_Data[idx].tag_mem == TAG_EXCH_MEM_INFO);
	
//	pServer_QP->rem_buff = (unsigned char*)pServer_QP->mr_shm_global->addr;
//	pServer_QP->rem_key = pServer_QP->mr_shm_global->rkey;
//	pServer_QP->rem_buff_size = pServer_QP->nSizeshm_Global;
	
//	printf("DBG> rem_buff_size = %d rem_buff at %p\n", pServer_QP->rem_buff_size, pServer_QP->rem_buff);
//	printf("INFO> Mine (%lx, %d) Remote (%lx, %d)\n", pServer_QP->mr_rem->addr, pServer_QP->mr_rem->rkey, pServer_QP->pQP_Data[idx].rem_addr, pServer_QP->pQP_Data[idx].rem_key);
//	printf("INFO> Mine (%lx, %d) Remote (%lx, %d)\n", pServer_QP->mr_shm_global->addr, pServer_QP->mr_shm_global->rkey, pServer_QP->pQP_Data[idx].rem_addr, pServer_QP->pQP_Data[idx].rem_key);
	
//	pServer_QP->tag_mem = TAG_EXCH_MEM_INFO;
//	write(fd, &(pServer_QP->tag_mem), 2*sizeof(int)+sizeof(long int));
	
//	read(fd, &JobInfo, sizeof(JOB_INFO_DATA));
//	assert(JobInfo.comm_tag == TAG_SUBMIT_JOB_INFO);

	pthread_mutex_lock(&lock_Modify_ActiveJob_List);
	idx_JobRec = pHT_ActiveJobs->DictSearch(pData_to_recv->JobInfo.jobid, &elt_list_ActiveJobs, &ht_table_ActiveJobs, &jobid_hash);
	if(idx_JobRec < 0)	{	// Do not exist. Need to insert it into hash table. 
		idx_JobRec = pHT_ActiveJobs->DictInsertAuto(pData_to_recv->JobInfo.jobid, &elt_list_ActiveJobs, &ht_table_ActiveJobs);
		Init_NewActiveJobRecord(idx_JobRec, pData_to_recv->JobInfo.jobid, pData_to_recv->JobInfo.nnode);
	}
	else	{
		fetch_and_add(&(ActiveJobList[idx_JobRec].nQP), 1);	// Increse the counter by 1
	}
	pthread_mutex_unlock(&lock_Modify_ActiveJob_List);

	
//	idx_Queue = ( ((idx-nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER + (idx-nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER/MAX_NUM_QUEUE_MX ) % MAX_NUM_QUEUE_MX) + nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER;
	idx_Queue = ( ((idx-nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER + (idx-nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER/MAX_NUM_QUEUE_MX ) % MAX_NUM_QUEUE_MX) + NUM_THREAD_IO_WORKER_INTER_SERVER;
//	printf("DBG> idx_qp = %d idx_Queue = %d\n", idx, idx_Queue);
	
//	printf("INFO> jobid = %d nnode = %d cip = %u ctid = %d idx_queue = %d\n", pData_to_recv->JobInfo.jobid, pData_to_recv->JobInfo.nnode, pData_to_recv->JobInfo.cip, pData_to_recv->JobInfo.ctid, idx_Queue);
	pServer_QP->pQP_Data[idx].idx_queue = idx_Queue;
	pServer_QP->pQP_Data[idx].jobid = pData_to_recv->JobInfo.jobid;
	pServer_QP->pQP_Data[idx].idx_JobRec = idx_JobRec;
	pServer_QP->pQP_Data[idx].cuid = pData_to_recv->JobInfo.cuid;
	pServer_QP->pQP_Data[idx].cgid = pData_to_recv->JobInfo.cgid;
	pServer_QP->pQP_Data[idx].ctid = pData_to_recv->JobInfo.ctid;
	memcpy(pServer_QP->pQP_Data[idx].szClientHostName, pData_to_recv->JobInfo.szClientHostName, MAX_HOSTNAME_LEN);
	memcpy(pServer_QP->pQP_Data[idx].szClientExeName, pData_to_recv->JobInfo.szClientExeName, MAX_EXENAME_LEN);
	pServer_QP->pQP_Data[idx].bTimeout = 0;
	pServer_QP->pQP_Data[idx].bServerReady = 1;

	printf("DBG> Rank = %d Creating QP: Jobid = %d idx_qp = %d idx_Queue = %d (%d qps) from %s on client %s nQP = %d FirstAV_QP = %d IdxLastQP = %d\n", 
		mpi_rank, pData_to_recv->JobInfo.jobid, idx, idx_Queue, ActiveJobList[idx_JobRec].nQP, pData_to_recv->JobInfo.szClientExeName, pData_to_recv->JobInfo.szClientHostName, pServer_QP->nQP, pServer_QP->FirstAV_QP, pServer_QP->IdxLastQP);
	
//	Global_Addr_Data.comm_tag = TAG_GLOBAL_ADDR_INFO;
//	Global_Addr_Data.addr_NewMsgFlag = (uint64_t)pServer_QP->p_shm_NewMsgFlag + sizeof(char)*idx;
//	Global_Addr_Data.addr_TimeHeartBeat = (uint64_t)pServer_QP->p_shm_TimeHeartBeat + sizeof(time_t)*idx;
//	Global_Addr_Data.addr_IO_Cmd_Msg = (uint64_t)pServer_QP->p_shm_IO_Cmd_Msg + sizeof(IO_CMD_MSG)*idx;
//	write(fd, &Global_Addr_Data, sizeof(GLOBAL_ADDR_DATA));
	
	free(pParam);

	return NULL;
}

int SERVER_QUEUEPAIR::Accept_Client()
{
	int fd, nBytes, idx, idx_fd, one=1;
	unsigned int *p_token;
	struct sockaddr_in in;
	socklen_t sz = sizeof(in);
	char szBuff[128];
	
	fd = accept(sock_fd,(struct sockaddr*)&in, &sz);
	if (fd == -1) {
		printf("INFO> accept: %s\n", strerror(errno)); 
	}
	else	{
		if(setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)	perror("setsockopt(2) error");
		if (sizeof(in) == sz) {
//			printf("INFO> connection fd %d from %s:%d\n", fd, inet_ntoa(in.sin_addr), (int)ntohs(in.sin_port));
		}
		
//		p_token = (unsigned int*)szBuff;
//		nBytes = read(fd, szBuff, 4*sizeof(int));
//		assert(p_token[0] == TAG_EXCH_QP_INFO);
//		if( nBytes != 16 )	{	// 4 int
//			printf("Error> nBytes = %d. Unexpected value.\n", nBytes);
//		}
		
		nConnectionAccu++;
		pthread_mutex_lock(&process_lock);
		idx = FindFirstAvailableQP();
		assert(idx >= 0);
		nQP++;
//		printf("DBG> QP is allocated at %d. nQP = %d\n", idx, nQP);
		if(IdxLastQP < idx)	{
			IdxLastQP = idx;
			IdxLastQP64 = Align64_Int(IdxLastQP+1);	// +1 is needed since IdxLastQP is included!
		}
//		Get_A_PreAllocated_QueuePair(idx);
		IB_CreateQueuePair(idx);

//		Rec_Add[nAdd] = (long int)(pQP_Data[idx].queue_pair);
//		nAdd++;
//		printf("DBG> New QP idx = %d nQP = %d FirstAV_QP = %d IdxLastQP = %d\n", idx, nQP, FirstAV_QP, IdxLastQP);
		if( FirstAV_QP > (nQP+NUM_THREAD_IO_WORKER_INTER_SERVER) )	{
			printf("DBG> Something wrong!\n");
		}

		pthread_mutex_unlock(&process_lock);
		idx_fd = p_Hash_socket_fd->DictInsert(fd, idx, &elt_list_socket_fd, &ht_table_socket_fd);

		if(Add_Epoll(EPOLLIN, fd) == -1) {
			close(fd);
			fd = -1;
		}
	}

	return fd;
}

void SERVER_QUEUEPAIR::Drain_Client(const int fd)
{
	int rc, idx, nBytes;
	char buf[2048];
	QPPARAM *pQPParam=NULL;
	DATA_SEND_BY_SERVER *pData_to_send=NULL;
	DATA_SEND_BY_CLIENT *pData_to_recv=NULL;
	pthread_t pthread_Setup_QP;
	
	rc = read(fd, buf, sizeof(DATA_SEND_BY_CLIENT));
	switch(rc) {
	default:
		idx = p_Hash_socket_fd->DictSearchOrg(fd, &elt_list_socket_fd, &ht_table_socket_fd);
		assert(idx >= 0);
		pQPParam = (QPPARAM*)malloc( sizeof(QPPARAM) + sizeof(DATA_SEND_BY_CLIENT) + sizeof(DATA_SEND_BY_SERVER) );
		assert(pQPParam != NULL);
		pData_to_recv = (DATA_SEND_BY_CLIENT *)((char*)pQPParam + sizeof(QPPARAM));
		pData_to_send = (DATA_SEND_BY_SERVER *)((char*)pQPParam + sizeof(QPPARAM) + sizeof(DATA_SEND_BY_CLIENT) );
		memcpy(pData_to_recv, buf, sizeof(DATA_SEND_BY_CLIENT));

		pQP_Data[idx].ib_pal_lid = pData_to_recv->qp.lid;
		pQP_Data[idx].ib_pal_qpn = pData_to_recv->qp.qp_n;
		pQP_Data[idx].ib_pal_psn = pData_to_recv->qp.psn;

		pQP_Data[idx].nPut_Get = 0;
		pQP_Data[idx].nPut_Get_Done = 0;

		pQP_Data[idx].rem_key = pData_to_recv->ib_mem.key;
		pQP_Data[idx].rem_addr = pData_to_recv->ib_mem.addr;


		pData_to_send->qp.comm_tag = TAG_EXCH_QP_INFO;
		pData_to_send->qp.lid = pQP_Data[idx].ib_my_lid;
		pData_to_send->qp.qp_n = pQP_Data[idx].ib_my_qpn;
		pData_to_send->qp.psn = pQP_Data[idx].ib_my_psn;

		pData_to_send->ib_mem.comm_tag = TAG_EXCH_MEM_INFO;
		pData_to_send->ib_mem.key = mr_shm_global->rkey;
		pData_to_send->ib_mem.addr = (uint64_t)(mr_shm_global->addr);

		pData_to_send->global_addr.comm_tag = TAG_GLOBAL_ADDR_INFO;
		pData_to_send->global_addr.addr_NewMsgFlag = (uint64_t)p_shm_NewMsgFlag + sizeof(char)*idx;
		pData_to_send->global_addr.addr_TimeHeartBeat = (uint64_t)p_shm_TimeHeartBeat + sizeof(time_t)*idx;
		pData_to_send->global_addr.addr_IO_Cmd_Msg = (uint64_t)p_shm_IO_Cmd_Msg + sizeof(IO_CMD_MSG)*idx;

		nBytes = write(fd, pData_to_send, sizeof(DATA_SEND_BY_SERVER));
		assert(nBytes == sizeof(DATA_SEND_BY_SERVER));

//		nBytes = read(fd, pData_to_recv, sizeof(DATA_SEND_BY_CLIENT));
//		assert(nBytes == sizeof(DATA_SEND_BY_CLIENT));

//		IB_CreateQueuePair(idx);	// DBG> IB_CreateQueuePair() 3345 us. Slow process! We can do pre-allocation to save time. When the number of pre-allocated QP is 
		// Get a queue pair from preallocated list of qps

		// not large enough, start a thread and do pre-allocation! 
//		pQP_Data[idx].ib_pal_lid = p_token[1];
//		pQP_Data[idx].ib_pal_qpn = p_token[2];
//		pQP_Data[idx].ib_pal_psn = p_token[3];

//		pQP_Data[idx].jobid = pData_to_recv->JobInfo.jobid;


		//		printf("INFO> Inserted a hash entry at %d\n", idx);
//		printf(" Pal token (%d, %d, %d) My token (%d, %d, %d)\n", pData_to_recv->qp.lid, pData_to_recv->qp.qp_n, pData_to_recv->qp.psn, pQP_Data[idx].ib_my_lid, pQP_Data[idx].ib_my_qpn, pQP_Data[idx].ib_my_psn);
		
//		pQP_Data[idx].tag_ib_me = TAG_EXCH_QP_INFO;
//		write(fd, &(pQP_Data[idx].tag_ib_me), 4*sizeof(int));
		//		write(fd, &(pQP_Data[idx].ib_my_lid), 4*sizeof(int));


		pQPParam->fd = fd;
		pQPParam->idx = idx;
		pQPParam->pServer_QP = this;
		pQPParam->nToken = Unique_Thread.Apply_A_Token();

		if(pthread_create(&pthread_Setup_QP, &thread_attr, Func_thread_Finish_QP_Setup, (void*)pQPParam)) {
			fprintf(stderr, "Error creating thread Func_thread_Finish_QP_Setup().\n");
			return;
		}

		break;
	case  0: 
//		printf("INFO> fd %d closed\n", fd);
		idx = p_Hash_socket_fd->DictSearchOrg(fd, &elt_list_socket_fd, &ht_table_socket_fd);
		if(idx >= 0)	{
			p_Hash_socket_fd->DictDelete(fd, &elt_list_socket_fd, &ht_table_socket_fd);	// !!!!!!!!!!!!!!!!!!!!!!!!!! only for test!!!!
			Del_Epoll(fd);
			close(fd);
		}
		else	{
			printf("Error: failed to find fd %d from hash table.\n", fd);
		}
		break;
	case -1: printf("INFO> recv: %s\n", strerror(errno));    break;
	}
	
	if (rc != 0) return;
	
// client closed. log it, tell epoll to forget it, close it.
//	printf("INFO> client %d has closed\n", fd);
//	Del_Epoll(fd);
//	close(fd);
	
}

void SERVER_QUEUEPAIR::Socket_Server_Loop(void)
{
	int i, epoll_ret;
	struct epoll_event ev;
	struct signalfd_siginfo info;
	
	CoreBinding.Bind_This_Thread();

	// block all signals. we take signals synchronously via signalfd
	sigset_t all;
	// signals that we'll accept synchronously via signalfd */
//	int sigs[] = {SIGIO,SIGHUP,SIGTERM,SIGINT,SIGQUIT,SIGALRM};
	int sigs[] = {SIGIO,SIGHUP,SIGTERM,SIGINT,SIGQUIT,SIGUSR1};
	
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
				fprintf(stderr,"Got signal %d (SIGTERM)\n", info.ssi_signo);
			}
			else	{
				fprintf(stderr,"Got signal %d\n", info.ssi_signo);  
			}
			goto done;
		}
		
		/* regular POLLIN. handle the particular descriptor that's ready */
		assert(ev.events & EPOLLIN);
//		fprintf(stderr,"INFO> handle POLLIN on fd %d\n", ev.data.fd);
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
	int i, ret;
	int devices;
	
	dev_list_ = ibv_get_device_list(&devices);
	
//	if (!dev_list_) {
//		int errno_backup = errno;
//		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_get_device_list (errno=%d).\n", __FILE__, __LINE__, errno_backup);
//		exit(1);
//	}
	assert(dev_list_ != NULL);
	
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

	assert(context_ != NULL);
	
//	struct ibv_device_attr device_attr;
	
//	if (!context_) {
//		fprintf(stderr, "Error occured at %s:L%d. Failure: No HCA can use.\n", __FILE__, __LINE__);
//		exit(1);
//	}
	
//	ret = ibv_query_device(context_, &device_attr);
//	printf("max_qp = %d max_qp_wr = %d\n", device_attr.max_qp, device_attr.max_qp_wr);
	
	ret = ibv_query_port(context_, 1, &port_attr_);
	
	if (ret != 0 || port_attr_.lid == 0) {
		// error handling
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_query_port.\n", __FILE__, __LINE__);
		exit(1);
	}
	
	pd_ = ibv_alloc_pd(context_);
	assert(pd_ != NULL);
	
//	if (!pd_) {
//		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_alloc_pd.\n", __FILE__, __LINE__);
//		exit(1);
//	}

	pIO_Cmd_ToSend_Other_Server = (IO_CMD_MSG *)memalign(64, DATA_COPY_THRESHOLD_SIZE + 4096);
	assert(pIO_Cmd_ToSend_Other_Server != NULL);
	mr_loc = IB_RegisterBuf_RW_Local_Remote(pIO_Cmd_ToSend_Other_Server, DATA_COPY_THRESHOLD_SIZE + 4096);
	assert(mr_loc != NULL);

	for(i=0; i<NUM_THREAD_IO_WORKER; i++)	{
		send_complete_queue[i] = ibv_create_cq(context_, 1+NUM_THREAD_IO_WORKER_INTER_SERVER, NULL, NULL, 0);
		assert(send_complete_queue[i] != NULL);
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
	int i;

	for(i=0; i<NUM_THREAD_IO_WORKER; i++)	{
		if(send_complete_queue[i])	{
			ibv_destroy_cq(send_complete_queue[i]);
			send_complete_queue[i] = NULL;
		}
	}

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
  rtr_attr.max_dest_rd_atomic = 1;	// Ref to ctx_set_out_reads() in simple.c!!!!!!!!!!!!!!!!

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
  rts_attr.max_rd_atomic = 1;	// Ref to ctx_set_out_reads() in simple.c!!!!!!!!!!!!!!!!
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
//	*pSend_complete_queue = ibv_create_cq(context, IB_QUEUE_SIZE, NULL, NULL, 0);
//	*pRecv_complete_queue = ibv_create_cq(context, IB_QUEUE_SIZE, NULL, NULL, 0);
	
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
	
	qp_init_attr.cap.max_send_wr = 1+NUM_THREAD_IO_WORKER_INTER_SERVER;
//	qp_init_attr.cap.max_recv_wr = 1;
	qp_init_attr.cap.max_recv_wr = 1;
	qp_init_attr.cap.max_send_sge = 1+NUM_THREAD_IO_WORKER_INTER_SERVER;
	qp_init_attr.cap.max_recv_sge = 1;
	qp_init_attr.sq_sig_all = 1;
	//  qp_init_attr.sq_sig_all = 0;	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	//  qp_init_attr.sq_sig_all = 1;	// 0 - In every Work Request submitted to the Send Queue, the user must decide whether to generate a Work Completion for successful 
	//     completions or not
	// 1 - All Work Requests that will be submitted to the Send Queue will always generate a Work Completion !!!!!!!!!!
	qp_init_attr.cap.max_inline_data = MAX_INLINE_SIZE;
	
	*pQueue_pair = ibv_create_qp(pd, &qp_init_attr);
	
	if (!pQueue_pair) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_create_qp().\n", __FILE__, __LINE__);
		exit(1);
	}
}

void SERVER_QUEUEPAIR::IB_CreateQueuePair(int idx)
{
	QP_DATA *pQP;
	int idx_io_worker;
	
	pQP = &(pQP_Data[idx]);
	
	pQP_Data[idx].bServerReady = 0;
	idx_io_worker = Get_IO_Worker_Index_from_QP_Index(idx);
	printf("DBG> rank = %d idx = %d idx_io_worker = %d\n", mpi_rank, idx, idx_io_worker);
	pQP->send_complete_queue = send_complete_queue[idx_io_worker];
	pQP->recv_complete_queue = send_complete_queue[idx_io_worker];

	CreateQueuePair(context_, pd_, &(pQP->send_complete_queue), &(pQP->recv_complete_queue), &(pQP->queue_pair), &(pQP->ib_my_psn));
	
	pQP->ib_my_lid = port_attr_.lid;
	pQP->ib_my_qpn = pQP->queue_pair->qp_num;

	if(pthread_mutex_init(&(pQP->qp_lock), NULL) != 0) {
		perror("pthread_mutex_init");
		exit(1);
	}
}

struct ibv_mr* SERVER_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote(void* buf, size_t len)
{
  struct ibv_mr* mr_buf = ibv_reg_mr(pd_, buf, len, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
  if (mr_buf == 0) {
	  perror("ibv_reg_mr");
      fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_reg_mr on RW_Local_Remote.\n", __FILE__, __LINE__);
					char szHostName[128];
					gethostname(szHostName, 63);
					printf("DBG> Hostname = %s pid = %d\n", szHostName, getpid());
					fflush(stdout);
					sleep(300);
      exit(1);
  }
  nSizeReg += len;
//  printf("DBG> nSizeReg = %ld\n", nSizeReg);

  return mr_buf;
}

void SERVER_QUEUEPAIR::IB_Put(int idx, void* loc_buf, uint32_t lkey, void* rem_buf, uint32_t rkey, size_t len)
{
	long int t1_ms, t2_ms;
	int ne, ret;
	struct ibv_sge sge = {};
	struct timeval tm1, tm2;	// tm1.tv_sec
	struct ibv_send_wr write_wr = {};
	struct ibv_wc wc = {};
	int bTimeOut=0;	// the flag of time out in PUT. 

//	while(pQP_Data[idx].bTimeout==0)	{
//	}
//	assert(pQP_Data[idx].bServerReady == 1);
	if(pQP_Data[idx].bTimeout)	{	// Something wrong with this QP. Client may disconnect or die...
		printf("WARNING> QP %d got timeout in previous Put(). Ignore all OPs for this QP. HostName = %s tid = %d\n", 
			idx, pQP_Data[idx].szClientHostName, pQP_Data[idx].ctid);
		return;
	}

	sge.addr = (uint64_t)(uintptr_t)loc_buf;
	sge.length = len;
	sge.lkey = lkey;
	

retry:
	memset(&write_wr, 0, sizeof(struct ibv_send_wr));
	write_wr.wr_id = 0;
	write_wr.sg_list = &sge;
	write_wr.num_sge = 1;
	write_wr.opcode = IBV_WR_RDMA_WRITE;
	write_wr.wr.rdma.rkey = rkey;
	write_wr.wr.rdma.remote_addr = (uint64_t)rem_buf;

	write_wr.send_flags = 2;
	if(len <= MAX_INLINE_SIZE)  write_wr.send_flags |= IBV_SEND_INLINE;

	struct ibv_send_wr* bad_wr;
	ret = ibv_post_send(pQP_Data[idx].queue_pair, &write_wr, &bad_wr);
	if (ret != 0) {
/*
		if( (ret == 12) && bTimeOut )	{
			printf("ERROR> Timeout in QP(%d) Put %zu bytes for client %s tid = %d nConnectionAccu = %d\n", idx, len, pQP_Data[idx].szClientHostName, pQP_Data[idx].ctid, nConnectionAccu);
			return;
		}
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_post_send in Put(). ret = %d nConnectionAccu = %d \n", __FILE__, __LINE__, ret, nConnectionAccu);
		exit(1);
*/
		if( ret == 12 )	{
			if(bTimeOut)	printf("ERROR> Rank = %d Timeout in QP(%d) Put %zu bytes for client %s tid = %d nConnectionAccu = %d Server(%d, %d, %d) Client(%d, %d, %d)\n", 
				mpi_rank, idx, len, pQP_Data[idx].szClientHostName, pQP_Data[idx].ctid, nConnectionAccu, pQP_Data[idx].ib_my_lid, pQP_Data[idx].ib_my_qpn, pQP_Data[idx].ib_my_psn, pQP_Data[idx].ib_pal_lid, pQP_Data[idx].ib_pal_qpn, pQP_Data[idx].ib_pal_psn);
			else	{	// Maybe the QP is not working anymore since client may exit already.
				pQP_Data[idx].bTimeout = 1;
				printf("Warning> Rank = %d Error in for QP(%d) Put %zu bytes for client %s tid = %d nConnectionAccu = %d Server(%d, %d, %d) Client(%d, %d, %d). Setting TIMEOUT flag.\n", 
				mpi_rank, idx, len, pQP_Data[idx].szClientHostName, pQP_Data[idx].ctid, nConnectionAccu, pQP_Data[idx].ib_my_lid, pQP_Data[idx].ib_my_qpn, pQP_Data[idx].ib_my_psn, pQP_Data[idx].ib_pal_lid, pQP_Data[idx].ib_pal_qpn, pQP_Data[idx].ib_pal_psn);
			}
			return;
		}
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_post_send in Put(). ret = %d tid= %d nConnectionAccu = %d Server(%d, %d, %d) Client(%d, %d, %d)\n", 
			__FILE__, __LINE__, ret, nConnectionAccu, pQP_Data[idx].ctid, pQP_Data[idx].ib_my_lid, pQP_Data[idx].ib_my_qpn, pQP_Data[idx].ib_my_psn, pQP_Data[idx].ib_pal_lid, pQP_Data[idx].ib_pal_qpn, pQP_Data[idx].ib_pal_psn);
		exit(1);

	}

	pQP_Data[idx].nPut_Get++;

//	if ( (pQP_Data[idx].nPut_Get%DEF_CQ_MOD) == (DEF_CQ_MOD - 1)) {
//		write_wr.send_flags |= IBV_SEND_SIGNALED;
//	}

	if( (pQP_Data[idx].nPut_Get - pQP_Data[idx].nPut_Get_Done) >= IB_QUEUE_SIZE ) {
		memset(&wc, 0, sizeof(struct ibv_wc));

		gettimeofday(&tm1, NULL);
		t1_ms = (tm1.tv_sec * 1000) + (tm1.tv_usec / 1000);
		while(1)	{
//			ne = ibv_poll_cq(pQP_Data[idx].send_complete_queue, CTX_POLL_BATCH, &wc);
			ne = ibv_poll_cq(pQP_Data[idx].send_complete_queue, 1, &wc);
			gettimeofday(&tm2, NULL);
			t2_ms = (tm2.tv_sec * 1000) + (tm2.tv_usec / 1000);
			if (ne == 1) {
				pQP_Data[idx].nPut_Get_Done +=1;
//				nPut_Done += DEF_CQ_MOD;
//				pthread_mutex_unlock(&(pQP_Data[idx].qp_lock));
				break;
			}
			else if(ne == 0)	{	// equal 0. Message not sent out yet!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! This could happen although it is rare!!!!!!!!!!!!!!!!
				if( (t2_ms - t1_ms) > QP_PUT_TIMEOUT_MS )	{
					bTimeOut = 1;
					goto retry;
				}
			}
			else if (ne < 0) {
				fprintf(stderr, "poll CQ failed %d\n",ne);
//				pthread_mutex_unlock(&(pQP_Data[idx].qp_lock));
				exit(1);
				return;
			}
			else	{
				fprintf(stderr, "ibv_poll_cq() return code is %d\n",ne);
//				pthread_mutex_unlock(&(pQP_Data[idx].qp_lock));
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
//	struct ibv_wc wc[CTX_POLL_BATCH];
	struct ibv_wc wc;

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

//	if ( (pQP_Data[idx].nPut_Get%DEF_CQ_MOD) == (DEF_CQ_MOD - 1)) {
//		write_wr.send_flags |= IBV_SEND_SIGNALED;
//	}

	if( (pQP_Data[idx].nPut_Get - pQP_Data[idx].nPut_Get_Done) >= IB_QUEUE_SIZE ) {
//		memset(wc, 0, sizeof(struct ibv_wc)*CTX_POLL_BATCH);
		memset(&wc, 0, sizeof(struct ibv_wc));

		while(1)	{
//			ne = ibv_poll_cq(pQP_Data[idx].send_complete_queue, CTX_POLL_BATCH, wc);
			ne = ibv_poll_cq(pQP_Data[idx].send_complete_queue, 1, &wc);
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
