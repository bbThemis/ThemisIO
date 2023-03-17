// g++ -march=skylake-avx512 -g -O0 -o server put_get_server.cpp -libverbs -lpthread -lrt -Wunused-variable -I/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/include -L/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/lib/release -L/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/lib -lmpicxx -lmpifort -lmpi -ldl
// gcc -g -o fsclient client/put_get_client.cpp dict.cpp xxhash.cpp -libverbs -lpthread -lrt

#include <cassert>
#include <cerrno>
#include <cstdio>

#include <unistd.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <execinfo.h>

#include <arpa/inet.h>
#include <netinet/in.h> 
#include <net/if.h>
#include <sys/ioctl.h>

#include <unistd.h>
#include <sys/syscall.h>
#include <signal.h>

#include <mpi.h> 

// #include "qp.h"
#include "ucx_rma.h"
#include "myfs.h"
#include "corebinding.h"
#include "unique_thread.h"
#include "io_queue.h"

#define T_FREQ_REPORT_RESULT (1)
#define PORT 8888
#define UCX_PORT 12589

CORE_BINDING CoreBinding;

extern int nActiveJob;
extern JOBREC ActiveJobList[MAX_NUM_ACTIVE_JOB];
extern LISTJOBREC IdxJobRecList[MAX_NUM_ACTIVE_JOB];

// extern PARAM_PREALLOCATE_QP pParam_PreAllocate[N_THREAD_PREALLOCATE_QP];
// extern pthread_t pthread_preallocate[N_THREAD_PREALLOCATE_QP];
extern pthread_t pthread_IO_Worker[NUM_THREAD_IO_WORKER];
extern pthread_t pthread_IO_Worker_UCX[NUM_THREAD_IO_WORKER];
extern pthread_t thread_ucx_worker_progress[NUM_THREAD_IO_WORKER];

extern CCreatedUniqueThread Unique_Thread;
extern CIO_QUEUE IO_Queue_List[MAX_NUM_QUEUE];

typedef	struct	{
	uint32_t lid;
	uint32_t qp_n;
	uint32_t psn;
	int rem_key;
	uint64_t rem_addr;
	uint64_t addr_NewMsgFlag;
	uint64_t addr_TimeHeartBeat;
	uint64_t addr_IO_Cmd_Msg;
}QPAIR_DATA;

typedef struct {
	char peer_address[MAX_UCP_ADDR_LEN];
	size_t peer_address_length = 0;
	char rkey_buffer[MAX_UCP_RKEY_SIZE];
    size_t rkey_buffer_size = 0;

	uint64_t rem_addr;
	uint64_t addr_NewMsgFlag;
	uint64_t addr_TimeHeartBeat;
	uint64_t addr_IO_Cmd_Msg;
}UCX_RMA_SETUP_DATA;

typedef	struct	{
//	uint64_t remote_addr_new_msg;
//	uint64_t remote_addr_heart_beat;	// the address of remote buffer to write heart beat time info.
//	uint64_t remote_addr_IO_CMD;	// the address of remote buffer to IO requests.
//	uint32_t lid;
//	uint32_t qp_n;
//	uint32_t psn;
//	int remote_rkey;
	struct in_addr sin_addr;
	int port;
	int ucx_port;
	char szIP[16];
}FS_SEVER_INFO;



int Server_Started=0;
int Ucx_Server_Started = 0;
FS_SEVER_INFO ThisNode;
FS_SEVER_INFO AllFSNodes[MAX_FS_UCX_SERVER];
QPAIR_DATA *pQPair_Inter_FS=NULL;
UCX_RMA_SETUP_DATA *pUCX_RMA_Inter_FS=NULL;

int mpi_rank, nFSServer=0;	// rank and size of MPI
int nNUMAPerNode=1;	// number of numa nodes per compute node

// SERVER_QUEUEPAIR Server_qp;
SERVER_RDMA Server_ucx;
pthread_attr_t thread_attr;

void Get_Local_Server_Info(void);
void Setup_QP_Among_Servers(void);
void Setup_UCX_Among_Servers(void);






void Setup_QP_Among_Servers(void)
{
	/*int i, j, idx;

	pQPair_Inter_FS = (QPAIR_DATA *)malloc(sizeof(QPAIR_DATA)*nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER);

	Server_qp.nQP = 0;	// the qp with other servers are always put at the beginning

	for(i=0; i<nFSServer; i++)	{
		if(i != mpi_rank)	{
			idx = i*NUM_THREAD_IO_WORKER_INTER_SERVER;
			for(j=0; j<NUM_THREAD_IO_WORKER_INTER_SERVER; j++)	{
				Server_qp.nQP++;
				Server_qp.IB_CreateQueuePair(idx+j);
				pQPair_Inter_FS[idx+j].lid = Server_qp.pQP_Data[idx+j].ib_my_lid;
				pQPair_Inter_FS[idx+j].qp_n = Server_qp.pQP_Data[idx+j].ib_my_qpn;
				pQPair_Inter_FS[idx+j].psn = Server_qp.pQP_Data[idx+j].ib_my_psn;

				pQPair_Inter_FS[idx+j].rem_key = Server_qp.mr_shm_global->rkey;
				pQPair_Inter_FS[idx+j].rem_addr = (uint64_t)Server_qp.p_shm_IO_Result_Recv;

				pQPair_Inter_FS[idx+j].addr_NewMsgFlag = (uint64_t)Server_qp.p_shm_NewMsgFlag + sizeof(char)*(idx+j);
				pQPair_Inter_FS[idx+j].addr_TimeHeartBeat = (uint64_t)Server_qp.p_shm_TimeHeartBeat + sizeof(time_t)*(idx+j);
				pQPair_Inter_FS[idx+j].addr_IO_Cmd_Msg = (uint64_t)Server_qp.p_shm_IO_Cmd_Msg + sizeof(IO_CMD_MSG)*(idx+j);
			}
		}
	}
	Server_qp.FirstAV_QP = nFSServer * NUM_THREAD_IO_WORKER_INTER_SERVER;
	Server_qp.IdxLastQP = nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER - 1;
	Server_qp.IdxLastQP64 = Align64_Int(Server_qp.IdxLastQP+1);	// +1 is needed since IdxLastQP is included!

	MPI_Alltoall(MPI_IN_PLACE, sizeof(QPAIR_DATA)*NUM_THREAD_IO_WORKER_INTER_SERVER, MPI_CHAR, pQPair_Inter_FS, sizeof(QPAIR_DATA)*NUM_THREAD_IO_WORKER_INTER_SERVER, MPI_CHAR, MPI_COMM_WORLD);

	for(i=0; i<nFSServer; i++)	{
		if(i != mpi_rank)	{
			idx = i*NUM_THREAD_IO_WORKER_INTER_SERVER;
			for(j=0; j<NUM_THREAD_IO_WORKER_INTER_SERVER; j++)	{
				Server_qp.pQP_Data[idx+j].ib_pal_lid = pQPair_Inter_FS[idx+j].lid;
				Server_qp.pQP_Data[idx+j].ib_pal_qpn = pQPair_Inter_FS[idx+j].qp_n;
				Server_qp.pQP_Data[idx+j].ib_pal_psn = pQPair_Inter_FS[idx+j].psn;

				Server_qp.pQP_Data[idx+j].rem_key = pQPair_Inter_FS[idx+j].rem_key;
				Server_qp.pQP_Data[idx+j].rem_addr = (uint64_t)(pQPair_Inter_FS[idx+j].rem_addr);
				
				Server_qp.pQP_Data[idx+j].remote_addr_new_msg = pQPair_Inter_FS[idx+j].addr_NewMsgFlag;
				Server_qp.pQP_Data[idx+j].remote_addr_heart_beat = pQPair_Inter_FS[idx+j].addr_TimeHeartBeat;
				Server_qp.pQP_Data[idx+j].remote_addr_IO_CMD = pQPair_Inter_FS[idx+j].addr_IO_Cmd_Msg;

				Server_qp.pQP_Data[idx+j].nPut_Get = 0;
				Server_qp.pQP_Data[idx+j].nPut_Get_Done = 0;
				Server_qp.IB_Modify_QP(Server_qp.pQP_Data[idx+j].queue_pair, Server_qp.pQP_Data[idx+j].ib_my_psn, (uint16_t)(Server_qp.pQP_Data[idx+j].ib_pal_lid), Server_qp.pQP_Data[idx+j].ib_pal_qpn, Server_qp.pQP_Data[idx+j].ib_pal_psn);

				Server_qp.pQP_Data[idx+j].idx_queue = j;	// the first queue is reserved for inter-server communication
			}
		}
	}
	
	free(pQPair_Inter_FS);

	MPI_Barrier(MPI_COMM_WORLD);

	static struct timeval tm;
//	if(mpi_rank == 0)	{
		gettimeofday(&tm, NULL);
		Server_qp.T_Start_us =  (tm.tv_sec + 15)*1000000 + tm.tv_usec;	// 9~10 s delay
//		Server_qp.T_Start_us = Server_qp.T_Start_us - (Server_qp.T_Start_us % 1000000);
//	}

//	MPI_Bcast(&(Server_qp.T_Start_us), sizeof(long int), MPI_CHAR, 0, MPI_COMM_WORLD);

	MPI_Bcast(&(Server_qp.pJob_OP_Recv), sizeof(void*), MPI_CHAR, 0, MPI_COMM_WORLD);
	printf("DBG> Server_qp.pJob_OP_Recv = %p\n", Server_qp.pJob_OP_Recv);
	if(mpi_rank)	Server_qp.pJob_OP_Recv += (mpi_rank);
	printf("DBG> Server_qp.pJob_OP_Recv = %p\n", Server_qp.pJob_OP_Recv);
	MPI_Bcast(&(Server_qp.pJobScale_Remote), sizeof(void*), MPI_CHAR, 0, MPI_COMM_WORLD);

	if(mpi_rank == 0)	Server_qp.pJobScale_Remote->nActiveJob = 0;

	printf("DBG> Rank = %d Finishing Setup_QP_Among_Servers().\n", mpi_rank);*/
}

void Setup_UCX_Among_Servers(void) {
	fprintf(stdout, "DBG> Setup_UCX_Among_Servers begin\n");
	int i, j, idx;

	pUCX_RMA_Inter_FS = (UCX_RMA_SETUP_DATA *)malloc(sizeof(UCX_RMA_SETUP_DATA)*nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER);

	Server_ucx.nQP = 0;	// the qp with other servers are always put at the beginning

	for(i=0; i<nFSServer; i++)	{
		if(i != mpi_rank)	{
			idx = i*NUM_THREAD_IO_WORKER_INTER_SERVER;
			for(j=0; j<NUM_THREAD_IO_WORKER_INTER_SERVER; j++)	{
				Server_ucx.nQP++;
				Server_ucx.AllocateUCPDataWorker(idx+j);
				memcpy(pUCX_RMA_Inter_FS[idx+j].peer_address, Server_ucx.pUCX_Data[idx+j].address_p, Server_ucx.pUCX_Data[idx+j].address_length);
				pUCX_RMA_Inter_FS[idx+j].peer_address_length = Server_ucx.pUCX_Data[idx+j].address_length;
				memcpy(pUCX_RMA_Inter_FS[idx+j].rkey_buffer, Server_ucx.rkey_buffer, Server_ucx.rkey_buffer_size);
				pUCX_RMA_Inter_FS[idx+j].rkey_buffer_size = Server_ucx.rkey_buffer_size;
				pUCX_RMA_Inter_FS[idx+j].rem_addr = (uint64_t)Server_ucx.p_shm_IO_Result_Recv;

				pUCX_RMA_Inter_FS[idx+j].addr_NewMsgFlag = (uint64_t)Server_ucx.p_shm_NewMsgFlag + sizeof(char)*(idx+j);
				pUCX_RMA_Inter_FS[idx+j].addr_TimeHeartBeat = (uint64_t)Server_ucx.p_shm_TimeHeartBeat + sizeof(time_t)*(idx+j);
				pUCX_RMA_Inter_FS[idx+j].addr_IO_Cmd_Msg = (uint64_t)Server_ucx.p_shm_IO_Cmd_Msg + sizeof(IO_CMD_MSG)*(idx+j);
				// fprintf(stdout, "mpi_rank %d: pUCX_RMA_Inter_FS[%d].addr_IO_Cmd_Msg = %p\n", mpi_rank, idx+j, (uint64_t)Server_ucx.p_shm_IO_Cmd_Msg + sizeof(IO_CMD_MSG)*(idx+j));
			}
		}
	}
	Server_ucx.FirstAV_QP = nFSServer * NUM_THREAD_IO_WORKER_INTER_SERVER;
	Server_ucx.IdxLastQP = nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER - 1;
	Server_ucx.IdxLastQP64 = Align64_Int(Server_ucx.IdxLastQP+1);	// +1 is needed since IdxLastQP is included!

	MPI_Alltoall(MPI_IN_PLACE, sizeof(UCX_RMA_SETUP_DATA)*NUM_THREAD_IO_WORKER_INTER_SERVER, MPI_CHAR, pUCX_RMA_Inter_FS, sizeof(UCX_RMA_SETUP_DATA)*NUM_THREAD_IO_WORKER_INTER_SERVER, MPI_CHAR, MPI_COMM_WORLD);
	ucs_status_t status;
	for(i=0; i<nFSServer; i++)	{
		if(i != mpi_rank)	{
			idx = i*NUM_THREAD_IO_WORKER_INTER_SERVER;
			for(j=0; j<NUM_THREAD_IO_WORKER_INTER_SERVER; j++)	{

				// Server_ucx.pUCX_Data[idx+j].peer_ep = pUCX_RMA_Inter_FS[idx+j].rem_key;
				// Server_ucx.pUCX_Data[idx+j].rkey = pUCX_RMA_Inter_FS[idx+j].rem_key;
				Server_ucx.server_create_ep(Server_ucx.pUCX_Data[idx+j].ucp_data_worker, (ucp_address_t*)pUCX_RMA_Inter_FS[idx+j].peer_address, &Server_ucx.pUCX_Data[idx+j].peer_ep);
				status = ucp_ep_rkey_unpack(Server_ucx.pUCX_Data[idx+j].peer_ep, pUCX_RMA_Inter_FS[idx+j].rkey_buffer, &Server_ucx.pUCX_Data[idx+j].rkey);
				assert(status == UCS_OK);
				Server_ucx.pUCX_Data[idx+j].rem_addr = (uint64_t)(pUCX_RMA_Inter_FS[idx+j].rem_addr);
				
				Server_ucx.pUCX_Data[idx+j].remote_addr_new_msg = pUCX_RMA_Inter_FS[idx+j].addr_NewMsgFlag;
				Server_ucx.pUCX_Data[idx+j].remote_addr_heart_beat = pUCX_RMA_Inter_FS[idx+j].addr_TimeHeartBeat;
				Server_ucx.pUCX_Data[idx+j].remote_addr_IO_CMD = pUCX_RMA_Inter_FS[idx+j].addr_IO_Cmd_Msg;

				Server_ucx.pUCX_Data[idx+j].nPut_Get = 0;
				Server_ucx.pUCX_Data[idx+j].nPut_Get_Done = 0;
				Server_ucx.pUCX_Data[idx+j].bTimeout = 0;

				Server_ucx.pUCX_Data[idx+j].idx_queue = j;	// the first queue is reserved for inter-server communication
				// fprintf(stdout, "mpi_rank %d: Server_ucx.pUCX_Data[%d].remote_addr_IO_CMD = %p\n", mpi_rank, idx+j, Server_ucx.pUCX_Data[idx+j].remote_addr_IO_CMD);
			}
		}
	}
	
	free(pUCX_RMA_Inter_FS);

	MPI_Barrier(MPI_COMM_WORLD);

	static struct timeval tm;
//	if(mpi_rank == 0)	{
		gettimeofday(&tm, NULL);
		Server_ucx.T_Start_us =  (tm.tv_sec + 15)*1000000 + tm.tv_usec;	// 9~10 s delay
//		Server_qp.T_Start_us = Server_qp.T_Start_us - (Server_qp.T_Start_us % 1000000);
//	}

//	MPI_Bcast(&(Server_qp.T_Start_us), sizeof(long int), MPI_CHAR, 0, MPI_COMM_WORLD);

	MPI_Bcast(&(Server_ucx.pJob_OP_Recv), sizeof(void*), MPI_CHAR, 0, MPI_COMM_WORLD);
	printf("DBG> Server_ucx.pJob_OP_Recv = %p\n", Server_ucx.pJob_OP_Recv);
	if(mpi_rank)	Server_ucx.pJob_OP_Recv += (mpi_rank);
	printf("DBG> Server_ucx.pJob_OP_Recv = %p\n", Server_ucx.pJob_OP_Recv);
	MPI_Bcast(&(Server_ucx.pJobScale_Remote), sizeof(void*), MPI_CHAR, 0, MPI_COMM_WORLD);

	if(mpi_rank == 0)	Server_ucx.pJobScale_Remote->nActiveJob = 0;

	printf("DBG> Rank = %d Finishing Setup_UCX_Among_Servers().\n", mpi_rank);
}

void Get_Local_Server_Info(void)
{
	int fd;
	struct ifreq ifr;
	
	fd = socket(AF_INET, SOCK_DGRAM, 0);
	ifr.ifr_addr.sa_family = AF_INET;
	strncpy(ifr.ifr_name, IB_DEVICE, strlen(IB_DEVICE)+1);
	ioctl(fd, SIOCGIFADDR, &ifr);
	close(fd);
	ThisNode.sin_addr = ((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr;
	sprintf(ThisNode.szIP, "%s", inet_ntoa(ThisNode.sin_addr));
	ThisNode.port = PORT + (mpi_rank % nNUMAPerNode);
	ThisNode.ucx_port = UCX_PORT + (mpi_rank % nNUMAPerNode);
}
//static struct timeval tm1, tm2;

static void* Func_thread_Print_Data(void *pParam)
{
	// SERVER_QUEUEPAIR *pServer_qp;
	SERVER_RDMA* pServer_ucx;
	struct timeval tm1;	// tm1.tv_sec
	
	pServer_ucx = (SERVER_RDMA *)pParam;
	while(1)	{
		sleep(1);
		if(pServer_ucx->pUCX_Data)	{
			if( (pServer_ucx->pUCX_Data[0].ucp_data_worker) || (pServer_ucx->pUCX_Data[1].ucp_data_worker) )	{
				break;
			}
		}
	}

	sleep(1);
//	printf("DBG> Rank = %d. pServer_qp->IdxLastQP = %d\n", mpi_rank, pServer_qp->IdxLastQP);

	while(1)	{
		gettimeofday(&tm1, NULL);
		for(int j=0; j<=pServer_ucx->IdxLastQP; j++)	{
			if(pServer_ucx->p_shm_TimeHeartBeat[j])	{
//				printf("DBG> Addr pServer_qp->p_shm_TimeHeartBeat[0] = %p Index(QP) = %d\n", &(pServer_qp->p_shm_TimeHeartBeat[j]), j);
//				printf("Rank %d: heart beat time stamp %ld My time %ld\n", j, pServer_qp->p_shm_TimeHeartBeat[j], tm1.tv_sec);
			}
		}
		pServer_ucx->ScanLostUCX();
		sleep(1);
	}

	return NULL;
}
/*
#define UCX_Rma_Put_Test_String "From %d to %d times %d"
#define UCX_Rma_Put_Checkpass_String "From %d to %d times %d pass put check\n"
#define UCX_Rma_Put_Checkfail_String "From %d to %d times %d fail put check\n"
#define UCX_Rma_Put_Test_String_Len 30  

static void* UCX_Rma_Put_Test(void* pParam) {
	SERVER_RDMA* pServer_RDMA;
	pServer_RDMA = (SERVER_RDMA*)pParam;
	size_t test_len = UCX_Rma_Put_Test_String_Len;
	fprintf(stdout, "mpi_rank: %d UCX_Rma_Put_Test begin\n", mpi_rank);
	for(int i=0; i<nFSServer; i++)	{
		if(i != mpi_rank)	{
			int idx = i*NUM_THREAD_IO_WORKER_INTER_SERVER;
			for(int j=0; j<NUM_THREAD_IO_WORKER_INTER_SERVER; j++)	{
				char* test_buff = (char*)malloc(test_len);
				// fprintf(stdout, "mpi_rank %d: UCX_Put From %d to %d times %d\n", mpi_rank,  mpi_rank, i, j);
				sprintf(test_buff, UCX_Rma_Put_Test_String, mpi_rank, i, j);
				uint64_t rem_addr = pServer_RDMA->pUCX_Data[idx+j].remote_addr_IO_CMD;
				ucp_rkey_h rkey = pUCX_Data[idx + j].rkey;
				pServer_RDMA->UCX_Put(idx + j, test_buff, (void*)rem_addr, test_len);
				free(test_buff);
			}
		}
	}

}
static void* UCX_Rma_Put_Test_Check(void* pParam) {
	fprintf(stdout, "mpi_rank: %d UCX_Rma_Put_Test_Check begin\n", mpi_rank);
	SERVER_RDMA* pServer_RDMA;
	pServer_RDMA = (SERVER_RDMA*)pParam;
	size_t test_len = UCX_Rma_Put_Test_String_Len;
	for(int i=0; i<nFSServer; i++)	{
		if(i != mpi_rank)	{
			int idx = i*NUM_THREAD_IO_WORKER_INTER_SERVER;
			for(int j=0; j<NUM_THREAD_IO_WORKER_INTER_SERVER; j++)	{
				char* check_buff = (char*)((uint64_t)Server_ucx.p_shm_IO_Cmd_Msg + sizeof(IO_CMD_MSG)*(idx+j));
				char* correct_buff = (char*)malloc(test_len);
				sprintf(correct_buff, UCX_Rma_Put_Test_String, i, mpi_rank, j);
				fprintf(stdout, "mpi_rank %d: UCX_Put Check From %d to %d times %d addr %p\n", mpi_rank, i, mpi_rank, j, check_buff);
				if(strcmp(check_buff, correct_buff) == 0) {
					fprintf(stdout, UCX_Rma_Put_Checkpass_String, i, mpi_rank, j);
				} else {
					fprintf(stdout, UCX_Rma_Put_Checkfail_String, i, mpi_rank, j);
					fprintf(stdout, "Expected: %s\n", correct_buff);
					fprintf(stdout, "Actual: %s\n", check_buff);
				}
				free(correct_buff);
			}
		}
	}
}

#define UCX_Rma_Get_Test_String "From %d to %d times %d"
#define UCX_Rma_Get_Checkpass_String "From %d to %d times %d pass get check\n"
#define UCX_Rma_Get_Checkfail_String "From %d to %d times %d fail get check\n"
#define UCX_Rma_Get_Test_String_Len 30  
static void* UCX_Rma_Get_Test(void* pParam, void** loc_pptr) {
	SERVER_RDMA* pServer_RDMA;
	pServer_RDMA = (SERVER_RDMA*)pParam;
	size_t test_len = UCX_Rma_Get_Test_String_Len;
	*loc_pptr = malloc(nFSServer * NUM_THREAD_IO_WORKER_INTER_SERVER * UCX_Rma_Get_Test_String_Len);
	memset(*loc_pptr, 0, nFSServer * NUM_THREAD_IO_WORKER_INTER_SERVER * UCX_Rma_Get_Test_String_Len);
	fprintf(stdout, "mpi_rank: %d UCX_Rma_Get_Test begin\n", mpi_rank);
	for(int i=0; i<nFSServer; i++)	{
		if(i != mpi_rank)	{
			int idx = i*NUM_THREAD_IO_WORKER_INTER_SERVER;
			for(int j=0; j<NUM_THREAD_IO_WORKER_INTER_SERVER; j++)	{
				uint64_t loc_buf = (uint64_t)(*loc_pptr) + (idx + j) * UCX_Rma_Get_Test_String_Len;
				uint64_t rem_addr = pServer_RDMA->pUCX_Data[idx+j].remote_addr_IO_CMD;
				ucp_rkey_h rkey = pUCX_Data[idx + j].rkey;
				pServer_RDMA->UCX_Get(idx + j, (void*)loc_buf, (void*)rem_addr, rkey, test_len);
			}
		}
	}

}
static void* UCX_Rma_Get_Test_Check(void* pParam, void* loc_ptr) {
	fprintf(stdout, "mpi_rank: %d UCX_Rma_Get_Test_Check begin\n", mpi_rank);
	SERVER_RDMA* pServer_RDMA;
	pServer_RDMA = (SERVER_RDMA*)pParam;
	size_t test_len = UCX_Rma_Get_Test_String_Len;
	for(int i=0; i<nFSServer; i++)	{
		if(i != mpi_rank)	{
			int idx = i*NUM_THREAD_IO_WORKER_INTER_SERVER;
			for(int j=0; j<NUM_THREAD_IO_WORKER_INTER_SERVER; j++)	{
				char* check_buff = (char*)((uint64_t)(loc_ptr) + (idx + j) * UCX_Rma_Get_Test_String_Len);
				char* correct_buff = (char*)malloc(test_len);
				sprintf(correct_buff, UCX_Rma_Get_Test_String, mpi_rank, i, j);
				fprintf(stdout, "mpi_rank %d: UCX_Get Check From %d to %d times %d addr %p\n", mpi_rank, mpi_rank, i, j, check_buff);
				if(strcmp(check_buff, correct_buff) == 0) {
					fprintf(stdout, UCX_Rma_Get_Checkpass_String, mpi_rank, i, j);
				} else {
					fprintf(stdout, UCX_Rma_Get_Checkfail_String, mpi_rank, i, j);
					fprintf(stdout, "Expected: %s\n", correct_buff);
					fprintf(stdout, "Actual: %s\n", check_buff);
				}
				free(correct_buff);
			}
		}
	}
	free(loc_ptr);
}
*/
static void* Func_thread_UCX_Polling_New_Msg(void *pParam)
{
	SERVER_RDMA *pServer_ucx;
	CoreBinding.Bind_This_Thread();

	pServer_ucx = (SERVER_RDMA *)pParam;
	while(1)	{
		sleep(1);
		if(pServer_ucx->pUCX_Data)	{
			break;
		}
	}

	sleep(1);

	while(1)	{
		pServer_ucx->ScanNewMsg();
	}

	return NULL;
}

static void* Func_thread_Polling_New_Msg(void *pParam)
{
	/*SERVER_QUEUEPAIR *pServer_qp;
	
	CoreBinding.Bind_This_Thread();

	pServer_qp = (SERVER_QUEUEPAIR *)pParam;
	while(1)	{
		sleep(1);
		if(pServer_qp->pQP_Data)	{
			break;
		}
	}

	sleep(1);

	while(1)	{
		pServer_qp->ScanNewMsg();
	}

	return NULL;*/
}

void* Func_thread_UCX_Worker_Progress(void* pParam) {
	ucp_worker_h data_worker = *((ucp_worker_h*)pParam);
	while(true && data_worker != NULL) {
		ucp_worker_progress(data_worker);
		// sleep(1);
	}
	return NULL;
}

static void* Func_thread_ucx_server(void *pParam) {
	SERVER_RDMA *pServer_ucx;
	int i;
	int IO_Worker_tid_List[NUM_THREAD_IO_WORKER];

	pServer_ucx = (SERVER_RDMA *)pParam;
	pServer_ucx->Init_Server_UCX_Env(DEFAULT_REM_BUFF_SIZE);
	pServer_ucx->Init_Server_Memory(2048, ThisNode.ucx_port);
	
	Init_ActiveJobList();
	Init_QueueList();

	for(i=0; i<NUM_THREAD_IO_WORKER; i++)	{
		IO_Worker_tid_List[i] = i;
		if(pthread_create(&(pthread_IO_Worker_UCX[i]), NULL, Func_thread_IO_Worker, &(IO_Worker_tid_List[i]))) {
			fprintf(stderr, "Error creating thread\n");
			return 0;
		}
	}

	for(i=0; i<NUM_THREAD_IO_WORKER; i++)	{
		if(pthread_create(&(thread_ucx_worker_progress[i]), NULL, Func_thread_UCX_Worker_Progress, &(pServer_ucx->ucp_data_worker[i]))) {
			fprintf(stderr, "Error creating thread\n");
			return 0;
		}
	}

	Ucx_Server_Started = 1;	// active the flag: Server started running!!!
	printf("Rank = %d. UCX Server is started.\n", mpi_rank);
	pServer_ucx->Socket_Server_Loop();
	
	return 0;
}

static void* Func_thread_qp_server(void *pParam)
{
// 	SERVER_QUEUEPAIR *pServer_qp;
// 	int i;
// 	int IO_Worker_tid_List[NUM_THREAD_IO_WORKER];

// 	pServer_qp = (SERVER_QUEUEPAIR *)pParam;
// 	pServer_qp->Init_Server_IB_Env(DEFAULT_REM_BUFF_SIZE);
// 	pServer_qp->Init_Server_Socket(2048, ThisNode.port);

// 	Init_ActiveJobList();
// 	Init_QueueList();
// /*
// 	Init_PreAllocated_QueuePair_List();

// 	for(i=0; i<N_THREAD_PREALLOCATE_QP; i++)	{
// 		pParam_PreAllocate[i].pServer_qp = pServer_qp;
// 		pParam_PreAllocate[i].t_rank = i;
// 		pParam_PreAllocate[i].nthread = N_THREAD_PREALLOCATE_QP;
// 		pParam_PreAllocate[i].nToken = Unique_Thread.Apply_A_Token();
// 		if(pthread_create(&(pthread_preallocate[i]), NULL, Func_thread_PreAllocate_QueuePair, &(pParam_PreAllocate[i]))) {
// 			fprintf(stderr, "Error creating thread\n");
// 			return 0;
// 		}
// 	}
// 	for(i=0; i<N_THREAD_PREALLOCATE_QP; i++)	{
// 		if(pthread_join(pthread_preallocate[i], NULL)) {
// 			fprintf(stderr, "Error joining thread.\n");
// 			return 0;
// 		}
// 	}
// */
// 	for(i=0; i<NUM_THREAD_IO_WORKER; i++)	{
// 		IO_Worker_tid_List[i] = i;
// 		if(pthread_create(&(pthread_IO_Worker[i]), NULL, Func_thread_IO_Worker, &(IO_Worker_tid_List[i]))) {
// 			fprintf(stderr, "Error creating thread\n");
// 			return 0;
// 		}
// 	}

// 	Server_Started = 1;	// active the flag: Server started running!!!
// 	printf("Rank = %d. Server is started.\n", mpi_rank);
// 	pServer_qp->Socket_Server_Loop();
	
// 	return 0;
}

extern long int nOPs_Done[NUM_THREAD_IO_WORKER];
static long int nOPs_Done_Sum=0;
static int T_Cur=0;

void sigalarm_handler(int signum)
{
	int i;
	long int nOPs_Done_Sum_New=0;
	double iops;
	
	for(i=0; i<NUM_THREAD_IO_WORKER; i++)	{
		nOPs_Done_Sum_New += nOPs_Done[i];
	}
	if(T_Cur > 0)	{
		iops = 0.000001*(nOPs_Done_Sum_New - nOPs_Done_Sum)/T_FREQ_REPORT_RESULT;
		if(iops > 0.05)	{
			printf("INFO> Reporting performance %5.3lf M iops T = %d\n", iops, T_Cur);
		}
	}
	T_Cur += T_FREQ_REPORT_RESULT;
	nOPs_Done_Sum = nOPs_Done_Sum_New;

//	printf("---------------------------------- nOP_Done\n");
//	for(i=0; i<nActiveJob; i++)	{
//		printf("INFO> jobid %d  nOP_Done = %ld\n", IdxJobRecList[i].jobid, ActiveJobList[IdxJobRecList[i].idx_rec_ht].nOps_Done);
//	}
/*
	printf("---------------------------------- Queue info\n");
	for(i=1; i<MAX_NUM_QUEUE; i++)	{
		if(IO_Queue_List[i].back>=0)	{
			printf("INFO> Queue %d (%8ld,%8ld)\n", i, IO_Queue_List[i].front, IO_Queue_List[i].back);
		}
	}
*/	
	alarm(T_FREQ_REPORT_RESULT);
}


/*
inline void Send_IO_Request(int idx_fs)
{
	IO_CMD_MSG *pIO_Cmd = (IO_CMD_MSG *)loc_buff;
	int bTimeout;

	while(1)	{
		// send the IO request first
		pIO_Cmd->tag_magic = rand();
		pClient_qp[idx_fs]->IB_Put(loc_buff, pClient_qp[idx_fs]->mr_loc->lkey, (void*)(pClient_qp[idx_fs]->remote_addr_IO_CMD + sizeof(IO_CMD_MSG)*pClient_qp[idx_fs]->Idx_fs), pClient_qp[idx_fs]->pal_remote_mem.key, sizeof(IO_CMD_MSG));

		// send a msg to notify that a new IO quest is coming.
		loc_buff[0] = TAG_NEW_REQUEST;
		pClient_qp[idx_fs]->IB_Put(loc_buff, pClient_qp[idx_fs]->mr_loc->lkey, (void*)(pClient_qp[idx_fs]->remote_addr_new_msg + sizeof(char)*pClient_qp[idx_fs]->Idx_fs), pClient_qp[idx_fs]->pal_remote_mem.key, 1);

		bTimeout = Wait_For_IO_Request_Result(pIO_Cmd->tag_magic);
		if(bTimeout==0)	break;
		if(pIO_Cmd->op == RF_RW_OP_DISCONNECT)	break;	// NEVER send multiple RF_RW_OP_DISCONNECT command!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	}
}
*/


static void sigsegv_handler(int sig, siginfo_t *siginfo, void *uc)
{
	char szMsg[256];

	sprintf(szMsg, "\n\n\n\n\n\n\n\n\nGot signal %d (SIGSEGV) rank = %d tid = %d\n\n\n\n\n\n\n", siginfo->si_signo, mpi_rank, syscall(SYS_gettid));
	write(STDERR_FILENO, szMsg, strlen(szMsg));
	fsync(STDERR_FILENO);
	sleep(3000);
//	if(org_segv)	org_segv(sig, siginfo, uc);
//	else	exit(1);
}

typedef void (*org_sighandler)(int sig, siginfo_t *siginfo, void *ptr);
static org_sighandler org_int=NULL;

static void sigint_handler(int sig, siginfo_t *siginfo, void *uc)
{
	char szMsg[]="Received sigint.\n";
	write(STDERR_FILENO, szMsg, strlen(szMsg));
	fsync(STDERR_FILENO);

        if(org_int)     org_int(sig, siginfo, uc);
        else    exit(0);
}


int main(int argc, char **argv)
{
	int i;
	FILE *fOut;
	pthread_t thread_qp_server, thread_polling_newmsg/*, thread_global_sharing, thread_print_data*/;
	pthread_t thread_ucx_polling_newmsg, thread_ucx_server;
	
	// pthread_t thread_ucx_test;
//	unsigned char *pNewMsg_ToSend=NULL;
//	IO_CMD_MSG *pIO_Cmd_toSend;
//	struct ibv_mr *mr_local;


	struct sigaction act, old_action;
	
    // Set up sigsegv handler
    memset (&act, 0, sizeof(act));
    act.sa_flags = SA_SIGINFO;
	
    act.sa_sigaction = sigsegv_handler;
    if (sigaction(SIGSEGV, &act, &old_action) == -1) {
        perror("Error: sigaction");
        exit(1);
    }

    act.sa_sigaction = sigint_handler;
    if (sigaction(SIGINT, &act, &old_action) == -1) {
        perror("Error: sigaction");
       exit(1);
    }
    if( (old_action.sa_handler != SIG_DFL) && (old_action.sa_handler != SIG_IGN) )  {
            org_int = old_action.sa_sigaction;
    }


	CoreBinding.Init_Core_Binding();
	Unique_Thread.Init_UniqueThread();
	
	pthread_attr_init(&thread_attr);
	pthread_attr_setdetachstate(&thread_attr, PTHREAD_CREATE_DETACHED);

	MPI_Init(NULL, NULL);
	MPI_Comm_size(MPI_COMM_WORLD, &nFSServer);
	MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);


	ServerOptions server_options;
	if (!server_options.parseCommandLineArgs(argc, argv)) {
		ServerOptions::printHelp();
		MPI_Finalize();
		return 1;
	}

	// Store the fairness_mode in Server_qp.  It will be used in Func_thread_IO_Worker_FairQueue.
	// Server_qp.fairness_mode = server_options.getFairnessMode();
	Server_ucx.fairness_mode = server_options.getFairnessMode();
	Init_Memory();
//	Test_File_System_Local();

	Get_Local_Server_Info();
	MPI_Allgather(&ThisNode, sizeof(FS_SEVER_INFO), MPI_CHAR, AllFSNodes, sizeof(FS_SEVER_INFO), MPI_CHAR, MPI_COMM_WORLD);

	if(mpi_rank == 0)	{
		/*printf("INFO> There are %d servers.\n", nFSServer);
		fOut = fopen(FS_PARAM_FILE, "w");
		if(fOut == NULL)	{
			printf("ERROR> Fail to open file: %s\nQuit.\n", FS_PARAM_FILE);
			exit(1);
		}
		fprintf(fOut, "%d %d\n", nFSServer, nNUMAPerNode);
		for(i=0; i<nFSServer; i++)	{
			printf("     %d %s %d\n", i, AllFSNodes[i].szIP, AllFSNodes[i].port);
			fprintf(fOut, "%s %d\n", AllFSNodes[i].szIP, AllFSNodes[i].port);
		}
		fclose(fOut);*/

		printf("INFO> There are %d servers.\n", nFSServer);
		fOut = fopen(UCX_FS_PARAM_FILE, "w");
		if(fOut == NULL)	{
			printf("ERROR> Fail to open file: %s\nQuit.\n", UCX_FS_PARAM_FILE);
			exit(1);
		}
		fprintf(fOut, "%d %d\n", nFSServer, nNUMAPerNode);
		for(i=0; i<nFSServer; i++)	{
			printf("     %d %s %d\n", i, AllFSNodes[i].szIP, AllFSNodes[i].ucx_port);
			fprintf(fOut, "%s %d\n", AllFSNodes[i].szIP, AllFSNodes[i].ucx_port);
		}
		fclose(fOut);
	}

	// if(pthread_create(&(thread_qp_server), NULL, Func_thread_qp_server, &Server_qp)) {
	// 	fprintf(stderr, "Error creating thread\n");
	// 	return 1;
	// }
	// UCX_TEST
	if(pthread_create(&(thread_ucx_server), NULL, Func_thread_ucx_server, &Server_ucx)) {
		fprintf(stderr, "Error creating thread\n");
		return 1;
	}

//	Setup_Signal_QueuePair();
	// while(1)	{
	// 	if(Server_Started)	break;
	// }
	
	// Setup_QP_Among_Servers();

	while(1)	{
		if(Ucx_Server_Started)	break;
	}
	MPI_Barrier(MPI_COMM_WORLD);
	Setup_UCX_Among_Servers();
	// UCX_Rma_Put_Test(&Server_ucx);
	// MPI_Barrier(MPI_COMM_WORLD);
	// UCX_Rma_Put_Test_Check(&Server_ucx);
	// void* ucx_rma_get_test_buff = NULL;
	// UCX_Rma_Get_Test(&Server_ucx, &ucx_rma_get_test_buff);
	// MPI_Barrier(MPI_COMM_WORLD);
	// UCX_Rma_Get_Test_Check(&Server_ucx, ucx_rma_get_test_buff);

//	if(pthread_create(&(thread_global_sharing), NULL, Func_thread_Global_Fair_Sharing, &Server_qp)) {
//		fprintf(stderr, "Error creating thread\n");
//		return 1;
//	}

//	if(pthread_create(&(thread_print_data), NULL, Func_thread_Print_Data, &Server_qp)) {
//		fprintf(stderr, "Error creating thread\n");
//		return 1;
//	}

	// if(pthread_create(&(thread_polling_newmsg), NULL, Func_thread_Polling_New_Msg, &Server_qp)) {
	// 	fprintf(stderr, "Error creating thread\n");
	// 	return 1;
	// }
	// printf("DBG> Rank = %d,  started Func_thread_Polling_New_Msg().\n", mpi_rank);
	
	if(pthread_create(&(thread_ucx_polling_newmsg), NULL, Func_thread_UCX_Polling_New_Msg, &Server_ucx)) {
		fprintf(stderr, "Error creating thread thread_ucx_polling_newmsg\n");
		return 1;
	}
	printf("DBG> Rank = %d,  started Func_thread_UCX_Polling_New_Msg().\n", mpi_rank);
	
	signal(SIGALRM, sigalarm_handler); // Register signal handler
	alarm(T_FREQ_REPORT_RESULT);

	while(Server_Started == 0)	{
		usleep(10000);
	}
	if( nFSServer>1 )	Query_Other_Server( (mpi_rank + 1) % nFSServer );

/*
	pNewMsg_ToSend = (unsigned char*)malloc(sizeof(IO_CMD_MSG));
	assert(pNewMsg_ToSend != NULL);
	pIO_Cmd_toSend = (IO_CMD_MSG *)pNewMsg_ToSend;
	mr_local = Server_qp.IB_RegisterBuf_RW_Local_Remote(pNewMsg_ToSend, sizeof(IO_CMD_MSG));
	assert(mr_local != NULL);
	
	sleep(2);
	int idx_TargetServer;
	RW_FUNC_RETURN *pResult;

	i = mpi_rank;
	idx_TargetServer = (i+1)%nFSServer;
	pIO_Cmd_toSend->rem_buff = (void*)(Server_qp.p_shm_IO_Result_Recv);
	pIO_Cmd_toSend->rkey = Server_qp.mr_shm_global->rkey;
	pIO_Cmd_toSend->tag_magic = rand();
	pIO_Cmd_toSend->op = IO_OP_MAGIC | RF_RW_OP_HELLO;

	pResult = (RW_FUNC_RETURN *)pIO_Cmd_toSend->rem_buff;
	pResult->nDataSize = 0; // init with an invalid tag. When return, this should be sizeof(RW_FUNC_RETURN) added with extra data.

//Server_qp.IB_Put(idx_TargetServer, (void*)(&(pIO_Cmd_toSend[mpi_rank])), mr_local->lkey, (void*)(Server_qp.pQP_Data[idx_TargetServer].remote_addr_IO_CMD + sizeof(IO_CMD_MSG)*idx_TargetServer), Server_qp.pQP_Data[idx_TargetServer].rem_key, sizeof(IO_CMD_MSG));
	Server_qp.IB_Put(idx_TargetServer, (void*)pIO_Cmd_toSend, mr_local->lkey, (void*)(Server_qp.pQP_Data[idx_TargetServer].remote_addr_IO_CMD), Server_qp.pQP_Data[idx_TargetServer].rem_key, sizeof(IO_CMD_MSG));
	pNewMsg_ToSend[0] = 0x80;	// new msg!
	Server_qp.IB_Put(idx_TargetServer, (void*)pIO_Cmd_toSend, mr_local->lkey, (void*)(Server_qp.pQP_Data[idx_TargetServer].remote_addr_new_msg), Server_qp.pQP_Data[idx_TargetServer].rem_key, 1);

	Wait_For_IO_Request_Result(pIO_Cmd_toSend->tag_magic, (RW_FUNC_RETURN*)(pIO_Cmd_toSend->rem_buff));
	printf("DBG> Rank = %d result = %d\n", mpi_rank, pResult->ret_value);
*/
	
	// if(pthread_join(thread_polling_newmsg, NULL)) {
	// 	fprintf(stderr, "Error joining thread.\n");
	// 	return 2;
	// }
	if(pthread_join(thread_ucx_polling_newmsg, NULL)) {
		fprintf(stderr, "Error joining thread.\n");
		return 2;
	}

//	if(pthread_join(thread_print_data, NULL)) {
//		fprintf(stderr, "Error joining thread.\n");
//		return 2;
//	}

	// if(pthread_join(thread_qp_server, NULL)) {
	// 	fprintf(stderr, "Error joining thread.\n");
	// 	return 2;
	// }

	if(pthread_join(thread_ucx_server, NULL)) {
		fprintf(stderr, "Error joining thread thread_ucx_server.\n");
		return 2;
	}


    MPI_Finalize();
	
	return 0;
}


bool ServerOptions::parseCommandLineArgs(int argc, char **argv) {
	fairness_mode = getDefaultFairnessMode();

	for (int argno = 1; argno < argc; argno++) {
		const char *arg = argv[argno];

		if (!strcmp(arg, "-h") || !strcmp(arg, "--help")) {
			printHelp();
			return false;
		}

		else if (!strcmp(arg, "--policy")) {
			if (argno+1 >= argc) return false;
			arg = argv[++argno];
			if (!strcmp(arg, "fifo")) {
				fairness_mode = FIFO;
			} else if (!strcmp(arg, "user-fair")) {
				fairness_mode = USER_FAIR;
			} else if (!strcmp(arg, "job-fair")) {
				fairness_mode = JOB_FAIR;
			} else if (!strcmp(arg, "size-fair")) {
				fairness_mode = SIZE_FAIR;

			/* disabled */
			/*
			} else if (!strcmp(arg, "user-size-fair")) {
				fairness_mode = USER_SIZE_FAIR;
			} else if (!strcmp(arg, "user-job-fair")) {
				fairness_mode = USER_JOB_FAIR;
			} else if (!strcmp(arg, "group-user-size-fair")) {
				fairness_mode = GROUP_USER_SIZE_FAIR;
			*/

			} else {
				printf("Policy mode urecognized: %s\n", arg);
				return false;
			}
		}

		else {
			return false;
		}
	}

	return true;
}


void ServerOptions::printHelp() {
	if (mpi_rank != 0) return;
	
	printf("\n"
				 "  server [<opts>]\n"
				 "  opts:\n"
				 "    --policy fifo|user-fair|job-fair|size-fair\n"
				 "      Sets client throughput fairness policy. default=%s\n"
				 "\n",
				 fairnessModeToString(getDefaultFairnessMode()));
}

		
		
