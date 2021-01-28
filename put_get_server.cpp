// g++ -march=skylake-avx512 -g -O0 -o server put_get_server.cpp -libverbs -lpthread -lrt -Wunused-variable -I/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/include -L/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/lib/release -L/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/lib -lmpicxx -lmpifort -lmpi -ldl

#include <cassert>
#include <cerrno>
#include <cstdio>
#include "qp.h"

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

#include <mpi.h> 

#define PORT 8888

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
	char szIP[16];
}FS_SEVER_INFO;

int Server_Started=0;
FS_SEVER_INFO ThisNode;
FS_SEVER_INFO AllFSNodes[MAX_FS_SERVER];
QPAIR_DATA *pQPair_Inter_FS=NULL;

int mpi_rank, nFSServer=0;	// rank and size of MPI
int nNUMAPerNode=2;	// number of numa nodes per compute node

SERVER_QUEUEPAIR Server_qp;

void Get_Local_Server_Info(void);
void Setup_QP_Among_Servers(void);

void Setup_QP_Among_Servers(void)
{
	int i;

	pQPair_Inter_FS = (QPAIR_DATA *)malloc(sizeof(QPAIR_DATA)*nFSServer);

	Server_qp.nQP = 0;	// the qp with other servers are always put at the beginning

	for(i=0; i<nFSServer; i++)	{
		if(i != mpi_rank)	{
			Server_qp.nQP++;
			Server_qp.IB_CreateQueuePair(i);
			pQPair_Inter_FS[i].lid = Server_qp.pQP_Data[i].ib_my_lid;
			pQPair_Inter_FS[i].qp_n = Server_qp.pQP_Data[i].ib_my_qpn;
			pQPair_Inter_FS[i].psn = Server_qp.pQP_Data[i].ib_my_psn;

			pQPair_Inter_FS[i].rem_key = Server_qp.mr_shm_global->rkey;
			pQPair_Inter_FS[i].rem_addr = (uint64_t)Server_qp.mr_shm_global->addr;

			pQPair_Inter_FS[i].addr_NewMsgFlag = (uint64_t)Server_qp.p_shm_NewMsgFlag + sizeof(char)*i;
			pQPair_Inter_FS[i].addr_TimeHeartBeat = (uint64_t)Server_qp.p_shm_TimeHeartBeat + sizeof(time_t)*i;
			pQPair_Inter_FS[i].addr_IO_Cmd_Msg = (uint64_t)Server_qp.p_shm_IO_Cmd_Msg + sizeof(IO_CMD_MSG)*i;
		}
	}
	Server_qp.FirstAV_QP = nFSServer;
	if( mpi_rank == (nFSServer-1) )	{	// the last one
		Server_qp.IdxLastQP = nFSServer-2;
	}
	else	{
		Server_qp.IdxLastQP = nFSServer-1;
	}
	Server_qp.IdxLastQP64 = Align64_Int(Server_qp.IdxLastQP+1);	// +1 is needed since IdxLastQP is included!


	MPI_Alltoall(MPI_IN_PLACE, sizeof(QPAIR_DATA), MPI_CHAR, pQPair_Inter_FS, sizeof(QPAIR_DATA), MPI_CHAR, MPI_COMM_WORLD);

	for(i=0; i<nFSServer; i++)	{
		if(i != mpi_rank)	{
			Server_qp.pQP_Data[i].ib_pal_lid = pQPair_Inter_FS[i].lid;
			Server_qp.pQP_Data[i].ib_pal_qpn = pQPair_Inter_FS[i].qp_n;
			Server_qp.pQP_Data[i].ib_pal_psn = pQPair_Inter_FS[i].psn;

			Server_qp.pQP_Data[i].rem_key = pQPair_Inter_FS[i].rem_key;
			Server_qp.pQP_Data[i].rem_addr = (uint64_t)(pQPair_Inter_FS[i].rem_addr);
			
			Server_qp.pQP_Data[i].remote_addr_new_msg = pQPair_Inter_FS[i].addr_NewMsgFlag;
			Server_qp.pQP_Data[i].remote_addr_heart_beat = pQPair_Inter_FS[i].addr_TimeHeartBeat;
			Server_qp.pQP_Data[i].remote_addr_IO_CMD = pQPair_Inter_FS[i].addr_IO_Cmd_Msg;

			Server_qp.pQP_Data[i].nPut_Get = 0;
			Server_qp.pQP_Data[i].nPut_Get_Done = 0;
			Server_qp.IB_Modify_QP(Server_qp.pQP_Data[i].queue_pair, Server_qp.pQP_Data[i].ib_my_psn, (uint16_t)(Server_qp.pQP_Data[i].ib_pal_lid), Server_qp.pQP_Data[i].ib_pal_qpn, Server_qp.pQP_Data[i].ib_pal_psn);

		}
	}
	
	free(pQPair_Inter_FS);
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
}
//static struct timeval tm1, tm2;

static void* Func_thread_Print_Data(void *pParam)
{
	SERVER_QUEUEPAIR *pServer_qp;
	struct timeval tm1;	// tm1.tv_sec
	
	pServer_qp = (SERVER_QUEUEPAIR *)pParam;
	while(1)	{
		sleep(1);
		if(pServer_qp->pQP_Data)	{
			if( (pServer_qp->pQP_Data[0].queue_pair) || (pServer_qp->pQP_Data[1].queue_pair) )	{
				break;
			}
		}
	}

	sleep(1);
	printf("DBG> Rank = %d. pServer_qp->IdxLastQP = %d\n", mpi_rank, pServer_qp->IdxLastQP);

	for(int i=0; i<1000; i++)	{
		gettimeofday(&tm1, NULL);
		for(int j=0; j<=pServer_qp->IdxLastQP; j++)	{
			if(pServer_qp->p_shm_TimeHeartBeat[j])	{
				printf("Rank %d: heart beat time stamp %ld My time %ld\n", j, pServer_qp->p_shm_TimeHeartBeat[0], tm1.tv_sec);
			}
		}
		pServer_qp->ScanLostQueuePairs();
		sleep(1);
	}

	return NULL;
}

static void* Func_thread_Polling_New_Msg(void *pParam)
{
	SERVER_QUEUEPAIR *pServer_qp;
	
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

	return NULL;
}


static void* Func_thread_qp_server(void *pParam)
{
	SERVER_QUEUEPAIR *pServer_qp;
	
	pServer_qp = (SERVER_QUEUEPAIR *)pParam;
	pServer_qp->Init_Server_IB_Env(DEFAULT_REM_BUFF_SIZE);
	pServer_qp->Init_Server_Socket(2048, ThisNode.port);
	Server_Started = 1;	// active the flag: Server started running!!!
	pServer_qp->Socket_Server_Loop();
	
	return 0;
}

int main(int argc, char **argv)
{
	int i;
	FILE *fOut;
	pthread_t thread_qp_server, thread_print_data, thread_polling_newmsg;
	unsigned char *pNewMsg=NULL;
	struct ibv_mr *mr_local;

    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &nFSServer);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

	Get_Local_Server_Info();
	MPI_Allgather(&ThisNode, sizeof(FS_SEVER_INFO), MPI_CHAR, AllFSNodes, sizeof(FS_SEVER_INFO), MPI_CHAR, MPI_COMM_WORLD);

	if(mpi_rank == 0)	{
		printf("INFO> There are %d servers.\n", nFSServer);
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
		fclose(fOut);
	}

	if(pthread_create(&(thread_qp_server), NULL, Func_thread_qp_server, &Server_qp)) {
		fprintf(stderr, "Error creating thread\n");
		return 1;
	}

//	Setup_Signal_QueuePair();
	while(1)	{
		if(Server_Started)	break;
	}
	printf("Rank = %d. Server is started.\n", mpi_rank);

	Setup_QP_Among_Servers();

	pNewMsg = (unsigned char*)malloc(nFSServer);
	assert(pNewMsg != NULL);
	mr_local = Server_qp.IB_RegisterBuf_RW_Local_Remote(pNewMsg, nFSServer);
	assert(mr_local != NULL);

//	if(mpi_rank == 0)	{
		sleep(3);
		for(i=0; i<nFSServer; i++)	{
			if(mpi_rank == i)	continue;
			pNewMsg[mpi_rank] = 0x80;	// new msg!
			Server_qp.IB_Put(i, &(pNewMsg[mpi_rank]), mr_local->lkey, (void*)(Server_qp.pQP_Data[i].remote_addr_new_msg+sizeof(char)*0), Server_qp.pQP_Data[i].rem_key, 1);
		}
//	}

	if(pthread_create(&(thread_print_data), NULL, Func_thread_Print_Data, &Server_qp)) {
		fprintf(stderr, "Error creating thread\n");
		return 1;
	}

	if(pthread_create(&(thread_polling_newmsg), NULL, Func_thread_Polling_New_Msg, &Server_qp)) {
		fprintf(stderr, "Error creating thread\n");
		return 1;
	}


	if(pthread_join(thread_polling_newmsg, NULL)) {
		fprintf(stderr, "Error joining thread.\n");
		return 2;
	}

	if(pthread_join(thread_print_data, NULL)) {
		fprintf(stderr, "Error joining thread.\n");
		return 2;
	}

	if(pthread_join(thread_qp_server, NULL)) {
		fprintf(stderr, "Error joining thread.\n");
		return 2;
	}

    MPI_Finalize();
	
	return 0;
}
