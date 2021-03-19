#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "qp.h"
#include "dict.h"
#include "io_queue.h"
#include "io_ops.h"
#include "utility.h"
#include "myfs_common.h"
#include "corebinding.h"

extern CORE_BINDING CoreBinding;
extern int nFSServer;
extern SERVER_QUEUEPAIR Server_qp;

long int nOPs_Done[NUM_THREAD_IO_WORKER];
JOBREC ActiveJobList[MAX_NUM_ACTIVE_JOB];

CIO_QUEUE __attribute__((aligned(64))) IO_Queue_List[MAX_NUM_QUEUE];
IO_CMD_MSG __attribute__((aligned(64))) QueueMsgBuff[MAX_NUM_QUEUE*IO_QUEUE_SIZE];

int nActiveJob=0;

CHASHTABLE_INT *pHT_ActiveJobs=NULL;
struct elt_Int *elt_list_ActiveJobs = NULL;
int *ht_table_ActiveJobs=NULL;


void Init_ActiveJobList(void)
{
	int i;

	memset(ActiveJobList, 0, sizeof(JOBREC)*MAX_NUM_ACTIVE_JOB);

	for(i=0; i<MAX_NUM_ACTIVE_JOB; i++)	{
		if(pthread_mutex_init(&(ActiveJobList[i].lock), NULL) != 0) { 
			printf("\n mutex lock init failed in Init_ActiveJobList()\n"); 
			exit(1);
		}
	}
	nActiveJob = 0;

	pHT_ActiveJobs = (CHASHTABLE_INT *)malloc(CHASHTABLE_INT::GetStorageSize(MAX_NUM_ACTIVE_JOB));
	pHT_ActiveJobs->DictCreate(MAX_NUM_ACTIVE_JOB, &elt_list_ActiveJobs, &ht_table_ActiveJobs);	// init hash table
}

void Init_NewActiveJobRecord(int idx_rec, int jobid, int nnode)
{
	ActiveJobList[idx_rec].jobid = jobid;
	ActiveJobList[idx_rec].nnode = nnode;
	ActiveJobList[idx_rec].nQP = 1;	// A new QP was just established. 
	ActiveJobList[idx_rec].nTokenAV = 0;
	ActiveJobList[idx_rec].nTokenReload = 0;
	ActiveJobList[idx_rec].nOps_Done = 0;
	ActiveJobList[idx_rec].nOps_Done_LastCycle = 0;
}

void Init_QueueList(void)
{
	int i;

	memset(nOPs_Done, 0, sizeof(long int)*NUM_THREAD_IO_WORKER);
	
	for(i=0; i<MAX_NUM_QUEUE; i++)	{
		IO_Queue_List[i].IdxWorker = INVALID_WORKERID;
		IO_Queue_List[i].front = 0;
		IO_Queue_List[i].back = -1;
		IO_Queue_List[i].pQueue_Data = (IO_CMD_MSG*)((char*)QueueMsgBuff + i*sizeof(IO_CMD_MSG)*IO_QUEUE_SIZE);
		if(pthread_mutex_init(&(IO_Queue_List[i].lock), NULL) != 0) { 
			printf("\n mutex lock init failed in Init_QueueList()\n"); 
			exit(1);
		}
	}

	//Create the first job queue for all inter-file-server requests about existance of file/directory
	IO_Queue_List[0].IdxWorker = INVALID_WORKERID;
//	IO_Queue_List[0].nQP = nFSServer - 1;
	IO_Queue_List[0].front = 0;
	IO_Queue_List[0].back = -1;

}

void* Func_thread_IO_Worker(void *pParam)	// process all IO wrok
{
	int i, thread_id, idx_op, IdxMin, IdxMax, idx_JobRec, nNumQueuePerWorker;
	IO_CMD_MSG Op_Msg, *pOP_Msg_Retrieve;
	CIO_QUEUE *pIO_Queue=NULL;
	
	thread_id = *((int*)pParam);
	printf("DBG> Func_thread_IO_Worker(): thread_id = %d\n", thread_id);
	CoreBinding.Bind_This_Thread();

	if(thread_id == 0)	{	// the first thread is dedicated for inter-server communication via queue[0]
		IdxMin = 0;
		IdxMax = 0;
	}
	else	{
		if( ( ( MAX_NUM_QUEUE - 1 ) % ( NUM_THREAD_IO_WORKER - 1 ) ) == 0 )	{
			nNumQueuePerWorker = ( MAX_NUM_QUEUE - 1 ) / ( NUM_THREAD_IO_WORKER - 1 ) ;
		}
		else	{
			nNumQueuePerWorker = ( MAX_NUM_QUEUE - 1 ) / ( NUM_THREAD_IO_WORKER - 1 ) + 1;
		}
		IdxMin = 1 + (thread_id-1)*nNumQueuePerWorker;
		IdxMax = min( (IdxMin + nNumQueuePerWorker - 1), (MAX_NUM_QUEUE - 1));
		printf("DBG> worker %d, (%d, %d)\n", thread_id, IdxMin, IdxMax);
	}
	
	while(1)	{	// loop forever
//		for(i=thread_id; i<MAX_NUM_QUEUE; i+=(NUM_THREAD_IO_WORKER-1))	{	// All IO worker handle queues independently now!!! No lock is needed now. 
		for(i=IdxMin; i<=IdxMax; i++)	{	// All IO worker handle queues independently now!!! No lock is needed now. 
			pIO_Queue = &(IO_Queue_List[i]);
			if( (pIO_Queue->back) >= (pIO_Queue->front) )	{	// A queue that is not empty.
				// start to process IO queue
				if( (pIO_Queue->back) >= (pIO_Queue->front) )	{	// Check again to make sure queue is not empty!!!
					idx_op = pIO_Queue->front & IO_QUEUE_SIZE_M1;
					idx_JobRec = pIO_Queue->pQueue_Data[idx_op].idx_JobRec;
					
					if(ActiveJobList[idx_JobRec].nTokenAV >= pIO_Queue->pQueue_Data[idx_op].nTokenNeeded)	{	// check whether the queue has enough token !!!!!!!!!!!!!!!!
						pOP_Msg_Retrieve = pIO_Queue->Dequeue();
						memcpy(&Op_Msg, pOP_Msg_Retrieve, sizeof(IO_CMD_MSG));
						Op_Msg.tid = thread_id;
						
						if (pthread_mutex_lock(&(Server_qp.pQP_Data[Op_Msg.idx_qp].qp_lock)) != 0) {
							perror("pthread_mutex_lock");
							exit(2);
						}
						
						Process_One_IO_OP(&Op_Msg);	// Do the real IO work!
						nOPs_Done[thread_id]++;
						
						if (pthread_mutex_unlock(&(Server_qp.pQP_Data[Op_Msg.idx_qp].qp_lock)) != 0) {
							perror("pthread_mutex_unlock");
							exit(2);
						}
						
					}
				}
			}
		}
	}
}

void Process_One_IO_OP(IO_CMD_MSG *pOP_Msg)
{
	int Op_Tag;

	Op_Tag = pOP_Msg->op & 0xFF;

	switch(Op_Tag)	{
	case RF_RW_OP_OPEN:
		RW_Open(pOP_Msg);
		break;
	case RF_RW_OP_CLOSE:
		RW_Close(pOP_Msg);
		break;
	case RF_RW_OP_OPENDIR:
		RW_Opendir(pOP_Msg);
		break;
	case RF_RW_OP_READ:
		RW_Read(pOP_Msg);
		break;
	case RF_RW_OP_WRITE:
		RW_Write(pOP_Msg);
		break;
	case RF_RW_OP_PREAD:
		RW_PRead(pOP_Msg);
		break;
//	case RF_RW_OP_PWRITE:
//		RW_PWrite(pOP_Msg);
//		break;
	case RF_RW_OP_SEEK:
		RW_Seek(pOP_Msg);
		break;
	case RF_RW_OP_STAT:
		RW_Stat(pOP_Msg);
		break;
	case RF_RW_OP_LSTAT:
		RW_Stat(pOP_Msg);	// link support is NOT implemented yet. 
//		RW_LStat(pOP_Msg);
		break;
	case RF_RW_OP_FSTAT:
		RW_FStat(pOP_Msg);
		break;
	case RF_RW_OP_REMOVE_FILE:
		RW_Unlink(pOP_Msg);
		break;
	case RF_RW_OP_REMOVE_DIR:
		RW_Remove_Dir(pOP_Msg);
		break;
	case RF_RW_OP_MKDIR:
		RW_Mkdir(pOP_Msg);
		break;
//	case RF_RW_OP_FILE_ALLOCATE:
//		RW_Fallocate(pOP_Msg);
//		break;
//	case RF_RW_OP_POSIX_FILE_ALLOCATE:
//		RW_Posix_Fallocate(pOP_Msg);
//		break;
	case RF_RW_OP_TRUNCATE:
		RW_Truncate(pOP_Msg);
		break;
	case RF_RW_OP_FTRUNCATE:
		RW_Ftruncate(pOP_Msg);
		break;
//	case RF_RW_OP_FSYNC:
//		RW_FSync(pOP_Msg);
//		break;
//	case RF_RW_OP_POSIX_FADVISE:
//		RW_Posix_Fadvise(pOP_Msg);
//		break;
//	case RF_RW_OP_FACCESSAT:
//		RW_FAccessat(pOP_Msg);
//		break;
//	case RF_RW_OP_POSIX_FADVISE:
//		RW_Posix_Fadvise(pOP_Msg);
//		break;
	case RF_RW_OP_FUTIMENS:
		RW_Futimens(pOP_Msg);
		break;
	case RF_RW_OP_UTIMES:
		RW_Utimes(pOP_Msg);
		break;
//	case RF_RW_OP_FUTIMENS:
//		RW_Futimens(pOP_Msg);
//		break;
//	case RF_RW_OP_REMOVEENTRY_PARENT_DIR:
//		RW_File_RemoveEntry_ParentDir(pOP_Msg);
//		break;
	case RF_RW_OP_DIR_EXIST:
		RW_Dir_Exist(pOP_Msg);
		break;
	case RF_RW_OP_PRINT_MEM:
		RW_Print_Mem();
		break;
	case RF_RW_OP_DISCONNECT:
		RW_Disconnect_QP(pOP_Msg);
		break;

	default:
		printf("ERROR> Unknown Op_Tag = %d in Process_One_IO_OP().\n", Op_Tag);
		break;
	}

}

void CIO_QUEUE::Enqueue(IO_CMD_MSG *pOp_Msg)
{ 
	IO_CMD_MSG *pMsg;
	
	while( ( back - front ) >= IO_QUEUE_SIZE )	{	// Queue is full. block until queue has enough space
		printf("Queued_Job_List is FULL.\n");
	}
	
	if (pthread_mutex_lock(&lock) != 0) {
		perror("pthread_mutex_lock");
		exit(2);
	}
	pMsg = &(pQueue_Data[ (back+1) & IO_QUEUE_SIZE_M1]);
	memcpy(pMsg, pOp_Msg, sizeof(IO_CMD_MSG));
	back++;
	if (pthread_mutex_unlock(&lock) != 0) {
		perror("pthread_mutex_unlock");
		exit(2);
	}
}

IO_CMD_MSG* CIO_QUEUE::Dequeue(void)
{
	IO_CMD_MSG *pMsg;
		
	if (pthread_mutex_lock(&lock) != 0) {
		perror("pthread_mutex_lock");
		exit(2);
	}
	
	pMsg = &(pQueue_Data[front & IO_QUEUE_SIZE_M1]);
	front++;
//	nTokenReload++;

	if (pthread_mutex_unlock(&lock) != 0) {
		perror("pthread_mutex_unlock");
		exit(2);
	}
	
	return pMsg;
}

