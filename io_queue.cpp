#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "io_queue.h"
#include "io_ops.h"
#include "utility.h"
#include "myfs_common.h"

extern int nFSServer;

int nJobQueue, FirstAV_Queue, IdxLastQueue;	// number of queues for job processing. First avaialble cell to a new queue creation. The index of last queue. Used for looping. 
int __attribute__((aligned(16))) ListJobID[MAX_NUM_QUEUE];
CIO_QUEUE __attribute__((aligned(64))) IO_Queue_List[MAX_NUM_QUEUE];
IO_CMD_MSG __attribute__((aligned(64))) QueueMsgBuff[MAX_NUM_QUEUE*IO_QUEUE_SIZE];

void Init_QueueList(void)
{
	int i;

	for(i=0; i<MAX_NUM_QUEUE; i++)	{
		ListJobID[i] = INVALID_JOBID;
		IO_Queue_List[i].pQueue_Data = (IO_CMD_MSG*)((char*)QueueMsgBuff + sizeof(IO_CMD_MSG)*IO_QUEUE_SIZE);
	}

	//Create the first job queue for all inter-file-server requests about existance of file/directory
	nJobQueue = 1;

	ListJobID[0] = SERVER_JOBID;	// the first queue is special. It is dedicated for inter-server communication.
	IO_Queue_List[0].IdxWorker = INVALID_WORKERID;
	IO_Queue_List[0].nQP = nFSServer - 1;
	IO_Queue_List[0].front = 0;
	IO_Queue_List[0].back = -1;

	FirstAV_Queue = 1;
	IdxLastQueue = 0;
}

int Create_A_Queue(int jobid)
{
	int idx;

	idx = FindFirstAvailableSpaceForQueue();
	if(idx > 0)	{	// Success in find a available queue for the new jobid
		ListJobID[idx] = jobid;
//		IO_Queue_List[idx].IdxWorker = INVALID_WORKERID;
//		IO_Queue_List[idx].nQP = 1;	// create the queue on first QP
		IO_Queue_List[idx].jobid = jobid;
		IO_Queue_List[idx].Init_Queue();

		return idx;
	}
	else	{
		printf("ERROR> Fail to allocate an IO queue for job %d\nQuit\n", jobid);
		return -1;
	}
}

int Query_Jobid_In_Queue(int jobid)
{
	int i;

	for(i=0; i<=IdxLastQueue; i++)	{
		if(jobid == ListJobID[i])	{
			return i;
		}
	}
	return (-1);	// Does not exist!!!
}

int FindFirstAvailableSpaceForQueue(void)	// Search from FirstAV_Queue 
{
	int i, idx = -1, Done=0;

	if(FirstAV_Queue < 0)	{
		return FirstAV_Queue;
	}
	idx = FirstAV_Queue;
	if(FirstAV_Queue > IdxLastQueue)	{
		IdxLastQueue = FirstAV_Queue;
	}
	FirstAV_Queue = -1;

	for(i=idx+1; i<MAX_NUM_QUEUE; i++)	{
		if(ListJobID[i] == INVALID_JOBID)	{
			FirstAV_Queue = i;	// update FirstAV_Queue
			Done = 1;
			break;
		}
	}
	if(FirstAV_Queue < 0)	{
		printf("WARNING> All space for queues are used.\n");
	}

	return idx;
}

void Free_A_Queue(int idx)
{
	int i;

	printf("DBG> Free %d queue.\n", idx);
	ListJobID[idx] = INVALID_JOBID;
	if(idx < FirstAV_Queue)	{
		FirstAV_Queue = idx;
	}
	if(idx == IdxLastQueue)	{	// Need to update IdxLastQueue
		for(i=idx-1; i>=0; i--)	{
			if(ListJobID[i] > 0)	{
				IdxLastQueue = i;
				break;
			}
		}
	}

	nJobQueue--;
}

void* Func_thread_IO_Worker(void *pParam)	// process all IO wrok
{
	int i, thread_id, OrgIdxWorker, idx_op;
	IO_CMD_MSG Op_Msg, *pOP_Msg_Retrieve;
	CIO_QUEUE *pIO_Queue=NULL;

	thread_id = *((int*)pParam);
	while(1)	{	// loop forever
//		for(i=0; i<IdxLastQueue; i++)	{
		for(i=0; i<=IdxLastQueue; i++)	{
			if(ListJobID[i] >= 0)	{	// A valid queue
				pIO_Queue = &(IO_Queue_List[i]);
				OrgIdxWorker = __cmpxchg((unsigned long *)(&(pIO_Queue->IdxWorker)), INVALID_WORKERID, thread_id, 4);
				if(OrgIdxWorker == INVALID_WORKERID)	{	// This queue was not being processed by other worker. Current worker is going to work on it. 
					// start to process IO queue
					if( (pIO_Queue->back) >= (pIO_Queue->front) )	{	// Queue is not empty!!!
						idx_op = pIO_Queue->front & IO_QUEUE_SIZE_M1;
						if(pIO_Queue->nTokenAV >= pIO_Queue->pQueue_Data[idx_op].nTokenNeeded)	{	// check whether the queue has enough token !!!!!!!!!!!!!!!!
							pOP_Msg_Retrieve = pIO_Queue->Dequeue();
							memcpy(&Op_Msg, pOP_Msg_Retrieve, sizeof(IO_CMD_MSG));
							Op_Msg.tid = thread_id;
							Process_One_IO_OP(&Op_Msg);	// Do the real IO work!
						}
					}
					pIO_Queue->IdxWorker = INVALID_WORKERID;	// set the status that no worker is processing this queue.
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
//	case RF_RW_OP_WRITE:
//		RW_Write(pOP_Msg);
//		break;
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
		RW_LStat(pOP_Msg);
		break;
	case RF_RW_OP_FSTAT:
		RW_FStat(pOP_Msg);
		break;
//	case RF_RW_OP_REMOVE_FILE:
//		RW_Remove_File(pOP_Msg);
//		break;
//	case RF_RW_OP_REMOVE_DIR:
//		RW_Remove_Dir(pOP_Msg);
//		break;
//	case RF_RW_OP_MKDIR:
//		RW_Mkdir(pOP_Msg);
//		break;
//	case RF_RW_OP_FILE_ALLOCATE:
//		RW_Fallocate(pOP_Msg);
//		break;
//	case RF_RW_OP_POSIX_FILE_ALLOCATE:
//		RW_Posix_Fallocate(pOP_Msg);
//		break;
//	case RF_RW_OP_TRUNCATE:
//		RW_Truncate(pOP_Msg);
//		break;
//	case RF_RW_OP_FTRUNCATE:
//		RW_Ftruncate(pOP_Msg);
//		break;
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
//	case RF_RW_OP_FUTIMENS:
//		RW_Futimens(pOP_Msg);
//		break;
//	case RF_RW_OP_UTIMES:
//		RW_Utimes(pOP_Msg);
//		break;
//	case RF_RW_OP_FUTIMENS:
//		RW_Futimens(pOP_Msg);
//		break;
//	case RF_RW_OP_REMOVEENTRY_PARENT_DIR:
//		RW_File_RemoveEntry_ParentDir(pOP_Msg);
//		break;
	case RF_RW_OP_DIR_EXIST:
		RW_Dir_Exist(pOP_Msg);
		break;

	default:
		printf("ERROR> Unknown Op_Tag = %d in Process_One_IO_OP().\n", Op_Tag);
		break;
	}

}

void CIO_QUEUE::Init_Queue(void)
{
	IdxWorker = INVALID_WORKERID;
	nQP = 1;

	front=0;
	back=-1;

	nOps_Done = nOps_Done_LastCycle = 0;

    if(pthread_mutex_init(&lock, NULL) != 0) { 
        printf("\n mutex lock init failed in CIO_QUEUE::Init()\n"); 
        exit(1);
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
	back++;
	pMsg = &(pQueue_Data[ back & IO_QUEUE_SIZE_M1]);
	memcpy(pMsg, pOp_Msg, sizeof(IO_CMD_MSG));
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
	nTokenReload++;

	if (pthread_mutex_unlock(&lock) != 0) {
		perror("pthread_mutex_unlock");
		exit(2);
	}
	
	return pMsg;
}

