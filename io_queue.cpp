#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "qp.h"
#include "dict.h"
#include "io_queue.h"
#include "io_ops.h"
#include "utility.h"
#include "corebinding.h"

extern CORE_BINDING CoreBinding;
extern int nFSServer, mpi_rank;
extern SERVER_QUEUEPAIR Server_qp;

long int nOPs_Done[NUM_THREAD_IO_WORKER];
JOBREC ActiveJobList[MAX_NUM_ACTIVE_JOB];
LISTJOBREC IdxJobRecList[MAX_NUM_ACTIVE_JOB];
float ActiveJobProbability[MAX_NUM_ACTIVE_JOB];

CIO_QUEUE __attribute__((aligned(64))) IO_Queue_List[MAX_NUM_QUEUE];
__thread uint64_t rseed[2];
__thread int idx_qp_server=0;
IO_CMD_MSG __attribute__((aligned(64))) QueueMsgBuff[MAX_NUM_QUEUE*IO_QUEUE_SIZE];


int nActiveJob=0;

CHASHTABLE_INT *pHT_ActiveJobs=NULL;
struct elt_Int *elt_list_ActiveJobs = NULL;
int *ht_table_ActiveJobs=NULL;

extern pthread_mutex_t lock_Modify_ActiveJob_List;

void Init_ActiveJobList(void)
{
	int i;

	memset(ActiveJobList, 0, sizeof(JOBREC)*MAX_NUM_ACTIVE_JOB);

	if(pthread_mutex_init(&lock_Modify_ActiveJob_List, NULL) != 0) { 
		printf("\n mutex lock init failed for lock_Modify_ActiveJob_List in Init_ActiveJobList()\n"); 
		exit(1);
	}
	for(i=0; i<MAX_NUM_ACTIVE_JOB; i++)	{
		if(pthread_mutex_init(&(ActiveJobList[i].lock), NULL) != 0) { 
			printf("\n mutex lock init failed in Init_ActiveJobList()\n"); 
			exit(1);
		}
	}

	for(i=0; i<MAX_NUM_ACTIVE_JOB; i++)	{
		IdxJobRecList[i].idx_rec_ht = -1;
		IdxJobRecList[i].jobid = -1;
	}

	nActiveJob = 0;

	pHT_ActiveJobs = (CHASHTABLE_INT *)malloc(CHASHTABLE_INT::GetStorageSize(MAX_NUM_ACTIVE_JOB));
	pHT_ActiveJobs->DictCreate(MAX_NUM_ACTIVE_JOB, &elt_list_ActiveJobs, &ht_table_ActiveJobs);	// init hash table
}

//void Update_Active_JobList(void);

void ConstructJobProbabilityList(void)
{
	int i, nnode_sum=0;
	float nnode_sum_inv, prob_Accu=0.0, ratio;

	if(nActiveJob == 0)	return;

	for(i=0; i<nActiveJob; i++)	{
		nnode_sum += (ActiveJobList[IdxJobRecList[i].idx_rec_ht].nnode);
	}

	nnode_sum_inv = 1.0 / nnode_sum;
	prob_Accu = 0.0;
	for(i=0; i<nActiveJob; i++)	{
		ratio = nnode_sum_inv * ActiveJobList[IdxJobRecList[i].idx_rec_ht].nnode;
		prob_Accu += ratio;
		ActiveJobProbability[i] = prob_Accu;
	}

	printf("INFO> ------------- probability of job list.\n");
	for(i=0; i<nActiveJob; i++)	{
		printf("INFO> jobid %d nnode %d %6.3lf\n", ActiveJobList[IdxJobRecList[i].idx_rec_ht].jobid, ActiveJobList[IdxJobRecList[i].idx_rec_ht].nnode, ActiveJobProbability[i]);
	}
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

	IdxJobRecList[nActiveJob].jobid = jobid;
	IdxJobRecList[nActiveJob].idx_rec_ht = idx_rec;
	nActiveJob++;
	ConstructJobProbabilityList();
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

int Random_Pick_a_TargetJob(uint64_t s[2])
{
	int i;
	double r;

//	r = 1.0f*next(s)/RAND_MAX;
	r = (next(s) >> 11) * 0x1.0p-53;

	for(i=0; i<nActiveJob; i++)	{
		if(r < ActiveJobProbability[i])	{
			return IdxJobRecList[i].idx_rec_ht;
		}
	}
	return (-1);
}
/*
void* Func_thread_IO_Worker(void *pParam)	// process all IO wrok
{
//	int i, idxTask, thread_id, idx_op, IdxMin, IdxMax, idx_JobRec, nNumQueuePerWorker, *pNext_IO_OP_Idx_Queue_List=NULL, range, nValid_Next_IO_OP=0;
	int i, idxTask, thread_id, idx_op, IdxMin, IdxMax, idx_JobRec, nNumQueuePerWorker, range, nValid_Next_IO_OP=0;
	IO_CMD_MSG Op_Msg, *pOP_Msg_Retrieve;
	CIO_QUEUE *pIO_Queue=NULL;
//	FIRSTOPLIST *pFirstOPList=NULL;
	FIRSTOPLIST pFirstOPList[400];	// need to make sure it is larger than range!!!!!
	unsigned long int T_queue_Earlyest, T_queue_Earlyest_TargetJob;
	int idx_Earlyest, idx_Earlyest_TargetJob, idx_rec_ht_Picked, nValidOPs, ToProcOP, IdxQueue_PreviousSelected=-1, idx_Cur;
	struct timeval tm1, tm2;	// tm1.tv_sec
	long int t_accum=0;
	long int nOp_Done=0;
	struct timeval tm;

	gettimeofday(&tm, NULL);
	rseed[0] = tm.tv_sec;
	rseed[1] = tm.tv_usec;

	thread_id = *((int*)pParam);
	printf("DBG> Func_thread_IO_Worker(): thread_id = %d\n", thread_id);
	CoreBinding.Bind_This_Thread();

	if(thread_id == 0)	{	// the first thread is dedicated for inter-server communication via queue[0]
		IdxMin = 0;
		IdxMax = 0;
//		sleep(36000);
		pIO_Queue = &(IO_Queue_List[0]);
		while(1)	{
			if( (pIO_Queue->back) >= (pIO_Queue->front) )	{	// A queue that is not empty.
				pOP_Msg_Retrieve = pIO_Queue->Dequeue();
				memcpy(&Op_Msg, pOP_Msg_Retrieve, sizeof(IO_CMD_MSG));
				fetch_and_add((int*)&(ActiveJobList[pOP_Msg_Retrieve->idx_JobRec].nOps_Done), 1);
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
		range = IdxMax-IdxMin+1;

//		pNext_IO_OP_Idx_Queue_List = (int*)malloc(sizeof(int)*range + sizeof(FIRSTOPLIST)*range);	// the list of index to sorted list according to T_Queued
//		assert(pNext_IO_OP_Idx_Queue_List != NULL);
//		pFirstOPList = (FIRSTOPLIST *)((char*)pNext_IO_OP_Idx_Queue_List + sizeof(int)*range);

		for(i=0; i<range; i++)	{
//			pNext_IO_OP_Idx_Queue_List[i] = i;

			pFirstOPList[i].idx_queue = -1;
			pFirstOPList[i].idx_op = -1;
			pFirstOPList[i].T_Queued = LARGE_T_QUEUED;
		}

	}
	
	IdxQueue_PreviousSelected = IdxMin - 1;
	while(1)	{	// loop forever
		if(nActiveJob == 0)	continue;
//		gettimeofday(&tm1, NULL);
		idx_rec_ht_Picked = Random_Pick_a_TargetJob(rseed);
		if(idx_rec_ht_Picked < 0)	continue;

		// loop over all queues this IO worker needs to cover and extract job info for the first OP
		T_queue_Earlyest = T_queue_Earlyest_TargetJob = LARGE_T_QUEUED;
		idx_Earlyest = idx_Earlyest_TargetJob = -1;
		nValidOPs = 0;

		for(i=1; i<=range; i++)	{	// All IO worker handle queues independently now!!! No lock is needed now.
			idx_Cur = IdxQueue_PreviousSelected + i;
			if(idx_Cur > IdxMax)	idx_Cur -= range;
			idxTask = idx_Cur -IdxMin;

			if(pFirstOPList[idxTask].idx_queue < 0)	{	// An invalid record. Need to grab the first OP info. 
				pIO_Queue = &(IO_Queue_List[idx_Cur]);
				if( (pIO_Queue->back) >= (pIO_Queue->front) )	{	// A queue that is not empty.
					pFirstOPList[idxTask].idx_queue = idx_Cur;
					idx_op = pIO_Queue->front & IO_QUEUE_SIZE_M1;
					pFirstOPList[idxTask].idx_op = idx_op;
					pFirstOPList[idxTask].idx_rec_ht = pIO_Queue->pQueue_Data[idx_op].idx_JobRec;
					pFirstOPList[idxTask].T_Queued = pIO_Queue->pQueue_Data[idx_op].T_Queued;

					nValidOPs++;
					// Need to insert this new record into the sorted list!!!!!!!!!!!!!!!!!!!!!!!
				}
				else	{
					pFirstOPList[idxTask].T_Queued = LARGE_T_QUEUED;
				}
			}

			if(pFirstOPList[idxTask].T_Queued < LARGE_T_QUEUED)	{	// a valid record!
				if(pFirstOPList[idxTask].idx_rec_ht == idx_rec_ht_Picked)	{
					if(pFirstOPList[idxTask].T_Queued < T_queue_Earlyest_TargetJob)	{
						T_queue_Earlyest_TargetJob = pFirstOPList[idxTask].T_Queued;
						idx_Earlyest_TargetJob = idxTask;
					}
				}
				if(pFirstOPList[idxTask].T_Queued < T_queue_Earlyest)	{	// find the earliest OP
					T_queue_Earlyest = pFirstOPList[idxTask].T_Queued;
					idx_Earlyest = idxTask;
				}
			}
		}

//		printf("DBG> Rank = %d idx_rec_ht_Picked = %d\n", mpi_rank, idx_rec_ht_Picked);
		ToProcOP = 0;
		if( idx_Earlyest_TargetJob >=0 )	{	// process the earliest target job request
			IdxQueue_PreviousSelected = pFirstOPList[idx_Earlyest_TargetJob].idx_queue;
			pIO_Queue = &(IO_Queue_List[IdxQueue_PreviousSelected]);
			idx_op = pIO_Queue->front & IO_QUEUE_SIZE_M1;
			ToProcOP = 1;
//			printf("INFO> Proc target   OP %3d queue %4d Time %ld\n", pFirstOPList[idx_Earlyest_TargetJob].idx_queue, pIO_Queue->front, pFirstOPList[idx_Earlyest_TargetJob].T_Queued);
			pFirstOPList[idx_Earlyest_TargetJob].idx_queue = -1;
		}

		else if( idx_Earlyest >=0 )	{	// process the earliest request then
			IdxQueue_PreviousSelected = pFirstOPList[idx_Earlyest].idx_queue;
			pIO_Queue = &(IO_Queue_List[IdxQueue_PreviousSelected]);
			idx_op = pIO_Queue->front & IO_QUEUE_SIZE_M1;
			ToProcOP = 1;
//			printf("INFO> Proc earliest OP %3d queue %4d Time %ld\n", pFirstOPList[idx_Earlyest].idx_queue, pIO_Queue->front, pFirstOPList[idx_Earlyest].T_Queued);
			pFirstOPList[idx_Earlyest].idx_queue = -1;
		}


		if(ToProcOP)	{
//			gettimeofday(&tm2, NULL);
//			t_accum += ( (tm2.tv_sec - tm1.tv_sec) * 1000000 + (tm2.tv_usec - tm1.tv_usec) );
			nOp_Done++;
//			if(nOp_Done % 100000 == 0)	{
//				printf("INFO> thread_id = %d Overhead %5.3lf\n", thread_id, 1.0 * t_accum / nOp_Done);
//			}

			pOP_Msg_Retrieve = pIO_Queue->Dequeue();
			memcpy(&Op_Msg, pOP_Msg_Retrieve, sizeof(IO_CMD_MSG));
			fetch_and_add((int*)&(ActiveJobList[pOP_Msg_Retrieve->idx_JobRec].nOps_Done), 1);
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
*/

void* Func_thread_IO_Worker(void *pParam)	// process all IO wrok
{
	int i, thread_id, idx_op, IdxMin, IdxMax, idx_JobRec, nNumQueuePerWorker;
	IO_CMD_MSG Op_Msg, *pOP_Msg_Retrieve;
	CIO_QUEUE *pIO_Queue=NULL;
	struct timeval tm;
	
	thread_id = *((int*)pParam);
	printf("DBG> Func_thread_IO_Worker(): thread_id = %d\n", thread_id);
	CoreBinding.Bind_This_Thread();
	idx_qp_server = thread_id % NUM_THREAD_IO_WORKER_INTER_SERVER;

	gettimeofday(&tm, NULL);
	rseed[0] = tm.tv_sec;
	rseed[1] = tm.tv_usec;

	if(thread_id < NUM_THREAD_IO_WORKER_INTER_SERVER)	{	// the first thread is dedicated for inter-server communication via queue[0]
		if(nFSServer == 1)	sleep(36000);
		IdxMin = thread_id;
		IdxMax = thread_id;
		pIO_Queue = &(IO_Queue_List[thread_id]);
		while(1)	{
			if( (pIO_Queue->back) >= (pIO_Queue->front) )	{	// A queue that is not empty.
				pOP_Msg_Retrieve = pIO_Queue->Dequeue();
				memcpy(&Op_Msg, pOP_Msg_Retrieve, sizeof(IO_CMD_MSG));
				fetch_and_add((int*)&(ActiveJobList[pOP_Msg_Retrieve->idx_JobRec].nOps_Done), 1);
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
	else	{
		if( ( ( MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER ) % ( NUM_THREAD_IO_WORKER - NUM_THREAD_IO_WORKER_INTER_SERVER ) ) == 0 )	{
			nNumQueuePerWorker = ( MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER ) / ( NUM_THREAD_IO_WORKER - NUM_THREAD_IO_WORKER_INTER_SERVER ) ;
		}
		else	{
			nNumQueuePerWorker = ( MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER ) / ( NUM_THREAD_IO_WORKER - NUM_THREAD_IO_WORKER_INTER_SERVER ) + 1;
		}
		IdxMin = NUM_THREAD_IO_WORKER_INTER_SERVER + (thread_id-NUM_THREAD_IO_WORKER_INTER_SERVER)*nNumQueuePerWorker;
		IdxMax = min( (IdxMin + nNumQueuePerWorker - 1), (MAX_NUM_QUEUE - 1));
		printf("DBG> worker %d, (%d, %d)\n", thread_id, IdxMin, IdxMax);
	}
	
	while(1)	{	// loop forever
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
	case RF_RW_OP_ADDENTRY_PARENT_DIR:
		RW_File_AddEntry_ParentDir(pOP_Msg);
		break;
	case RF_RW_OP_REMOVEENTRY_PARENT_DIR:
		RW_File_RemoveEntry_ParentDir(pOP_Msg);
		break;
//	case RF_RW_OP_UPDATE_IDX_PARENT_DIR_ENTRY_LIST:
//		RW_File_UpdateEntry_ParentDir_EntryIdx(pOP_Msg);
//		break;
	case RF_RW_OP_DIR_EXIST:
		RW_Dir_Exist(pOP_Msg);
		break;
	case RF_RW_OP_PRINT_MEM:
		RW_Print_Mem();
		break;
	case RF_RW_OP_HELLO:
		RW_Hello(pOP_Msg);
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

