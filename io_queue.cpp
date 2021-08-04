#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>

#include "qp.h"
#include "dict.h"
#include "io_queue.h"
#include "io_ops.h"
#include "utility.h"
#include "corebinding.h"
#include "fair_queue.h"

extern CORE_BINDING CoreBinding;
extern int nFSServer, mpi_rank;
extern SERVER_QUEUEPAIR Server_qp;


int active_prob=0;	// the index of active probability set. Only can be 0 or 1. ^ 1 to flip it. 

// array of possible operations to execute in Func_thread_IO_Worker_FairQueue
// must be larger than 'range'  (range = IdxMax - IdxMin + 1)
#define FIRSTOPLIST_SIZE 640

// XXX each counter in this array is updated by a different thread (via nOPs_Done[thread_id]++)
// so the performance of those updates will suffer from false sharing
long int nOPs_Done[NUM_THREAD_IO_WORKER];

// writes are synchronized with lock_Modify_ActiveJob_List in qp.cpp, reads are not synchronized
JOBREC ActiveJobList[MAX_NUM_ACTIVE_JOB];

// size=nActiveJob
// IdxJobRecList[i].idx_rec_ht is an index into ActiveJobList
// new jobs are appended to this in Init_NewActiveJobRecord()
// XXX there needs to be a way to remove inactive jobs, and this should be made thread-safe
LISTJOBREC IdxJobRecList[MAX_NUM_ACTIVE_JOB];

// Set in ConstructJobProbabilityList each time Init_NewActiveJobRecord is called,
// this is used by Random_Pick_a_TargetJob to choose jobs.
float ActiveJobProbability[2][MAX_NUM_ACTIVE_JOB];	// two set of list of probablities since we keep updating them. 

CIO_QUEUE __attribute__((aligned(64))) IO_Queue_List[MAX_NUM_QUEUE];
__thread uint64_t rseed[2];
__thread int idx_qp_server=0;
IO_CMD_MSG __attribute__((aligned(64))) QueueMsgBuff[MAX_NUM_QUEUE*IO_QUEUE_SIZE];


int nActiveJob=0;

CHASHTABLE_INT *pHT_ActiveJobs=NULL;
struct elt_Int *elt_list_ActiveJobs = NULL;
int *ht_table_ActiveJobs=NULL;

// defined in qp.cpp, this protects ActiveJobList
extern pthread_mutex_t lock_Modify_ActiveJob_List;
extern pthread_mutex_t *pAccess_qp0_lock;

// Send my record of job OP to rank 0
void Upload_Job_OP_List(long int T_Upload)
{
	int idx_qp;

	if(mpi_rank == 0)	return;
	if(nActiveJob == 0)	return;

	idx_qp = 0;	// Using the first QP
	Server_qp.pJob_OP_Send->T_op_us = T_Upload;

	if(Server_qp.pQP_Data[idx_qp].queue_pair == NULL)	return;

	if (pthread_mutex_lock(&(pAccess_qp0_lock[idx_qp])) != 0) {
		perror("pthread_mutex_lock");
		exit(2);
	}

	Server_qp.IB_Put(idx_qp, (void*)Server_qp.pJob_OP_Send, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pJob_OP_Recv), Server_qp.pQP_Data[idx_qp].rem_key, sizeof(JOB_OP_REC)*(nActiveJob+1));

	if (pthread_mutex_unlock(&(pAccess_qp0_lock[idx_qp])) != 0) {
		perror("pthread_mutex_unlock");
		exit(2);
	}
}

int Query_JobID(int job_id, JOB_OP_SEND *pJobList)
{
	int i, nJob;

	nJob = pJobList->nActiveJob;

	for(i=0; i<nJob; i++)	{	// Will use hash table for better efficiency later! 
		if( job_id == pJobList->Job_Op[i].jobid )	{
			return i;
		}
	}
	return (-1);
}

void Download_Scaling_Factors(long int T_Download)
{
	int idx_qp, nBytesExpected;

	if(mpi_rank == 0)	return;

	idx_qp = 0;	// Using the first QP

	if(Server_qp.pQP_Data[idx_qp].queue_pair == NULL)	return;

	if (pthread_mutex_lock(&(pAccess_qp0_lock[idx_qp])) != 0) {
		perror("pthread_mutex_lock");
		exit(2);
	}

	Server_qp.IB_Get(idx_qp, (void*)Server_qp.pJobScale_Local, Server_qp.mr_shm_global->lkey, (void*)(Server_qp.pJobScale_Remote), Server_qp.pQP_Data[idx_qp].rem_key, N_BYTE_READ_SCALING);
	nBytesExpected = sizeof(int)*4 + Server_qp.pJobScale_Local->nActiveJob*sizeof(int)*2;
	if(nBytesExpected > N_BYTE_READ_SCALING)	{
		Server_qp.IB_Get(idx_qp, (void*)((char*)(Server_qp.pJobScale_Local) + N_BYTE_READ_SCALING), Server_qp.mr_shm_global->lkey, 
			(void*)((char*)(Server_qp.pJobScale_Remote) + N_BYTE_READ_SCALING), Server_qp.pQP_Data[idx_qp].rem_key, nBytesExpected-N_BYTE_READ_SCALING);
	}

	if (pthread_mutex_unlock(&(pAccess_qp0_lock[idx_qp])) != 0) {
		perror("pthread_mutex_unlock");
		exit(2);
	}
}


void Sum_OP_Done_All_Servers(long int T_Download)
{
	int idx_server, idx_job, nJob_Local, idx;
	int nNode_Total=0;
	float scale;
	long int nOP_Done_Total=0;
	JOB_OP_SEND JobList;

	JobList.nActiveJob = 0;

	for(idx_server=0; idx_server<nFSServer; idx_server++)	{
		nJob_Local = Server_qp.pJob_OP_Recv[idx_server].nActiveJob;
		for(idx_job=0; idx_job<nJob_Local; idx_job++)	{
			idx = Query_JobID(Server_qp.pJob_OP_Recv[idx_server].Job_Op[idx_job].jobid, &JobList);
			printf("DBG> %d nOPs_Done = %ld\n", idx_server, Server_qp.pJob_OP_Recv[idx_server].Job_Op[idx_job].nOps_Done);
			if(idx >=0)	{	// found 
				JobList.Job_Op[idx].nOps_Done += ( Server_qp.pJob_OP_Recv[idx_server].Job_Op[idx_job].nOps_Done );
			}
			else	{	// Not found. Append this record. 
				JobList.Job_Op[JobList.nActiveJob].jobid = Server_qp.pJob_OP_Recv[idx_server].Job_Op[idx_job].jobid;
				JobList.Job_Op[JobList.nActiveJob].nnode = Server_qp.pJob_OP_Recv[idx_server].Job_Op[idx_job].nnode;
				JobList.Job_Op[JobList.nActiveJob].nOps_Done = Server_qp.pJob_OP_Recv[idx_server].Job_Op[idx_job].nOps_Done;
				JobList.nActiveJob++;
			}
		}
	}

	for(idx_job=0; idx_job<JobList.nActiveJob; idx_job++)	{
		nNode_Total += JobList.Job_Op[idx_job].nnode;
		nOP_Done_Total += JobList.Job_Op[idx_job].nOps_Done;
	}
	printf("DBG> nOP_Done_Total = %ld\n", nOP_Done_Total);


	if(nOP_Done_Total < N_OP_DONE_THRESHOLD)	Server_qp.pJobScale_Remote->nActiveJob = 0;
	Server_qp.pJobScale_Remote->nActiveJob = JobList.nActiveJob;

	for(idx_job=0; idx_job<JobList.nActiveJob; idx_job++)	{
//		if(JobList.Job_Op[idx_job].nOps_Done == 0)	{
//			JobList.Job_Op[idx_job].nOps_Done = 1;
//		}
		JobList.Job_Op[idx_job].nOps_Done++;	// Make sure it is larger than zero

		Server_qp.pJobScale_Remote->Job_Scale[idx_job].jobid = JobList.Job_Op[idx_job].jobid;
		scale = (float)( 1.0 * JobList.Job_Op[idx_job].nnode * nOP_Done_Total/(JobList.Job_Op[idx_job].nOps_Done*nNode_Total) );
		scale = MIN(scale, nFSServer*1.0);
		if(JobList.Job_Op[idx_job].nOps_Done < N_OP_DONE_THRESHOLD)	{	// Not an active job on this node
			scale = PROB_LOWWER_BOUND;
		}
		Server_qp.pJobScale_Remote->Job_Scale[idx_job].scale = scale;
		printf("DBG> s[%d] = %lf %d %d %ld %ld\n", idx_job, Server_qp.pJobScale_Remote->Job_Scale[idx_job].scale, JobList.Job_Op[idx_job].nnode, nNode_Total, 
			nOP_Done_Total, JobList.Job_Op[idx_job].nOps_Done);
	}

}

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

/* Fills ActiveJobProbability[] with a cumulative probability table,
   one entry per active job, where the magnitude of each entry is
   proportional to the number of nodes in the job, and the total is 1.
   This is used by Random_Pick_a_TargetJob. */
void ConstructJobProbabilityList(void)
{
	int i;
	float nnode_sum=0.0, nnode_sum_inv, prob_Accu=0.0, ratio, *pJobProb=NULL;

	if(nActiveJob == 0)	return;

	pJobProb = ActiveJobProbability[active_prob];

	for(i=0; i<nActiveJob; i++)	{
		pJobProb[i] = ActiveJobList[IdxJobRecList[i].idx_rec_ht].nnode;
		nnode_sum += pJobProb[i];
	}

	nnode_sum_inv = 1.0 / nnode_sum;
	prob_Accu = 0.0;
	for(i=0; i<nActiveJob; i++)	{
		ratio = nnode_sum_inv * pJobProb[i];
		prob_Accu += ratio;
		pJobProb[i] = prob_Accu;
	}

	printf("INFO> ------------- probability of job list.\n");
	for(i=0; i<nActiveJob; i++)	{
		printf("INFO> jobid %d nnode %d %6.3lf\n", ActiveJobList[IdxJobRecList[i].idx_rec_ht].jobid, ActiveJobList[IdxJobRecList[i].idx_rec_ht].nnode, pJobProb[i]);
	}
}

float Query_Scaling_Factor(int jobid)
{
	int i;

	for(i=0; i<Server_qp.pJobScale_Local->nActiveJob; i++)	{
		if(Server_qp.pJobScale_Local->Job_Scale[i].jobid == jobid)	{
			return Server_qp.pJobScale_Local->Job_Scale[i].scale;
		}
	}
	return 1.0f;
}

void Scale_Probability_List(void)
{
	int i, new_active_prob;
	float nnode_sum=0.0, nnode_sum_inv, prob_Accu=0.0, ratio, *pJobProb=NULL;
	long int nOP_Done_Sum=0;

	if(nActiveJob == 0)	return;
	if(Server_qp.pJobScale_Remote == NULL)	return;

	new_active_prob = active_prob ^ 1;	// flip
	pJobProb = ActiveJobProbability[new_active_prob];

	for(i=0; i<nActiveJob; i++)	{
		Server_qp.pJobScale_Local->Job_Scale[i].scale;
//		pJobProb[i] = ActiveJobList[IdxJobRecList[i].idx_rec_ht].nnode;
		printf("rank = %d s[%d] = %lf\n", mpi_rank, i, Query_Scaling_Factor(IdxJobRecList[i].jobid));
		pJobProb[i] = (ActiveJobList[IdxJobRecList[i].idx_rec_ht].nnode * Query_Scaling_Factor(IdxJobRecList[i].jobid));
		nnode_sum += pJobProb[i];
	}

	nnode_sum_inv = 1.0 / nnode_sum;
	prob_Accu = 0.0;
	for(i=0; i<nActiveJob; i++)	{
		ratio = nnode_sum_inv * pJobProb[i];
		prob_Accu += ratio;
		pJobProb[i] = prob_Accu;
	}

	printf("INFO> ------------- probability of job list.\n");
	for(i=0; i<nActiveJob; i++)	{
		printf("INFO> jobid %d nnode %d %6.3lf\n", ActiveJobList[IdxJobRecList[i].idx_rec_ht].jobid, ActiveJobList[IdxJobRecList[i].idx_rec_ht].nnode, pJobProb[i]);
	}

	Server_qp.pJob_OP_Send->nActiveJob = nActiveJob;
	for(i=0; i<nActiveJob; i++)	{
		Server_qp.pJob_OP_Send->Job_Op[i].jobid = ActiveJobList[i].jobid;
		Server_qp.pJob_OP_Send->Job_Op[i].nnode = ActiveJobList[i].nnode;
		Server_qp.pJob_OP_Send->Job_Op[i].nOps_Done = 0;	// recount from 0
	}

	active_prob ^= 1;
}

void Init_NewActiveJobRecord(int idx_rec, int jobid, int nnode, int user_id)
{
	int i;
	JOB_OP_SEND *pJob_OP = Server_qp.pJob_OP_Send;
	JOB_OP_REC *pJob_Done = pJob_OP->Job_Op;

	ActiveJobList[idx_rec].jobid = jobid;
	ActiveJobList[idx_rec].nnode = nnode;
	ActiveJobList[idx_rec].nQP = 1;	// A new QP was just established. 
	ActiveJobList[idx_rec].uid = user_id;
	ActiveJobList[idx_rec].nTokenAV = 0;
	ActiveJobList[idx_rec].nTokenReload = 0;
	ActiveJobList[idx_rec].nOps_Done = 0;
	ActiveJobList[idx_rec].nOps_Done_LastCycle = 0;

	IdxJobRecList[nActiveJob].jobid = jobid;
	IdxJobRecList[nActiveJob].idx_rec_ht = idx_rec;
	nActiveJob++;

	ConstructJobProbabilityList();

	pJob_OP->nActiveJob = nActiveJob;
	for(i=0; i<nActiveJob; i++)	{
		pJob_Done[i].jobid = ActiveJobList[i].jobid;
		pJob_Done[i].nnode = ActiveJobList[i].nnode;
		pJob_Done[i].nOps_Done = 0;	// recount from 0
	}
	if(mpi_rank == 0)	{
		Server_qp.pJobScale_Remote->nActiveJob = 0;
	}
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

/* Returns the an index into ActiveJobList[] of a randomly selected
	 job, weighted by number of nodes in the job. */
int Random_Pick_a_TargetJob(uint64_t s[2], int *p_Idx_Job)
{
	int i;
	double r;
	float *pJobProb;

	pJobProb = ActiveJobProbability[active_prob];

	// choose a random float in the range [0,1)
	//	r = 1.0f*next(s)/RAND_MAX;
	r = (next(s) >> 11) * 0x1.0p-53;

	for(i=0; i<nActiveJob; i++)	{
		if(r < pJobProb[i])	{
			*p_Idx_Job = i;
			return IdxJobRecList[i].idx_rec_ht;
		}
	}
	*p_Idx_Job = (-1);
	return (-1);
}


void* Func_thread_IO_Worker_LeiSizeFair(void *pParam)	// process all IO wrok
{
//	int i, idxTask, thread_id, idx_op, IdxMin, IdxMax, idx_JobRec, nNumQueuePerWorker, *pNext_IO_OP_Idx_Queue_List=NULL, range, nValid_Next_IO_OP=0;
	int i, idxTask, thread_id, idx_op, IdxMin, IdxMax, idx_JobRec, nNumQueuePerWorker, range, nValid_Next_IO_OP=0;
	int idx_job;
	IO_CMD_MSG Op_Msg;
	CIO_QUEUE *pIO_Queue=NULL;

	// array of possible operations to execute
	// must be larger than 'range'  (range = IdxMax - IdxMin + 1)
	FIRSTOPLIST pFirstOPList[FIRSTOPLIST_SIZE];

	unsigned long int T_queue_Earlyest, T_queue_Earlyest_TargetJob;
	int idx_Earlyest, idx_Earlyest_TargetJob, idx_rec_ht_Picked, nValidOPs, ToProcOP, IdxQueue_PreviousSelected=-1, idx_Cur;
	struct timeval tm1, tm2;	// tm1.tv_sec
	long int t_accum=0;
	long int nOp_Done=0;
	long int T_Start_us, T_Now, T_Upload, T_Download, T_Sum;
	struct timeval tm;

	while(! Server_qp.pJobScale_Remote)	{
	}
	sleep(1);

	T_Start_us = Server_qp.T_Start_us;
	T_Download = T_Start_us;
	T_Upload = T_Download + T_FOR_UPLOAD;
	T_Sum = T_Download + T_FOR_SUM;

	gettimeofday(&tm, NULL);
	rseed[0] = tm.tv_sec;
	rseed[1] = tm.tv_usec;

	thread_id = *((int*)pParam);
	printf("DBG> Func_thread_IO_Worker(): thread_id = %d\n", thread_id);
	CoreBinding.Bind_This_Thread();

//	if(thread_id == 0)	{	// the first thread is dedicated for inter-server communication via queue[0]
	// the first NUM_THREAD_IO_WORKER_INTER_SERVER thread is dedicated for inter-server communication via queue[0]
	if(thread_id < NUM_THREAD_IO_WORKER_INTER_SERVER)	{
		if(nFSServer == 1)	sleep(3600000);	// no work to do if there is only one server
//		IdxMin = 0;
//		IdxMax = 0;
//		pIO_Queue = &(IO_Queue_List[0]);
		IdxMin = thread_id;
		IdxMax = thread_id;
		pIO_Queue = &(IO_Queue_List[thread_id]);
		while(1)	{

			if(thread_id == 0)	{	// upload load info and download scaling factors
				gettimeofday(&tm, NULL);
				T_Now = tm.tv_sec*1000000 + tm.tv_usec;
				if( T_Now >= T_Download )	{
					// Download data from server 0;
					Download_Scaling_Factors(T_Now);
					printf("Time %ld %ld\n", T_Download, T_Now);
					T_Download += T_WINDOW;	// update to next time to download
					Scale_Probability_List();
				}
				if( T_Now >= T_Upload )	{
					Upload_Job_OP_List(T_Now);
					T_Upload += T_WINDOW;
				}
				if(mpi_rank == 0)	{
					if( T_Now >= T_Sum )	{
						Sum_OP_Done_All_Servers(T_Now);
						T_Sum += T_WINDOW;
					}
				}
			}

			if( (pIO_Queue->back) >= (pIO_Queue->front) )	{	// A queue that is not empty.
//				pOP_Msg_Retrieve = pIO_Queue->Dequeue();
				// 0 - success (not empty)
				if( pIO_Queue->Dequeue(&Op_Msg) )	{	// failed
					continue;
				}
				// XXX this performs a 32-bit update of a 64-bit counter, so at 2**32-1, it will overflow back down to 0
				fetch_and_add((int*)&(ActiveJobList[Op_Msg.idx_JobRec].nOps_Done), 1);	// ??????????????????????????????????????????????????????
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
		IdxMax = MIN( (IdxMin + nNumQueuePerWorker - 1), (MAX_NUM_QUEUE - 1));
		printf("DBG> worker %d, (%d, %d)\n", thread_id, IdxMin, IdxMax);

		range = IdxMax-IdxMin+1;
		assert(FIRSTOPLIST_SIZE >= range);

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
		// XXX without thread synchronization, this variable may be cached
		// in a register and never re-read, creating an infinite loop
		if(nActiveJob == 0)	continue;
		idx_rec_ht_Picked = Random_Pick_a_TargetJob(rseed, &idx_job);
		if(idx_rec_ht_Picked < 0)	continue;

		// loop over all queues this IO worker needs to cover and extract job info for the first OP
		T_queue_Earlyest = T_queue_Earlyest_TargetJob = LARGE_T_QUEUED;
		idx_Earlyest = idx_Earlyest_TargetJob = -1;
		nValidOPs = 0;

    // Scan all queues in the inclusive range [IdxMin:IdxMax], checking
    // just the first entry in each (not a full scan of all queued events).
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

      // idx_Earlyest_TargetJob: oldest entry from the job selected by Random_Pick_a_TargetJob
      // idx_Earlyest: oldest entry from any job
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
      // % is a slow operation. (nOp_Done & ((1<<17)-1))==0 would be better
//			if(nOp_Done % 100000 == 0)	{
//				printf("INFO> thread_id = %d Overhead %5.3lf\n", thread_id, 1.0 * t_accum / nOp_Done);
//			}

//			pOP_Msg_Retrieve = pIO_Queue->Dequeue();
			if( pIO_Queue->Dequeue(&Op_Msg) )	{	// failed
				continue;
			}
//			memcpy(&Op_Msg, pOP_Msg_Retrieve, sizeof(IO_CMD_MSG));
			fetch_and_add((int*)&(ActiveJobList[Op_Msg.idx_JobRec].nOps_Done), 1);
			fetch_and_add((int*)&(Server_qp.pJob_OP_Send->Job_Op[idx_job].nOps_Done), 1);
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


// IO worker thread implementing fair queue

void* Func_thread_IO_Worker_FairQueue(void *pParam)
{
	//int i, idxTask, thread_id, idx_op, IdxMin, IdxMax, idx_JobRec, nNumQueuePerWorker, *pNext_IO_OP_Idx_Queue_List=NULL, range, nValid_Next_IO_OP=0;
	int i, idxTask, thread_id, idx_op, IdxMin, IdxMax, idx_JobRec, nNumQueuePerWorker, range, nValid_Next_IO_OP=0;
	IO_CMD_MSG Op_Msg, *pOP_Msg_Retrieve;
	CIO_QUEUE *pIO_Queue=NULL;
	//FIRSTOPLIST *pFirstOPList=NULL;
	FIRSTOPLIST pFirstOPList[400];// need to make sure it is larger than range!!!!!
	unsigned long int T_queue_Earlyest, T_queue_Earlyest_TargetJob;
	int idx_Earlyest, idx_Earlyest_TargetJob, idx_rec_ht_Picked, nValidOPs, ToProcOP, IdxQueue_PreviousSelected=-1, idx_Cur;
	struct timeval tm1, tm2;// tm1.tv_sec
	long int t_accum=0;
	long int nOp_Done=0;
	struct timeval tm;

	gettimeofday(&tm, NULL);
	rseed[0] = tm.tv_sec;
	rseed[1] = tm.tv_usec;

	thread_id = *((int*)pParam);
	printf("DBG> Func_thread_IO_Worker(): thread_id = %d\n", thread_id);
	CoreBinding.Bind_This_Thread();

	if(thread_id == 0){// the first thread is dedicated for inter-server communication via queue[0]
		IdxMin = 0;
		IdxMax = 0;
		//sleep(36000);
		pIO_Queue = &(IO_Queue_List[0]);
		while(1){
			if( (pIO_Queue->back) >= (pIO_Queue->front) ){// A queue that is not empty.
				pIO_Queue->Dequeue(pOP_Msg_Retrieve);
				memcpy(&Op_Msg, pOP_Msg_Retrieve, sizeof(IO_CMD_MSG));
				fetch_and_add((int*)&(ActiveJobList[pOP_Msg_Retrieve->idx_JobRec].nOps_Done), 1);
				Op_Msg.tid = thread_id;
				
				if (pthread_mutex_lock(&(Server_qp.pQP_Data[Op_Msg.idx_qp].qp_lock)) != 0) {
					perror("pthread_mutex_lock");
					exit(2);
				}
				
				Process_One_IO_OP(&Op_Msg);// Do the real IO work!
				nOPs_Done[thread_id]++;
				
				if (pthread_mutex_unlock(&(Server_qp.pQP_Data[Op_Msg.idx_qp].qp_lock)) != 0) {
					perror("pthread_mutex_unlock");
					exit(2);
				}
			}
		}
	}
	else{
		if( ( ( MAX_NUM_QUEUE - 1 ) % ( NUM_THREAD_IO_WORKER - 1 ) ) == 0 ){
			nNumQueuePerWorker = ( MAX_NUM_QUEUE - 1 ) / ( NUM_THREAD_IO_WORKER - 1 ) ;
		}
		else{
			nNumQueuePerWorker = ( MAX_NUM_QUEUE - 1 ) / ( NUM_THREAD_IO_WORKER - 1 ) + 1;
		}
		IdxMin = 1 + (thread_id-1)*nNumQueuePerWorker;
		IdxMax = MIN( (IdxMin + nNumQueuePerWorker - 1), (MAX_NUM_QUEUE - 1));
		printf("DBG> worker %d, (%d, %d)\n", thread_id, IdxMin, IdxMax);
		range = IdxMax-IdxMin+1;

		//pNext_IO_OP_Idx_Queue_List = (int*)malloc(sizeof(int)*range + sizeof(FIRSTOPLIST)*range);// the list of index to sorted list according to T_Queued
		//assert(pNext_IO_OP_Idx_Queue_List != NULL);
		//pFirstOPList = (FIRSTOPLIST *)((char*)pNext_IO_OP_Idx_Queue_List + sizeof(int)*range);

		for(i=0; i<range; i++){
			//pNext_IO_OP_Idx_Queue_List[i] = i;

			pFirstOPList[i].idx_queue = -1;
			pFirstOPList[i].idx_op = -1;
			pFirstOPList[i].T_Queued = LARGE_T_QUEUED;
		}

	}
	
	IdxQueue_PreviousSelected = IdxMin - 1;
	while(1){// loop forever
		if(nActiveJob == 0)continue;
		//gettimeofday(&tm1, NULL);
		idx_rec_ht_Picked = Random_Pick_a_TargetJob(rseed, 0);
		if(idx_rec_ht_Picked < 0)continue;

		// loop over all queues this IO worker needs to cover and extract job info for the first OP
		T_queue_Earlyest = T_queue_Earlyest_TargetJob = LARGE_T_QUEUED;
		idx_Earlyest = idx_Earlyest_TargetJob = -1;
		nValidOPs = 0;

		for(i=1; i<=range; i++){// All IO worker handle queues independently now!!! No lock is needed now.
			idx_Cur = IdxQueue_PreviousSelected + i;
			if(idx_Cur > IdxMax)idx_Cur -= range;
			idxTask = idx_Cur -IdxMin;

			if(pFirstOPList[idxTask].idx_queue < 0){// An invalid record. Need to grab the first OP info. 
				pIO_Queue = &(IO_Queue_List[idx_Cur]);
				if( (pIO_Queue->back) >= (pIO_Queue->front) ){// A queue that is not empty.
					pFirstOPList[idxTask].idx_queue = idx_Cur;
					idx_op = pIO_Queue->front & IO_QUEUE_SIZE_M1;
					pFirstOPList[idxTask].idx_op = idx_op;
					pFirstOPList[idxTask].idx_rec_ht = pIO_Queue->pQueue_Data[idx_op].idx_JobRec;
					pFirstOPList[idxTask].T_Queued = pIO_Queue->pQueue_Data[idx_op].T_Queued;

					nValidOPs++;
					// Need to insert this new record into the sorted list!!!!!!!!!!!!!!!!!!!!!!!
				}
				else{
					pFirstOPList[idxTask].T_Queued = LARGE_T_QUEUED;
				}
			}

			if(pFirstOPList[idxTask].T_Queued < LARGE_T_QUEUED){// a valid record!
				if(pFirstOPList[idxTask].idx_rec_ht == idx_rec_ht_Picked){
					if(pFirstOPList[idxTask].T_Queued < T_queue_Earlyest_TargetJob){
						T_queue_Earlyest_TargetJob = pFirstOPList[idxTask].T_Queued;
						idx_Earlyest_TargetJob = idxTask;
					}
				}
				if(pFirstOPList[idxTask].T_Queued < T_queue_Earlyest){// find the earliest OP
					T_queue_Earlyest = pFirstOPList[idxTask].T_Queued;
					idx_Earlyest = idxTask;
				}
			}
		}

		//printf("DBG> Rank = %d idx_rec_ht_Picked = %d\n", mpi_rank, idx_rec_ht_Picked);
		ToProcOP = 0;
		if( idx_Earlyest_TargetJob >=0 ){// process the earliest target job request
			IdxQueue_PreviousSelected = pFirstOPList[idx_Earlyest_TargetJob].idx_queue;
			pIO_Queue = &(IO_Queue_List[IdxQueue_PreviousSelected]);
			idx_op = pIO_Queue->front & IO_QUEUE_SIZE_M1;
			ToProcOP = 1;
			//printf("INFO> Proc target   OP %3d queue %4d Time %ld\n", pFirstOPList[idx_Earlyest_TargetJob].idx_queue, pIO_Queue->front, pFirstOPList[idx_Earlyest_TargetJob].T_Queued);
			pFirstOPList[idx_Earlyest_TargetJob].idx_queue = -1;
		}

		else if( idx_Earlyest >=0 ){// process the earliest request then
			IdxQueue_PreviousSelected = pFirstOPList[idx_Earlyest].idx_queue;
			pIO_Queue = &(IO_Queue_List[IdxQueue_PreviousSelected]);
			idx_op = pIO_Queue->front & IO_QUEUE_SIZE_M1;
			ToProcOP = 1;
			//printf("INFO> Proc earliest OP %3d queue %4d Time %ld\n", pFirstOPList[idx_Earlyest].idx_queue, pIO_Queue->front, pFirstOPList[idx_Earlyest].T_Queued);
			pFirstOPList[idx_Earlyest].idx_queue = -1;
		}


		if(ToProcOP){
			//gettimeofday(&tm2, NULL);
			//t_accum += ( (tm2.tv_sec - tm1.tv_sec) * 1000000 + (tm2.tv_usec - tm1.tv_usec) );
			nOp_Done++;
			//if(nOp_Done % 100000 == 0){
			//printf("INFO> thread_id = %d Overhead %5.3lf\n", thread_id, 1.0 * t_accum / nOp_Done);
			//}

			pIO_Queue->Dequeue(&Op_Msg);
			fetch_and_add((int*)&(ActiveJobList[Op_Msg.idx_JobRec].nOps_Done), 1);
			Op_Msg.tid = thread_id;
			
			if (pthread_mutex_lock(&(Server_qp.pQP_Data[Op_Msg.idx_qp].qp_lock)) != 0) {
				perror("pthread_mutex_lock");
				exit(2);
			}
			
			Process_One_IO_OP(&Op_Msg);// Do the real IO work!
			nOPs_Done[thread_id]++;
			
			if (pthread_mutex_unlock(&(Server_qp.pQP_Data[Op_Msg.idx_qp].qp_lock)) != 0) {
				perror("pthread_mutex_unlock");
				exit(2);
			}
		}
	}
}


// IO worker thread implementing FIFO queue

void* Func_thread_IO_Worker_FIFO(void *pParam)	// process all IO wrok
{
	int i, thread_id, idx_op, IdxMin, IdxMax, idx_JobRec, nNumQueuePerWorker;
	IO_CMD_MSG Op_Msg;
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
				if( ! pIO_Queue->Dequeue(&Op_Msg) )	{	// 0 - success (not empty)
					fetch_and_add((int*)&(ActiveJobList[Op_Msg.idx_JobRec].nOps_Done), 1);
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
	else	{
		if( ( ( MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER ) % ( NUM_THREAD_IO_WORKER - NUM_THREAD_IO_WORKER_INTER_SERVER ) ) == 0 )	{
			nNumQueuePerWorker = ( MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER ) / ( NUM_THREAD_IO_WORKER - NUM_THREAD_IO_WORKER_INTER_SERVER ) ;
		}
		else	{
			nNumQueuePerWorker = ( MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER ) / ( NUM_THREAD_IO_WORKER - NUM_THREAD_IO_WORKER_INTER_SERVER ) + 1;
		}
		IdxMin = NUM_THREAD_IO_WORKER_INTER_SERVER + (thread_id-NUM_THREAD_IO_WORKER_INTER_SERVER)*nNumQueuePerWorker;
		IdxMax = MIN( (IdxMin + nNumQueuePerWorker - 1), (MAX_NUM_QUEUE - 1));
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
						pIO_Queue->Dequeue(&Op_Msg);
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


// IO worker thread, either fair or FIFO
void* Func_thread_IO_Worker(void *pParam)
{

	// return Func_thread_IO_Worker_LeiSizeFair(pParam);
	// return Func_thread_IO_Worker_FairQueue(pParam);
	return Func_thread_IO_Worker_FIFO(pParam);

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
//	case RF_RW_OP_PREAD:
//		RW_PRead(pOP_Msg);
//		break;
//	case RF_RW_OP_PWRITE:
//		RW_PWrite(pOP_Msg);
//		break;
//	case RF_RW_OP_SEEK:
//		RW_Seek(pOP_Msg);
//		break;
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
	case RF_RW_OP_FREE_STRIPE_DATA:
		RW_Free_Stripe_Data(pOP_Msg);
		break;
	case RF_RW_OP_PRINT_MEM:
		RW_Print_Mem();
		break;
	case RF_RW_OP_HELLO:
		RW_Hello(pOP_Msg);
		break;
	case RF_RW_OP_STAT_FS:
		RW_StatFS(pOP_Msg);
		break;
	case RF_RW_OP_READ_DIR_ENTRIES:
		RW_Read_Dir_Entries(pOP_Msg);
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

int CIO_QUEUE::Dequeue(IO_CMD_MSG *pOp_Msg)	// 1 - Queue is empty, 0 - Success. 
{
	IO_CMD_MSG *pMsg;
		
	if (pthread_mutex_lock(&lock) != 0) {
		perror("pthread_mutex_lock");
		exit(2);
	}
	if( back >= front )	{	// A queue that is not empty.
		memcpy(pOp_Msg, &(pQueue_Data[front & IO_QUEUE_SIZE_M1]), sizeof(IO_CMD_MSG));
		front++;
//		nTokenReload++;
	}
	else	{	// empty
		if (pthread_mutex_unlock(&lock) != 0) {
			perror("pthread_mutex_unlock");
			exit(2);
		}
		return 1;	// empty queue
	}

	if (pthread_mutex_unlock(&lock) != 0) {
		perror("pthread_mutex_unlock");
		exit(2);
	}
	return 0;	// success
}

