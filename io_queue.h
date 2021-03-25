#ifndef __IO_QUEUE
#define __IO_QUEUE

#include <pthread.h>
#include "io_ops_common.h"

#define LARGE_T_QUEUED	(0x60000000000000UL)
#define	N_SECOND_THRESHOLD_REMOVE_JOB	(15)	// When (nQP == 0) and dT > N_SECOND_THRESHOLD_REMOVE_JOB, remove current job from active job list

//#define NUM_THREAD_IO_WORKER	(1)
//#define NUM_THREAD_IO_WORKER  (16+1)
#define NUM_THREAD_IO_WORKER  (16+1)
//#define NUM_THREAD_IO_WORKER	(4)

#define MAX_NUM_ACTIVE_JOB	(1024)	// max number of concurrent slurm job id

//#define MAX_NUM_QUEUE	(1024)
#define MAX_NUM_QUEUE	(720 + 1)	// The first queue is reserved for inter-server communications! 
#define MAX_NUM_QUEUE_M1	(MAX_NUM_QUEUE - 1)

#define NUM_QUEUE_PER_WORKER	(MAX_NUM_QUEUE_M1/(NUM_THREAD_IO_WORKER-1))

#define INVALID_JOBID	(-1)
#define INVALID_WORKERID	(-1)
#define SERVER_JOBID	(0)
#define SCLAE_NNODE_to_TOKEN	(10000)
#define LARGE_NNODE		(1024*256)	// For queue 0 that is for inter-server IO comminication.

#define IO_QUEUE_SIZE	(1024)
#define IO_QUEUE_SIZE_M1	(IO_QUEUE_SIZE-1)

void Init_ActiveJobList(void);
void Init_QueueList(void);
void Init_NewActiveJobRecord(int idx_rec, int jobid, int nnode);
void* Func_thread_IO_Worker(void *pParam);	// process all IO wrok
void Update_Active_JobList(void);
void ConstructJobProbabilityList(void);

// The first queue takes care of special jobs (jobid == 0). e.g., query whether a directory exists or not. No token is needed for such OPs. 

void Process_One_IO_OP(IO_CMD_MSG *pOP_Msg);

typedef	struct	{
	int jobid;	// slurm job id
	int nnode;	// the number of node of this job. nTokenPerReload will be calculated based on this number. 
	int nQP;	// number of queue pairs are associated with this jobid.
	int pad;

	long int Time;	// time stamp in seconds of last reload
	long int nTokenAV;	// the number of token available
	long int nTokenReload;	// the number of token recharge in a new cycle. It is calculated from job size (priority). It could be adjusted based on global historical usage among the whole file systems on all nodes. 
	long int nOps_Done, nOps_Done_LastCycle;
	long int T_Last_OP;	// the time stamp in second when last OP request was processed. 
	pthread_mutex_t lock;	// 40 bytes
	// nOp in current queue, nOps_Done - Ops done in current cycle, nOps_Done_LastCycle - done in last cycle. It will be used for algathering and next cycle allocation projection. 
}JOBREC,*PJOBREC;


typedef	struct	{
	int jobid;	// slurm job id
	int idx_rec_ht;	// index that would be found by hashtable querying
}LISTJOBREC,*PLISTJOBREC;

typedef	struct	{
	int idx_queue, idx_op;
	int idx_rec_ht, pad;	// index that would be found by hashtable querying
	unsigned long int T_Queued;
}FIRSTOPLIST,*PFIRSTOPLIST;

class CIO_QUEUE {
public:
	int IdxWorker;	// the index of the IO worker who is processing this queue. -1 means not being processes. 
	long int front, back;	// 16 bytes
	pthread_mutex_t lock;	// 40 bytes
	IO_CMD_MSG *pQueue_Data;		// 8 bytes

	void Enqueue(IO_CMD_MSG *pOp_Msg);
	IO_CMD_MSG* Dequeue(void);
};

#endif
