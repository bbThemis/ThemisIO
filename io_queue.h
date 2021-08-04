#ifndef __IO_QUEUE
#define __IO_QUEUE

#include <pthread.h>
#include "io_ops_common.h"

#define T_WINDOW	(500000)	// unit is us. 
#define T_FOR_UPLOAD	(T_WINDOW - 3000)	// unit is us. 
#define T_FOR_SUM	(T_FOR_UPLOAD + 1500)	// unit is us. 
#define N_OP_DONE_THRESHOLD	(20)	// should proportional to T_WINDOW
#define PROB_LOWWER_BOUND	(0.0002)

#define N_BYTE_READ_SCALING	(192)	// make it smaller than the max inline size

#define LARGE_T_QUEUED	(0x60000000000000UL)
#define	N_SECOND_THRESHOLD_REMOVE_JOB	(15)	// When (nQP == 0) and dT > N_SECOND_THRESHOLD_REMOVE_JOB, remove current job from active job list

#define NUM_THREAD_IO_WORKER_INTER_SERVER  (8)
#define NUM_THREAD_IO_WORKER  (16+NUM_THREAD_IO_WORKER_INTER_SERVER)
//#define NUM_THREAD_IO_WORKER  (14+NUM_THREAD_IO_WORKER_INTER_SERVER)

#define MAX_NUM_ACTIVE_JOB	(1024)	// max number of concurrent slurm job id
//#define MAX_NUM_QUEUE (1120 + NUM_THREAD_IO_WORKER_INTER_SERVER)
#define MAX_NUM_QUEUE (640 + NUM_THREAD_IO_WORKER_INTER_SERVER)
//#define MAX_NUM_QUEUE	(640 + NUM_THREAD_IO_WORKER_INTER_SERVER)	// The first queue is reserved for inter-server communications! 
#define MAX_NUM_QUEUE_MX	(MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER)

#define NUM_QUEUE_PER_WORKER	(MAX_NUM_QUEUE_MX/(NUM_THREAD_IO_WORKER-NUM_THREAD_IO_WORKER_INTER_SERVER))

#define INVALID_JOBID	(-1)
#define INVALID_WORKERID	(-1)
#define SERVER_JOBID	(0)
#define SCLAE_NNODE_to_TOKEN	(10000)
#define LARGE_NNODE		(1024*256)	// For queue 0 that is for inter-server IO comminication.

#define IO_QUEUE_SIZE	(1024)
#define IO_QUEUE_SIZE_M1	(IO_QUEUE_SIZE-1)

void Init_ActiveJobList(void);
void Init_QueueList(void);
void Init_NewActiveJobRecord(int idx_rec, int jobid, int nnode, int user_id);
void* Func_thread_IO_Worker(void *pParam);	// process all IO wrok
void* Func_thread_Global_Fair_Sharing(void *pParam);

void Update_Active_JobList(void);
void ConstructJobProbabilityList(void);

// The first queue takes care of special jobs (jobid == 0). e.g., query whether a directory exists or not. No token is needed for such OPs. 

void Process_One_IO_OP(IO_CMD_MSG *pOP_Msg);

typedef	struct	{
	int jobid;	// slurm job id
	int nnode;	// the number of node of this job. nTokenPerReload will be calculated based on this number. 
	int nQP;	// number of queue pairs are associated with this jobid.
	int uid;  // user id

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
	int jobid;	// slurm job id
	float scale;	// Scaling factor
}JOB_SCALE,*PJOB_SCALE;	// The data all server gets from rank 0. 

typedef	struct	{
	int jobid;	// slurm job id
	int nnode;	// the number of nodes allocated 
	long int nOps_Done;	// The number of operations processed in last window
}JOB_OP_REC,*PJOB_OP_REC;

typedef	struct	{
//	struct timeval T_op;	// 16 bytes
	int nActiveJob, pad;
	long int T_op_us;	// time in ms since the beginning 
	JOB_OP_REC Job_Op[MAX_NUM_ACTIVE_JOB-1];	// each record uses 16 bytes. -1 for alignment!!!!!
}JOB_OP_SEND,*PJOB_OP_SEND;

typedef	struct	{
	int nActiveJob, pad;
	long int T_op_us;	// time in ms since the beginning 
	JOB_SCALE Job_Scale[MAX_NUM_ACTIVE_JOB-2];	// each record uses 16 bytes. -2 for alignment!!!!!!!
}JOB_SCALE_LIST,*PJOB_SCALE_LIST;

typedef	struct	{
	// index into IO_Queue_List
	int idx_queue;

	// position of op in that queue, IO_Queue_List[idx_queue].pQueue_Data[idx_op]
	int idx_op;

	// index that would be found by hashtable querying
	// and index into ActiveJobList[]
	int idx_rec_ht;

	int pad;

	// timestamp when the request was received in microseconds
	unsigned long int T_Queued;
}FIRSTOPLIST,*PFIRSTOPLIST;

class CIO_QUEUE {
public:
	int IdxWorker;	// the index of the IO worker who is processing this queue. -1 means not being processes. 
	volatile long int front, back;	// 16 bytes
	pthread_mutex_t lock;	// 40 bytes
	IO_CMD_MSG *pQueue_Data;		// 8 bytes

	void Enqueue(IO_CMD_MSG *pOp_Msg);
	int  Dequeue(IO_CMD_MSG *pOp_Msg);	// 1 - Queue is empty, 0 - Success. 
};

#endif
