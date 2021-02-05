#ifndef __IO_QUEUE
#define __IO_QUEUE

#define MAX_NUM_QUEUE	(1024)
#define INVALID_JOBID	(-1)
#define SERVER_JOBID	(0)
#define SCLAE_NNODE_to_TOKEN	(10000)

void Init_QueueList(void);
int FindFirstAvailableSpaceForQueue(void);	// Search from FirstAV_Queue 
void Free_A_Queue(int idx);
int Create_A_Queue(int jobid);
int Query_Jobid_In_Queue(int jobid);
//void Associate_A_QP_with_Queue(int jobid);

// The first queue takes care of special jobs (jobid == 0). e.g., query whether a directory exists or not. No token is needed for such OPs. 

class IO_QUEUE {	// Each io queue dedicates to only one slurm job id. We can easily control priority/number of tokens. 
public:
	int IdxWorker;	// the index of the IO worker who is processing this queue. -1 means not being processes. 
	int nnode;	// the number of node of this job. nTokenPerReload will be calculated based on this number. 
	int nQP;	// number of queue pairs are associated with this jobid.
	int jobid;	// slurm job id
	
	long int Time;	// time stamp in seconds of last reload
	long int nTokenAV;	// the number of token available
	long int nTokenReload;	// the number of token recharge in a new cycle. It is calculated from job size (priority). It could be adjusted based on global historical usage among the whole file systems on all nodes. 
	long int nOps_ToDo, nOps_Done, nOps_Done_LastCycle, nOps_Done_Accum;
	// nOp in current queue, nOps_Done - Ops done in current cycle, nOps_Done_LastCycle - done in last cycle. It will be used for algathering and next cycle allocation projection.  
};



#endif
