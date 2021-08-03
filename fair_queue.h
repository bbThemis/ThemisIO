#include <cassert>
// #include <cstdlib>
#include <cstring>
#include <pthread.h>
#include <queue>
#include <unordered_map>
#include <random>
#include "io_queue.h"

#if 0
typedef struct	{
	int fd;				// fd valid only on the host holding the file
	unsigned int rkey;				// remote key for RDMA
	void *rem_buff;// the buffer for remote memory access. For return code, errno and results sent back!
	char szName[160];	// May need to be increased later!

	int nLen_FileName;			// the length of szName
	int nLen_Parent_Dir_Name;	// the length of parent dir

	int flag;			// open() needs flag
	int mode;			// open() needs mode when creating files

	long int offset;	// the offset of file
	unsigned long int nLen;		// the number of bytes to read/write
	
	int tag_magic;
	int op;		// operation tag. Containing a magic tag at the end! // Offset 212
	
	unsigned long long file_hash;	// file hash calculated on each client to save the cpu on file server
	unsigned long long parent_dir_hash;

	// -----------------------------------------------------------------------------
	// Unued
//	int idx;			// index in the list of openned remote files
//	int pad;

	// The data set by server
	int idx_qp;			// index of queue pair
	int tid;			// the index of IO worker that is handling this request
	int idx_JobRec;		// the index of job record. It is determined by jobid which is known from QP.
      // AKA index into ActiveJobList[]
	int nTokenNeeded;	// the number of token needed to finish this operation
	unsigned long int T_Queued;	// time in us when this OP was queued. 
}IO_CMD_MSG, *PIO_CMD_MSG;



typedef	struct	{
	int jobid;	// slurm job id
	int nnode;	// the number of node of this job. nTokenPerReload will be calculated based on this number. 
	int nQP;	// number of queue pairs are associated with this jobid.

  // int pad;
  int uid;  // user id

	long int Time;	// time stamp in seconds of last reload
	long int nTokenAV;	// the number of token available
	long int nTokenReload;	// the number of token recharge in a new cycle. It is calculated from job size (priority). It could be adjusted based on global historical usage among the whole file systems on all nodes. 
	long int nOps_Done, nOps_Done_LastCycle;
	long int T_Last_OP;	// the time stamp in second when last OP request was processed. 
	pthread_mutex_t lock;	// 40 bytes
	// nOp in current queue, nOps_Done - Ops done in current cycle, nOps_Done_LastCycle - done in last cycle. It will be used for algathering and next cycle allocation projection. 
}JOBREC,*PJOBREC;
#endif


/* Encapsulate looking up data on a job. The ActiveJobList data structure is likely
   to change, so when it does, all that needs to change is this interface class
   rather than all the code that uses it. */
class JobInfoLookup {
public:
	JobInfoLookup(JOBREC *ActiveJobList_, int *active_job_count_)
		: ActiveJobList(ActiveJobList_), active_job_count(active_job_count_) {}

	int getSlurmJobId(const IO_CMD_MSG *msg) {
		return ActiveJobList[jobKey(msg)].jobid;
	}

	int getNodeCount(const IO_CMD_MSG *msg) {
		return ActiveJobList[jobKey(msg)].nnode;
	}

	int getUserId(const IO_CMD_MSG *msg) {
		return ActiveJobList[jobKey(msg)].uid;
	}

private:
	int jobKey(const IO_CMD_MSG *msg) {
		int job_key = msg->idx_JobRec;
		assert(job_key >= 0 && job_key < *active_job_count);
		return job_key;
	}

	JOBREC *ActiveJobList;
	int *active_job_count;
};


/* Not thread-safe.

	 Currently each IO_CMD_MSG struct is copied into a queue in putMessage()
	 and copied out in getMessage(). A copy could be eliminated by allocating
	 IO_CMD_MSG structs independently and queuing just the pointers.
	 For efficiency, it would be good to use a local pool allocator for that.

*/
class FairQueue {
 public:
	enum class Mode {SIZE_FAIR, JOB_FAIR, USER_FAIR};

	/* mode: fairness mode
		 job_info_lookup: a wrapper used to access the active job list
		 max_idle_sec: deallocate queues that have been empty for at least this many seconds.
	*/
	FairQueue(Mode mode, JobInfoLookup &job_info_lookup, int max_idle_sec = 600);
	~FairQueue();

	// Add a message to the queue. The data is copied from 'msg'.
	void putMessage(const IO_CMD_MSG *msg);
	
	bool isEmpty();

	// Selects a message to process. If there are no messages, this returns false.
	// Otherwise this copies the message to 'msg' and returns true.
	bool getMessage(IO_CMD_MSG *msg);

	
private:
	struct MessageContainer {
		IO_CMD_MSG msg;

		MessageContainer() {}
		
		// use memcpy rather than field-by-field copy
		MessageContainer(const MessageContainer &other) {
			memcpy(&msg, &other.msg, sizeof msg);
		}
	};
	
	struct MessageQueue {
		MessageQueue(long now, int weight_) : idle_timestamp(now), weight(weight_) {}
								 
		std::queue<MessageContainer> messages;
		long unsigned idle_timestamp;	 // timestamp this queue was most recently nonempty
		const int weight;	 // size-fair: node count. Otherwise 1.
	};

	// return index into nonempty_queues
	int chooseRandomNonemptyQueue();

	
	int getKey(const IO_CMD_MSG *msg) {
		if (fairness_mode == Mode::USER_FAIR) {
			return job_info_lookup.getUserId(msg);
		} else {
			return job_info_lookup.getSlurmJobId(msg);
		}
	}

	// return the current time in microseconds since epoch
	static long unsigned now();
	
	// scan all message queues and remove those which have been idle for too long
	void purgeIdle();

	Mode fairness_mode;
	JobInfoLookup &job_info_lookup;
	int max_idle_sec;

	std::default_random_engine random_engine;
	
	// all current message queues
	// key is job_id or user_id
	typedef std::unordered_map<int, MessageQueue*> IndexedQueuesType;
	IndexedQueuesType indexed_queues;

	// all nonempty message queues
	std::vector<MessageQueue*> nonempty_queues;
	std::vector<int> nonempty_weights;
	
	// 0 if queues are unweighted
	long total_nonempty_weight;

	// Call purgeIdle() when a message comes in with a timestamp after this.
	long unsigned next_purge_timestamp;
};
