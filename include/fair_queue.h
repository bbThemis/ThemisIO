#include <cassert>
// #include <cstdlib>
#include <cstring>
#include <pthread.h>
#include <vector>
#include <queue>
#include <unordered_map>
#include <set>
#include <algorithm>
#include <random>
#include <sstream>
#include <iomanip>
#include "qp.h"
#include "io_queue.h"

#define TIME_PER_CYCLE_MICROSEC       (30)
//#define TIME_PER_CYCLE_MICROSEC	(40)	// the length of one time slice. The time spended on each job is t*w. 
//#define TIME_PER_CYCLE_MICROSEC       (1000)

/* Encapsulate looking up data on a job. The ActiveJobList data
	 structure is likely to change, so when it does, all that needs to
	 change is this interface class rather than all the code that uses
	 it. */
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
	/* mode: fairness mode (enum defined in qp.h)
		 job_info_lookup: a wrapper used to access the active job list
		 max_idle_sec: deallocate queues that have been empty for at least this many seconds.
	*/
	FairQueue(FairnessMode mode, int mpi_rank, int thread_id,
						JobInfoLookup &job_info_lookup, int max_idle_sec = 10);
	~FairQueue();
	
	bool isEmpty();

	// returns the number of queued messages
	int getCount();

	// Add a message to the queue. The data is copied from 'msg'.
	void putMessage(const IO_CMD_MSG *msg);

	// Selects a message to process. If there are no messages, this returns false.
	// Otherwise this copies the message to 'msg' and returns true.
	bool getMessage(IO_CMD_MSG *msg);

	// This will be called occasionally.
	// It provides a way to print status output or perform garbage collection.
	void housekeeping();

	// Returns the current time in microseconds since epoch.
	static long unsigned getTime();

	// Returns the number of seconds since the object was created.
	double getElapsed();

  // unused remnants of time-sharing version
  /*
	// Note: it is bad design to have multiple versions of putMessage and getMessage.
	// The scheduling algorithm is set when the object is constructed.
	// Once the algorithm is set, the user of this class shouldn't have to to
	// anything different to use different scheduling algorithms.
	void putMessage_TimeSharing(const IO_CMD_MSG *msg);
	bool getMessage_FromActiveJob(IO_CMD_MSG *msg);
	void Update_Job_Weight(void);
	// recharge all jobs with designed time length
	void reload();
	void SetFirstJobActive(void);
	void SetNextJobActive(void);
  */
	
private:
  // unused remnants of time-sharing version
	/*
	// the sum of weight of all jobs
	float weight_sum;
	int nJob;
	int IdxActiveJob;
	int pad;
	// MessageQueue *q_ActiveJob;
	double T_ThisCycle;
	*/

	struct MessageContainer {
		IO_CMD_MSG msg;

		MessageContainer() {}
		
		// use memcpy rather than field-by-field copy
		MessageContainer(const MessageContainer &other) {
			memcpy(&msg, &other.msg, sizeof msg);
		}
	};
	
	struct MessageQueue {
		MessageQueue(int id_, int job_id_, int user_id_, long now, int weight_)
			: id(id_), job_id(job_id_), user_id(user_id_), idle_timestamp(now), weight(weight_) {
				// T_Create = now * 1.0;
			}
								 
		std::queue<MessageContainer> messages;

		// id, either job id or user id, depending on fairness mode
		int id;
		float weight;	 // size-fair: node count. Otherwise 1.

		int job_id, user_id;

		// timestamp this queue was most recently nonempty
		long unsigned idle_timestamp;
		
		// unused remnants of time-sharing version
		/*
		// The time when current queue was create for current job
		long int T_Create;

		// the time balance current queue has. It could be negative since one long OP could take long time. 
		long int T_Balance;

		// the length of time slice reloading for current job. Proportional to weight. TIME_PER_CYCLE_MICROSEC * weight
		long int dT_Reload;

		// the time current job was schduled in current cycle
		long int T_Cycle_Start;

		// accumulated time for operation
		long int T_Accum_Op;

		// accumulated time for idling
		long int T_Accum_Idle;
		*/

		void add(const IO_CMD_MSG *msg) {
			MessageContainer *mc = (MessageContainer*) msg;
			messages.push(*mc);
		}

		bool remove(IO_CMD_MSG *msg) {
			if (messages.empty()) return false;
			MessageContainer &msg_c = messages.front();
			memcpy(msg, &msg_c.msg, sizeof *msg);
			messages.pop();

			return true;
		}

		/* Computes a priority for this queue, which is currently just the 
			 weight of the queue. For job-fair and user-fair queues it's 1,
			 and for size-fair queues it's the node count of the job. */
		double getPriority(long unsigned now) {
			return weight;
		}
		
	};


	/* Log fairness decisions, tracking the set of messages queues with
		 data and which was selected. Choices from the same set of queues
		 will be consolidated into one entry.

		 Each set of choices is represented with an integer array.
       array[0] = N = # of queues from choice a choice was made
			 array[1..N] = id numbers for the input queues. These may be job ids
			   or user ids, depending on the fairness mode
			 array[0..N] is the key for the 'decisions' set
			 array[N+1..2N] = frequency count for each choice

     For example, if a set of possible ids (50,123,999) were
     considered in 1000 instances, and 50 was chosen 10% of the time,
     123 40% of the time, and 999 50% of the time, this will be stored
     as [3, 50, 128, 999, 100, 400, 500] and in toString form it will
     be "50,123,999:100,400,500".
  */
	class DecisionLog {
	public:
		// using a constant so the compiler can easily disable the code with no overhead
		static const bool enabled = false;

		DecisionLog() {}
		~DecisionLog() {clear();}

		// track how many threads there are, so we know how many need to check in before
		// we output a report containing everyone's data.
		static void changeThreadCount(int count) {
			pthread_mutex_lock(&shared_decision_log_lock);
			thread_count += count;
			pthread_mutex_unlock(&shared_decision_log_lock);
		}
		
		void log(const std::vector<MessageQueue*> &nonempty_queues, int choice_id);

		// combine the counts from that to this
		void addLog(const DecisionLog &that);
		
		bool empty() {return decisions.empty();}

		// represent all the decision sets in an easy-to-parse way
		std::string toString();

		// reset all the counters, deallocating memory
		void clear();

	private:

		// order decision sets by size and then ids
		struct DecisionSetLessThan {
			bool operator()(const int *a, const int *b) {
				int n = a[0];
				if (n != b[0])
					return n < b[0];

				for (int i=1; i < n+1; i++)
					if (a[i] != b[i])
						return a[i] < b[i];

				return false;
			}
		};


		// represent one decision set as an easy-to-parse string
		std::string decisionSetToString(int *data);
  
		std::vector<int> temp_storage;
		std::set<int*, DecisionSetLessThan> decisions;

	};  // DecisionLog


	// return index into nonempty_queues
	int chooseRandomNonemptyQueue();

	
	int getKey(const IO_CMD_MSG *msg) {
		if (fairness_mode == USER_FAIR) {
			return job_info_lookup.getUserId(msg);
		} else {
			return job_info_lookup.getSlurmJobId(msg);
		}
	}

	void reportDecisionLog();
	
	// Scans all message queues and removes those which have been idle for too long.
	// This is called by housekeeping().
	void purgeIdle();
	
	FairnessMode fairness_mode;
	int mpi_rank, thread_id;
	JobInfoLookup &job_info_lookup;
	unsigned long start_time_usec;
	int max_idle_sec;

	// total number messages in queue
	int message_count;

	std::default_random_engine random_engine;
	
	// all current message queues
	// key is job_id or user_id
	std::unordered_map<int, MessageQueue*> indexed_queues;

	// ??? misleading comment
	// all nonempty message queues
	// std::vector<MessageQueue*> all_queues;

	// all nonempty message queues
	std::vector<MessageQueue*> nonempty_queues;

	// Used in chooseRandomNonemptyQueue(). This is filled with the
	// priority for each nonempty queue, then one is chosen randomly.
	std::vector<double> nonempty_priorities;

	// Call purgeIdle() when a message comes in with a timestamp after this.
	long unsigned next_purge_timestamp;

	DecisionLog decision_log;

	// shared by all threads, shared_decision_log_lock protects these variables
	static pthread_mutex_t shared_decision_log_lock;
	static DecisionLog shared_decision_log;
	static int thread_count, n_threads_reported;
};
