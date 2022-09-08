#include <cassert>
// #include <cstdlib>
#include <cstring>
#include <pthread.h>
#include <map>
#include <vector>
#include <queue>
#include <unordered_map>
#include <set>
#include <algorithm>
#include <random>
#include <sstream>
#include <iomanip>

#ifndef FAIR_QUEUE_STANDALONE
#include "qp.h"
#include "io_queue.h"
#endif

// the length of one time slice. The time spent on each job is time*weight. 
// Currently unused.
// #define TIME_PER_CYCLE_MICROSEC       (30)

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

	 Fairness options:
	   mode: fifo, job-fair, or user-fair
		   In fifo mode, all messages are put in the same queue and processed in the order
			   in which they were received.
		   job-fair: maintain one message queue per job id. When selecting a
			   message, choose one of the per-job queues and process the first message.
			 user-fair: same as job fair, but there is one queue per user id.

			 indexed_queues maps id (either job or user) to MessageQueue*

     selection method
		   probabilistic: weight each queue, choose randomly (count-balanced)
			   Nonempty queues are maintained in a vector. When one is added it is appended.
				 When one is remove it is swapped with the last.
			 cycle: round-robin order with repeats
			   Nonempty queues are maintained in a map so that removing
				 one does not change the order of others.
			   How to do weighting: maintain rolling_weight, starting at 0.
				 On each cycle, rollover_weight += weight. Process up to floor(rollover_weight) messages,
				 then subtract floor(rollover_weight).
				 0, 1.7 (do 1) .7 -> 2.4 (do 2) .4 -> 2.1 (do 2) .1 -> 1.8 (do 1) ...
			 time: choose queue with least cumulative time
			   Nonempty queues are maintained in a heap where the heap with the minimum
				 cumulative time is at the top.

			   FYI: timer function performance
				 MPI_Wtime: avg call time 17.3ns, precision .23ns
				 std::chrono: avg call time 19.2ns, precision 1ns
				 gettimeofday(microseconds): avg call time 18.7ns, precision 1000ns
				 clock_gettime(CLOCK_MONOTONIC): avg call time 18.0ns, precision 1ns
				 clock_gettime(CLOCK_MONOTONIC_COARSE) avg call time 5.0ns, precision 1000000ns
				 clock_gettime(CLOCK_MONOTONIC_RAW) avg call time 292ns, precision 1ns
				 __rdtsc(clock ticks): avg call time 6.3ns, precision 2 ticks or .74ns

		 weight_by_node_count
		   If this is true, then each queue is weighted by the number of nodes it represents.
			 In job-fair mode this is the number of nodes in the job. In user-fair mode this is
			 the sum of the number of nodes in all jobs this user has running.

			 BUG: user-fair node weighting isn't working right now

*/
class FairQueue {
 public:
	/* mode: who is competing, jobs or users (enum defined in qp.h)
		 weight_by_node_count: implement size-fair by weighting queues by node count
		 job_info_lookup: a wrapper used to access the active job list
		 max_idle_sec: deallocate queues that have been empty for at least this many seconds.
	*/
	FairQueue(FairnessMode mode,
						bool weight_by_node_count,
						int mpi_rank, int thread_id,
						JobInfoLookup &job_info_lookup, int max_idle_sec = 10);
	virtual ~FairQueue();
	
	// bool isEmpty();

	// returns the number of queued messages
	int getCount() {return message_count;}

	// Add a message to the queue. The data is copied from 'msg'.
	void putMessage(const IO_CMD_MSG *msg);

	// Selects a message to process. If there are no messages, this returns false.
	// Otherwise this copies the message to 'msg' and returns true.
	virtual bool getMessage(IO_CMD_MSG *msg) = 0;

	// After processing the last message returned by getMessage,
	// caller can call again to tell me the cost of handling the message.
	// This can be used to track how much time was spent on each message.
	virtual void chargePreviousMessageCost(double value) {}

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
	
 protected:

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
		: id(id_), weight(weight_), carry_weight(weight_), job_id(job_id_), user_id(user_id_),
			idle_timestamp(now), cumulative_time(0) {
			assert(weight > 0);
				// T_Create = now * 1.0;
			}
								 
		std::queue<MessageContainer> messages;

		// id, either job id or user id, depending on fairness mode
		int id;
		float weight;	 // size-fair: node count. Otherwise 1.

		// used to implement weight in FAIR_ORDER_CYCLE mode
		float carry_weight;

		int job_id, user_id;

		// timestamp this queue was most recently nonempty
		long unsigned idle_timestamp;

		// time spent processing messages from this queue
		// used by FairQueueTime
		double cumulative_time;
		
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

	// implemented by subclasses, since each one organizes the queues differently
	virtual void addNonemptyQueue(int key, MessageQueue *q) = 0;


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
		static const bool enabled = true;

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
		void log(const std::map<int,MessageQueue*> &nonempty_queues, int choice_id);

    // This is called by both versions of log() above after they have filled
    // temp_storage with [n, id(0), id(1), ..., id(n-1)].
    void logInternal(int choice_id);

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
			bool operator()(const int *a, const int *b) const {
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


	// Given a message, return the key used to index into indexed_queues.
	// In other words, if there is one queue per job this will return the job id,
	// and if there is one queue per user it will return the user id.
	// In FIFO mode everything ends up in one queue, so this returns 0.
	int getKey(const IO_CMD_MSG *msg) {
		switch (fairness_mode) {
		case USER_FAIR: return job_info_lookup.getUserId(msg);
		case JOB_FAIR: return job_info_lookup.getSlurmJobId(msg);
		default:
			assert(fairness_mode == FIFO);
			return 0;
		}
	}

	void reportDecisionLog();
	
	// Scans all message queues and removes those which have been idle for too long.
	// This is called by housekeeping().
	void purgeIdle();

	// user-fair, or job-fair
	FairnessMode fairness_mode;
	bool weight_by_node_count;

	int mpi_rank, thread_id;
	JobInfoLookup &job_info_lookup;
	unsigned long start_time_usec;
	int max_idle_sec;

	// total number messages in queue
	int message_count;

	std::default_random_engine random_engine;
	
	// all current message queues, empty or not
	// key is job_id or user_id
	using IndexedQueueType = std::unordered_map<int, MessageQueue*>;
	IndexedQueueType indexed_queues;

	// duplicate data structure created for timesharing code
	// std::vector<MessageQueue*> all_queues;

	// Call purgeIdle() when a message comes in with a timestamp after this.
	long unsigned next_purge_timestamp;

	DecisionLog decision_log;

	// shared by all threads, shared_decision_log_lock protects these variables
	static pthread_mutex_t shared_decision_log_lock;
	static DecisionLog shared_decision_log;
	static int thread_count, n_threads_reported;
};


/* Choose message queue probabilistically. Nonempty queues are stored in a vector. */
class FairQueueRandom : public FairQueue {
 public:
	FairQueueRandom(FairnessMode mode,
									bool weight_by_node_count,
									int mpi_rank, int thread_id,
									JobInfoLookup &job_info_lookup,
									int max_idle_sec = 10)
		: FairQueue(mode, weight_by_node_count, mpi_rank, thread_id, job_info_lookup, max_idle_sec)
		{}
	

	virtual void addNonemptyQueue(int key, MessageQueue *q) {
		nonempty_queues.push_back(q);
	}

	virtual bool getMessage(IO_CMD_MSG *msg);
	// virtual void removeNonemptyQueue(MessageQueue *q);

 private:
	bool isEmpty() {return nonempty_queues.empty();}
	int chooseRandomNonemptyQueue();

	std::vector<MessageQueue*> nonempty_queues;

	// Used in chooseRandomNonemptyQueue(). This is filled with the
	// priority for each nonempty queue, then one is chosen randomly.
	std::vector<double> nonempty_priorities;
};


/* Cycle through nonempty message queues in round-robin order.
	 Queues are stored in an ordered map. An unordered map would
	 be OK too, probably a little faster, but it could lead to
	 irregular patterns of cycles if entries are repeatedly remove
	 and re-added.
. */
class FairQueueCycle : public FairQueue {
 public:
	FairQueueCycle(FairnessMode mode,
								 bool weight_by_node_count,
								 int mpi_rank, int thread_id,
								 JobInfoLookup &job_info_lookup,
								 int max_idle_sec = 10)
		: FairQueue(mode, weight_by_node_count, mpi_rank, thread_id, job_info_lookup, max_idle_sec),
		next_queue(nonempty_queues.end())
		{}

	virtual void addNonemptyQueue(int key, MessageQueue *q) {
		nonempty_queues[key] = q;
		q->carry_weight = q->weight;
	}

	virtual bool getMessage(IO_CMD_MSG *msg);

 private:
	// key is user id or job id, depending on FairQueue::fairness_mode
	using QueueMap = std::map<int, MessageQueue*>;
	QueueMap nonempty_queues;

	// Queue from which we should next select.
	// May be nonempty_queues::end() if there's nothing to select.
	QueueMap::iterator next_queue;
	// int next_queue_id;
};


/* Choose message queue that has spent the least cumulative time,
	 to balance time spent on each job/user.
	 Nonempty queues are stored in a heap. */
class FairQueueTime : public FairQueue {
 public:
  FairQueueTime(FairnessMode mode,
                bool weight_by_node_count,
                int mpi_rank, int thread_id,
                JobInfoLookup &job_info_lookup,
                int max_idle_sec = 10)
		: FairQueue(mode, weight_by_node_count, mpi_rank, thread_id, job_info_lookup, max_idle_sec)
		{}

  virtual void addNonemptyQueue(int key, MessageQueue *q) {}
	virtual void removeNonemptyQueue(MessageQueue *q) {}
	virtual bool getMessage(IO_CMD_MSG *msg) {return false;}
	virtual void chargePreviousMessageCost(double value) {}
};


FairQueue* createFairQueue
(FairnessOrder order,
 FairnessMode mode,
 bool weight_by_node_count,
 int mpi_rank, int thread_id,
 JobInfoLookup &job_info_lookup,
 int max_idle_sec = 10);
