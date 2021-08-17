#include <cassert>
// #include <cstdlib>
#include <cstring>
#include <pthread.h>
#include <queue>
#include <unordered_map>
#include <random>
#include "qp.h"
#include "io_queue.h"

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
	enum class Mode {SIZE_FAIR, JOB_FAIR, USER_FAIR};

	/* mode: fairness mode
		 job_info_lookup: a wrapper used to access the active job list
		 max_idle_sec: deallocate queues that have been empty for at least this many seconds.
	*/
	FairQueue(Mode mode, int mpi_rank, int thread_id,
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
		MessageQueue(int id_, long now, int weight_)
			: id(id_), idle_timestamp(now), weight(weight_) {}
								 
		std::queue<MessageContainer> messages;

		// id, either job id or user id, depending on fairness mode
		int id; 

		// timestamp this queue was most recently nonempty
		long unsigned idle_timestamp;
		
		// timestamp of the first item in the queue
		long unsigned front_timestamp;

		const int weight;	 // size-fair: node count. Otherwise 1.

		void add(const IO_CMD_MSG *msg) {
			if (messages.empty())
				front_timestamp = msg->T_Queued;
			
			MessageContainer *mc = (MessageContainer*) msg;
			messages.push(*mc);
		}

		bool remove(IO_CMD_MSG *msg) {
			if (messages.empty()) return false;
			MessageContainer &msg_c = messages.front();
			memcpy(msg, &msg_c.msg, sizeof *msg);
			messages.pop();

			if (messages.empty())
				front_timestamp = 0;

			return true;
		}

		// compute a priority for this queue, which is the age of the message on
		// the front of the queue times the weight of the job.
		double getPriority(long unsigned now) {
			long usec_waiting = now - front_timestamp;
			if (usec_waiting <= 0)
				usec_waiting = 1;
			return usec_waiting * weight;
		}
		
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
	static long unsigned getTime();
	
	// scan all message queues and remove those which have been idle for too long
	void purgeIdle();

	Mode fairness_mode;
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

	// all nonempty message queues
	std::vector<MessageQueue*> nonempty_queues;

	// Used in chooseRandomNonemptyQueue(). This is filled with the
	// priority for each nonempty queue, then one is chosen randomly.
	std::vector<double> nonempty_priorities;

	// Call purgeIdle() when a message comes in with a timestamp after this.
	long unsigned next_purge_timestamp;

	const bool log_choices = false;
	FILE *choice_log;
};
