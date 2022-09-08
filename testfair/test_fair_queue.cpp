/*
	This is a framework for testing fair_queue outside the Themis server
	to make it easy to develop on a desktop machine.
*/

#include <assert.h>
#include <unistd.h>


// one job, which may be an MPI process running on many nodes
struct JOBREC {
  int jobid, nnode, uid;

  JOBREC() {}
  JOBREC(int jobid_, int nnode_, int uid_)
    : jobid(jobid_), nnode(nnode_), uid(uid_) {}
};


// one command from a client
struct IO_CMD_MSG {
  int idx_JobRec;  // index into array of JOBREC objects

  IO_CMD_MSG() {}
  IO_CMD_MSG(int idx) : idx_JobRec(idx) {}
};


#define MAX_JOB_COUNT 1000

enum FairnessMode {
  // traditional first in first out
  FIFO, 

  // Priority based on jobs such that each job gets equal throughput.
  JOB_FAIR,

  // Priority based on users such that each user gets equal throughput.
  USER_FAIR
};


// Keep this in sync with ServerOptions::queueOrderToString()
enum FairnessOrder {
  FAIR_ORDER_RANDOM,  // message queue chosen randomly
  FAIR_ORDER_CYCLE,   // queue chosen in round-robin order
  FAIR_ORDER_TIME     // queue chosen to balance cumulative time
};

class ServerOptions {
public:
  static const char *queueOrderToString(FairnessOrder order) {
    switch (order) {
    case FAIR_ORDER_RANDOM: return "FAIR_ORDER_RANDOM";
    case FAIR_ORDER_CYCLE: return "FAIR_ORDER_CYCLE";
    case FAIR_ORDER_TIME: return "FAIR_ORDER_TIME";
    default: return "<unknown>";
    }
  }
};

#define FAIR_QUEUE_STANDALONE 1
#include "../src/fair_queue.cpp"


using KeyCounter = std::map<int,int>;


/* Given a map of id->count, increment the count for this id. */
void counterInc(KeyCounter &counter, int id) {
	auto it = counter.find(id);
	if (it == counter.end()) {
		counter[id] = 1;
	} else {
		it->second++;
	}
}

/* Given a map if->count, list all ids and counts. */
void counterList(const KeyCounter &counter) {
	for (auto &e : counter) {
		printf("  %d: %d\n", e.first, e.second);
	}
}


int main() {
	FairnessOrder order;
	FairnessMode mode;

	order = FairnessOrder::FAIR_ORDER_RANDOM;
	// order = FairnessOrder::FAIR_ORDER_CYCLE;
	// order = FairnessOrder::FAIR_ORDER_TIME;

	// mode = FairnessMode::FIFO
	mode = FairnessMode::USER_FAIR;
	// mode = FairnessMode::JOB_FAIR;
	
  bool weight_by_node_count = true;
  int mpi_rank = 0, thread_id = 100;
  JOBREC job_list[MAX_JOB_COUNT];
  int job_list_size = 0;  // !! update this when job_list is updated
  JobInfoLookup job_info_lookup(job_list, &job_list_size);
  
  FairQueue *q = createFairQueue
    (order, mode, weight_by_node_count,
     mpi_rank, thread_id, job_info_lookup);

	KeyCounter job_id_counter, user_id_counter;

  job_list[job_list_size++] = JOBREC(101, 1, 1001);
  job_list[job_list_size++] = JOBREC(102, 1, 1002);

  // add 10 messages to each job queue
  for (int i=0; i < 10; ++i) {
		IO_CMD_MSG msg_101(0);
		q->putMessage(&msg_101);
	}
	
  for (int i=0; i < 10; ++i) {
		IO_CMD_MSG msg_102(1);
		q->putMessage(&msg_102);
	}

	// pull all messages from the queue
	IO_CMD_MSG msg;
	while (q->getMessage(&msg)) {
		int job_id = job_info_lookup.getSlurmJobId(&msg);
		int user_id = job_info_lookup.getUserId(&msg);
		printf("msg for job %d (%d)\n", job_id, msg.idx_JobRec);
		counterInc(job_id_counter, job_id);
		counterInc(user_id_counter, user_id);
	}

	printf("By job id:\n");
	counterList(job_id_counter);

	printf("By user id:\n");
	counterList(user_id_counter);
  
	delete q;
  
	return 0;
}

     
     
