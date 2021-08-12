#include "fair_queue.h"
#include <sys/time.h>


FairQueue::FairQueue(Mode mode_, JobInfoLookup &job_info_lookup_, int max_idle_sec_) :
	fairness_mode(mode_),
	job_info_lookup(job_info_lookup_),
	max_idle_sec(max_idle_sec_),
	total_nonempty_weight(0),
	next_purge_timestamp(0)
{

	std::random_device rdev;
	random_engine.seed(rdev());
	
}


FairQueue::~FairQueue() {
	auto it = indexed_queues.begin();
	while (it != indexed_queues.end()) {
		delete it->second;
		it = indexed_queues.erase(it);
	}
}


void FairQueue::putMessage(const IO_CMD_MSG *msg) {
	int key = getKey(msg);
	MessageQueue *q;
	bool was_empty;

	auto iqt = indexed_queues.find(key);
	if (iqt == indexed_queues.end()) {
		// create new message queue
		int weight = fairness_mode == Mode::SIZE_FAIR ? job_info_lookup.getNodeCount(msg) : 1;
		q = new MessageQueue(now(), weight);
		was_empty = true;
	} else {
		q = iqt->second;
		was_empty = q->messages.empty();
	}

	MessageContainer *mc = (MessageContainer*) msg;
	q->messages.push(*mc);

	if (was_empty) {
		nonempty_queues.push_back(q);
		nonempty_weights.push_back(q->weight);
		total_nonempty_weight += q->weight;
	}

	if (msg->T_Queued > next_purge_timestamp) {
		purgeIdle();
		next_purge_timestamp = msg->T_Queued + max_idle_sec * (long)1000000;
	}
}


bool FairQueue::isEmpty() {
	return nonempty_queues.empty();
}


int FairQueue::chooseRandomNonemptyQueue() {
	assert(!nonempty_weights.empty() && total_nonempty_weight > 0);
	
	if (fairness_mode == Mode::SIZE_FAIR) {
		
		// weighted random choice
		std::uniform_int_distribution<long> distrib(0, total_nonempty_weight-1);
		long r = distrib(random_engine);
		for (size_t i = 0; i < nonempty_weights.size(); i++) {
			if (r < nonempty_weights[i]) {
				return i;
			} else {
				r -= nonempty_weights[i];
			}
		}
		return nonempty_weights.size() - 1;
		
	} else {
		
		// nonweighted random choice
		std::uniform_int_distribution<int> distrib(0, nonempty_queues.size()-1);
		return distrib(random_engine);

	}
}


bool FairQueue::getMessage(IO_CMD_MSG *msg) {
	if (isEmpty()) return false;

	int queue_idx = chooseRandomNonemptyQueue();
	
	MessageQueue *q = nonempty_queues[queue_idx];
	MessageContainer &msg_c = q->messages.front();
	memcpy(msg, &msg_c.msg, sizeof *msg);
	q->messages.pop();

	if (q->messages.empty()) {
		// mark this queue idle and move it off the nonempty list
		q->idle_timestamp = now();
		total_nonempty_weight -= q->weight;
		size_t last_idx = nonempty_queues.size() - 1;
		nonempty_queues[queue_idx] = nonempty_queues[last_idx];
		nonempty_queues.resize(last_idx);
		nonempty_weights[queue_idx] = nonempty_weights[last_idx];
		nonempty_weights.resize(last_idx);
	}

	return true;
}
 

long unsigned FairQueue::now() {
	struct timeval t;
	gettimeofday(&t, 0);
	return t.tv_sec * 1000000 + t.tv_usec;
}


void FairQueue::purgeIdle() {
	long unsigned too_old = now() - max_idle_sec * 1000000;

	auto it = indexed_queues.begin();
	while (it != indexed_queues.end()) {
		MessageQueue *q = it->second;
		if (q->messages.empty() && q->idle_timestamp < too_old) {
			delete q;
			it = indexed_queues.erase(it);
		} else {
			it++;
		}
	}
}
