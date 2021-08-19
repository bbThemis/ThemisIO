#include "fair_queue.h"
#include <sys/time.h>
#include <random>


FairQueue::FairQueue(FairnessMode mode_, int mpi_rank_, int thread_id_,
										 JobInfoLookup &job_info_lookup_, int max_idle_sec_) :
	fairness_mode(mode_),
	mpi_rank(mpi_rank_),
	thread_id(thread_id_),
	job_info_lookup(job_info_lookup_),
	max_idle_sec(max_idle_sec_),
	message_count(0),
	next_purge_timestamp(0)
{

	std::random_device rdev;
	random_engine.seed(rdev());
	start_time_usec = getTime();
	
	if (log_choices) {
		char fairness_log_name[100];
		snprintf(fairness_log_name, 100, "fair_queue.rank%d.thread%d.log", mpi_rank, thread_id);
		choice_log = fopen(fairness_log_name, "w");
	} else {
		choice_log = nullptr;
	}
}


FairQueue::~FairQueue() {
	if (choice_log) fclose(choice_log);
	for (auto it = indexed_queues.begin();
			 it != indexed_queues.end();
			 it++) {
		delete it->second;
	}
}


void FairQueue::putMessage(const IO_CMD_MSG *msg) {
	int key = getKey(msg);
	MessageQueue *q;
	bool was_empty;

	message_count++;
	auto iqt = indexed_queues.find(key);
	if (iqt == indexed_queues.end()) {
		// create new message queue
		int weight = fairness_mode == SIZE_FAIR ? job_info_lookup.getNodeCount(msg) : 1;
		q = new MessageQueue(key, getTime(), weight);
		was_empty = true;
	} else {
		q = iqt->second;
		was_empty = q->messages.empty();
	}

	q->add(msg);

	if (was_empty) {
		nonempty_queues.push_back(q);
	}

	if (msg->T_Queued > next_purge_timestamp) {
		purgeIdle();
		next_purge_timestamp = msg->T_Queued + max_idle_sec * (long)1000000;
	}
}


bool FairQueue::isEmpty() {
	return nonempty_queues.empty();
}


int FairQueue::getCount() {
	return message_count;
}


int FairQueue::chooseRandomNonemptyQueue() {
	long unsigned now = getTime();
	int n = nonempty_queues.size();

	double priority_sum = 0;
	nonempty_priorities.resize(n);
	for (int i=0; i < n; i++) {
		nonempty_priorities[i] = nonempty_queues[i]->getPriority(now);
		assert(nonempty_priorities[i] > 0);
		priority_sum += nonempty_priorities[i];
	}

	std::uniform_real_distribution<double> distrib(0, priority_sum);
	double r = distrib(random_engine), r_orig = r;

	// if nothing is selected (possibly due to roundoff errors), select the last one
	int choice_idx = n-1;

	for (int i=0; i < n-1; i++) {
		if (nonempty_priorities[i] > r) {
			choice_idx = i;
			break;
		} else {
			r -= nonempty_priorities[i];
		}
	}

	if (log_choices) {
		double timestamp = (now - start_time_usec) / 1000000.;
		fprintf(choice_log, "choice %.6f, %d queue%s probsum=%lf r=%lf choice=%d\n",
						timestamp, n, n==1 ? "" : "s", priority_sum, r_orig, choice_idx);
		for (int i=0; i < n; i++) {
			MessageQueue *q = nonempty_queues[i];
			fprintf(choice_log, "  %d. id=%d age=%lu priority=%lf\n", i, q->id, now - q->front_timestamp,
							nonempty_priorities[i]);
		}
	}

	return choice_idx;
}


bool FairQueue::getMessage(IO_CMD_MSG *msg) {
	if (isEmpty()) return false;

	int queue_idx = chooseRandomNonemptyQueue();
	
	MessageQueue *q = nonempty_queues[queue_idx];
	q->remove(msg);
	message_count--;
	
	if (q->messages.empty()) {
		// mark this queue idle and move it off the nonempty list
		q->idle_timestamp = getTime();

		size_t last_idx = nonempty_queues.size() - 1;
		nonempty_queues[queue_idx] = nonempty_queues[last_idx];
		nonempty_queues.resize(last_idx);
	}

	return true;
}
 

long unsigned FairQueue::getTime() {
	struct timeval t;
	gettimeofday(&t, 0);
	return t.tv_sec * 1000000 + t.tv_usec;
}


void FairQueue::purgeIdle() {
	// printf("%.6f purgeIdle thread %d\n", (getTime() - start_time_usec)/1000000., thread_id);
	long unsigned too_old = getTime() - max_idle_sec * 1000000;

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

	if (log_choices) fflush(choice_log);
}
