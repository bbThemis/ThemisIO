#include "fair_queue.h"
#include <sys/time.h>
#include <random>
#include <iomanip>

/* Set this to a nonzero value to enable a debug output message each time a
	 new job is encountered, and a message when that job is considered idle
	 and is purged. */
#define REPORT_JOB_START_AND_END 0


pthread_mutex_t FairQueue::shared_decision_log_lock = PTHREAD_MUTEX_INITIALIZER;
FairQueue::DecisionLog FairQueue::shared_decision_log;
int FairQueue::thread_count = 0;
int FairQueue::n_threads_reported = 0;


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
	
	decision_log.changeThreadCount(1);
}


FairQueue::~FairQueue() {
	decision_log.changeThreadCount(-1);
	for (auto it = user_to_job_queues.begin(); it != user_to_job_queues.end(); it++) {
        
        for(auto itt = it->second.begin(); itt != it->second.end(); itt++){
            delete itt->second;
        }
	}
}


void FairQueue::putMessage(const IO_CMD_MSG *msg) {
	int key = getKey(msg);

    int user_key = job_info_lookup.getUserId(msg);
    int job_key = job_info_lookup.getSlurmJobId(msg);

	MessageQueue *q;
	bool was_empty;
	static bool first_output = true;

	message_count++;
	auto iqt = user_to_job_queues.find(user_key);
	if (iqt == user_to_job_queues.end()) {
		// create new message queue
        bool size_fair = fairness_mode == SIZE_FAIR || fairness_mode == USER_SIZE_FAIR;
		int weight = size_fair ? job_info_lookup.getNodeCount(msg) : 1;

		q = new MessageQueue(job_key, job_key, user_key, getTime(), weight);
        
		user_to_job_queues[user_key][job_key] = q;
		was_empty = true;

#if REPORT_JOB_START_AND_END
		printf("FairQueue::putMessage.newqueue time=%.2f threadid=%d key=%d jobid=%d userid=%d nodecount=%d weight=%d\n", 
					 getTime()/1000000., thread_id, key, job_id, user_id, job_info_lookup.getNodeCount(msg), weight);
#endif

	} else {
		auto job_map_for_user = iqt->second;
        auto it = job_map_for_user.find(job_key);

        bool size_fair = fairness_mode == SIZE_FAIR || fairness_mode == USER_SIZE_FAIR;
		int weight = size_fair ? job_info_lookup.getNodeCount(msg) : 1;
        
        if(it == job_map_for_user.end()){
            q = new MessageQueue(job_key, job_key, user_key, getTime(), weight);  
            job_map_for_user[job_key] = q;  
        }
        else{
            q = job_map_for_user[job_key];
        }
		was_empty = q->messages.empty();
	}

	q->add(msg);

	if (was_empty) {
		//nonempty_queues.push_back(q);
        user_to_nonempty_queues[user_key].push_back(q);
	}
}


bool FairQueue::isEmpty() {
    bool ans = true;
    for(auto it : user_to_nonempty_queues){
        ans = ans && it.second.empty();
    }
	return ans;
}


int FairQueue::getCount() {
	return message_count;
}

int FairQueue::chooseRandomNonemptyQueuePerUser(std::vector<MessageQueue*>& nonempty_queues){
    long unsigned now = getTime();
	int n = nonempty_queues.size();

	double priority_sum = 0;
    nonempty_priorities.clear();
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

	decision_log.log(nonempty_queues, nonempty_queues[choice_idx]->id);

	return choice_idx;
}

std::pair<int, int> FairQueue::chooseRandomNonemptyQueue() {
    int num_nonempty_users = user_to_nonempty_queues.size();
    auto it = user_to_nonempty_queues.begin();
    int random_index = rand() % num_nonempty_users;
    std::advance(it, random_index);

    int user_key = it->first;
    int q_idx = chooseRandomNonemptyQueuePerUser(it->second);

    return std::make_pair(user_key, q_idx);

}


bool FairQueue::getMessage(IO_CMD_MSG *msg) {
	if (isEmpty()) return false;
	
	std::pair<int, int> user_key_and_q_idx = chooseRandomNonemptyQueue();

    int user_key = user_key_and_q_idx.first;
    int q_idx = user_key_and_q_idx.second;  

    std::vector<MessageQueue*> nonempty_queues = user_to_nonempty_queues[user_key];

    MessageQueue * q = nonempty_queues[q_idx];
    

	q->remove(msg);
	message_count--;
	
	if (q->messages.empty()) {
		// mark this queue idle and move it off the nonempty list
		q->idle_timestamp = getTime();

		size_t last_idx = nonempty_queues.size() - 1;
		nonempty_queues[q_idx] = nonempty_queues[last_idx];
		nonempty_queues.resize(last_idx);
        if(nonempty_queues.empty()){
            user_to_nonempty_queues.erase(user_key);
        }
	}

	return true;
}


void FairQueue::housekeeping() {

	if (decision_log.enabled) reportDecisionLog();

	purgeIdle();
}
 

// Returns the current time in microseconds since epoch.
long unsigned FairQueue::getTime() {
	struct timeval t;
	gettimeofday(&t, 0);
	return t.tv_sec * 1000000 + t.tv_usec;
}


// Returns the number of seconds since the object was created.
double FairQueue::getElapsed() {
	return (getTime() - start_time_usec) * .000001;
}



void FairQueue::reportDecisionLog() {

	// single-thread report
	/*
		std::ostringstream buf;
		buf << "FairQueue report thread_id=" << thread_id << " time=" << 
		std::fixed << std::setprecision(2) << getElapsed() << " " << decision_log.toString() << '\n';
		std::string report = buf.str();
		write(STDERR_FILENO, report.data(), report.length());
	*/

	// combine the data from this thread with the shared data
	std::string shared_report;

	pthread_mutex_lock(&shared_decision_log_lock);

	shared_decision_log.addLog(decision_log);

	// after a cycle of everyone checking in, produced a shared report
	if (++n_threads_reported == thread_count) {
		shared_report = shared_decision_log.toString();
		shared_decision_log.clear();
		n_threads_reported = 0;
	}
			
	pthread_mutex_unlock(&shared_decision_log_lock);

	// my data has been moved to the shared log; clear this copy
	decision_log.clear();

	// to minimize time holding the lock, the report was generated and saved into a string,
	// then after the lock was released we'll print the report
	if (shared_report.length()) {
		std::ostringstream buf;
		buf << "FairQueue::reportDecisionLog rank " << mpi_rank << " time=" << 
			std::fixed << std::setprecision(2) << getElapsed() << " " << shared_report << '\n';
		std::string report = buf.str();
		write(STDERR_FILENO, report.data(), report.length());
	}

}


// Scans all message queues and removes those which have been idle for too long.
void FairQueue::purgeIdle() {
	long now = getTime();

	// printf("%.6f purgeIdle thread %d\n", getElapsed(), thread_id);
	long unsigned too_old = now - max_idle_sec * 1000000;

    for(auto & itt : user_to_job_queues){
        auto user_queue = itt.second;
        auto it = user_queue.begin();
        while (it != user_queue.end()) {
            MessageQueue *q = it->second;
            /* printf("FairQueue::purgeIdle thread_id=%d job %d idle time %.6f, empty=%s\n",
                        thread_id, q->job_id, (now - q->idle_timestamp) / 1000000.,
                        q->messages.empty() ? "true" : "false"); */
            if (q->messages.empty() && q->idle_timestamp < too_old) {
    #if REPORT_JOB_START_AND_END
                printf("FairQueue::purgeIdle time=%.2f thread_id=%d purge job %d, idle for %.2f sec\n",
                            now/1000000., thread_id, q->job_id, (now - q->idle_timestamp) / 1000000.);
    #endif
                delete q;
                it = user_queue.erase(it);
            } else {
                it++;
            }
        }
    }

}


void FairQueue::DecisionLog::log(const std::vector<MessageQueue*> &nonempty_queues, int choice_id) {

	if (!enabled) return;
             
	// make a copy of the all the ids of the queues from which the choice
	// was made.  Use persistent temp_storage object to minimize reallocations.
	int n = nonempty_queues.size();
	temp_storage.resize(n + 1);
	int *temp = temp_storage.data();
	temp[0] = n;
    
	for (int i=0; i < n; i++) {
		int id = nonempty_queues[i]->id;
		temp[i+1] = id;
	}

	// order the ids so alternate orderings are merged
	std::sort(temp+1, temp+1+n);

	// find where choice_id ended up in the sorted array
	int *choice_ptr = std::lower_bound(temp+1, temp+1+n, choice_id);
	// check that I did the binary search correctly
	assert(choice_ptr >= temp+1 && choice_ptr < temp+1+n);
	// check that choice_id matches one of the inputs
	assert(*choice_ptr == choice_id);
    
	int choice_idx = choice_ptr - (temp+1);

	// see if this set already exists
	int *decision;
	auto it = decisions.find(temp);
	if (it == decisions.end()) {
		// first time; create the set with all counters set to zero
		decision = new int[n * 2 + 1];
		memcpy(decision, temp, sizeof(int) * (n + 1));
		memset(decision + n + 1, 0, sizeof(int) * n);
		// add it to the decision set
		decisions.insert(decision);
	} else {
		decision = *it;
	}

	// offset of frequency counter for choice_idx
	int freq_idx = n + 1 + choice_idx;

	// increment frequency counter
	decision[freq_idx] += 1;
}


// combine the counts from that to this
void FairQueue::DecisionLog::addLog(const FairQueue::DecisionLog &that) {
	for (auto that_iter = that.decisions.begin();
			 that_iter != that.decisions.end();
			 that_iter++) {

		// see if we have an entry with the same key
		int *that_dec = *that_iter;
		int n = that_dec[0];
		auto this_iter = decisions.find(that_dec);

		if (this_iter == decisions.end()) {
			// this is new; make a copy of it
			int len = n * 2 + 1;
			int *dec_copy = new int[len];
			memcpy(dec_copy, that_dec, len * sizeof(int));
			decisions.insert(dec_copy);
		} 

		else {
			// this key exists. Verify that the key matches, just in case
			int *this_dec = *this_iter;
			assert(0 == memcmp(this_dec, that_dec, sizeof(int) * (n + 1)));
			
			// add counters from that to this
			for (int i=n+1; i <= n*2; i++) {
				this_dec[i] += that_dec[i];
			}
		}
	}
}


// Represent all the decision sets in an easy-to-parse way
std::string FairQueue::DecisionLog::toString() {
	std::ostringstream buf;
	auto it = decisions.begin();
	for (it = decisions.begin();
			 it != decisions.end();
			 it++) {
		if (it != decisions.begin())
			buf << ';';
		int *dset = *it;
		buf << decisionSetToString(dset);
	}
	return buf.str();
}


void FairQueue::DecisionLog::clear() {
	auto it = decisions.begin();
	while (it != decisions.end()) {
		delete [] *it;
		it = decisions.erase(it);
	}
}


// represent one decision set as an easy-to-parse string
std::string FairQueue::DecisionLog::decisionSetToString(int *data) {
	std::ostringstream buf;
	int n = data[0];
	for (int i=0; i < n; i++) {
		if (i > 0) buf << ',';
		buf << data[i+1];
	}
	buf << ':';
	for (int i=0; i < n; i++) {
		if (i > 0) buf << ',';
		buf << data[n+i+1];
	}
	return buf.str();
}
