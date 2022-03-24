#include "fair_queue.h"
#include <sys/time.h>
#include <random>
#include <iomanip>
#include <unordered_map>

/* Set this to a nonzero value to enable a debug output message each time a
	 new job is encountered, and a message when that job is considered idle
	 and is purged. */
#define REPORT_JOB_START_AND_END 0

// Defined in io_queue.cpp, this maps user id to group id, 
// and is currently unused.
extern std::unordered_map<int, int> uid_gid;

pthread_mutex_t FairQueue::shared_decision_log_lock = PTHREAD_MUTEX_INITIALIZER;
FairQueue::DecisionLog FairQueue::shared_decision_log;
int FairQueue::thread_count = 0;
int FairQueue::n_threads_reported = 0;


FairQueue* createFairQueue
(FairnessOrder order,
 FairnessMode mode,
 bool weight_by_node_count,
 int mpi_rank, int thread_id,
 JobInfoLookup &job_info_lookup,
 int max_idle_sec) {

	if (order == FAIR_ORDER_RANDOM) {
		return new FairQueueRandom(mode, weight_by_node_count, mpi_rank, thread_id,
															 job_info_lookup, max_idle_sec);
	} else {
		printf("order \"%s\" not supported yet\n", ServerOptions::queueOrderToString(order));
		return nullptr;
	}
}



FairQueue::FairQueue(FairnessMode mode_, 
										 bool weight_nodes_,
										 int mpi_rank_, int thread_id_,
										 JobInfoLookup &job_info_lookup_, int max_idle_sec_) :
	fairness_mode(mode_),
	weight_by_node_count(weight_nodes_),
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

	// unused
	/*
	weight_sum = 0;
	nJob = 0;
	IdxActiveJob = -1;
	it_ActiveJob = NULL;
	*/	

	decision_log.changeThreadCount(1);
}


FairQueue::~FairQueue() {
	decision_log.changeThreadCount(-1);
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
	static bool first_output = true;

	// printf("putMessage %p jobid=%d userid=%d\n", msg, job_info_lookup.getSlurmJobId(msg), job_info_lookup.getUserId(msg));

	message_count++;
	auto iqt = indexed_queues.find(key);
	if (iqt == indexed_queues.end()) {
		// create new message queue
		// XXX with user-fair, this will need to be updated when user adds and removes jobs
		int weight = weight_by_node_count ? job_info_lookup.getNodeCount(msg) : 1;
		int job_id = job_info_lookup.getSlurmJobId(msg);
		int user_id = job_info_lookup.getUserId(msg);
		q = new MessageQueue(key, job_id, user_id, getTime(), weight);
		indexed_queues[key] = q;
		was_empty = true;

#if REPORT_JOB_START_AND_END
		printf("FairQueue::putMessage.newqueue time=%.2f threadid=%d key=%d jobid=%d userid=%d nodecount=%d weight=%d\n", 
					 getTime()/1000000., thread_id, key, job_id, user_id, job_info_lookup.getNodeCount(msg), weight);
#endif

	} else {
		q = iqt->second;
		was_empty = q->messages.empty();
	}

	q->add(msg);

	if (was_empty)
		addNonemptyQueue(q);
}

// unused remnants of time-sharing version
#if 0
void FairQueue::SetFirstJobActive(void)
{
	IdxActiveJob = 0;
	
	MessageQueue *q = all_queues[IdxActiveJob];
	q->T_Cycle_Start = (long int)getTime();
}

void FairQueue::SetNextJobActive(void)
{
	if(indexed_queues.empty())	{
		IdxActiveJob = -1;
		return;
	}

	IdxActiveJob++;

	if(IdxActiveJob >= nJob)	{	// reaching the end. Rewind to the beginning
		IdxActiveJob = 0;
	}
//	MessageQueue *q = all_queues[IdxActiveJob];
//	if(q->T_Balance > 0)	q->T_Cycle_Start = (long int)getTime();
}
/*
void FairQueue::putMessage_TimeSharing(const IO_CMD_MSG *msg) {
	int key = getKey(msg);
	MessageQueue *q;
	static bool first_output = true;

	message_count++;
	auto iqt = indexed_queues.find(key);
	if (iqt == indexed_queues.end()) {
		// create new message queue
		int weight = fairness_mode == SIZE_FAIR ? job_info_lookup.getNodeCount(msg) : 1;
		int job_id = job_info_lookup.getSlurmJobId(msg);
		int user_id = job_info_lookup.getUserId(msg);
		q = new MessageQueue(key, job_id, user_id, getTime(), weight);
		indexed_queues[key] = q;
		all_queues.push_back(q);

		nJob++;
		// Start the first job, set it as the active job. 
		if(nJob == 1)	{
			SetFirstJobActive();
		}

		// Need to update the reload time length for all jobs
		weight_sum += weight;

		for(int i=0; i<nJob; i++)	{
			MessageQueue *q_loc = all_queues[i];
			q_loc->dT_Reload = TIME_PER_CYCLE_MICROSEC * (q_loc->weight) / weight_sum;
			q_loc->T_Balance = q_loc->dT_Reload;	// charge the time balance for a new job
		}

#if REPORT_JOB_START_AND_END
		printf("FairQueue::putMessage_TimeSharing.newqueue time=%.2f threadid=%d key=%d jobid=%d userid=%d nodecount=%d weight=%d\n", 
					 getTime()/1000000., thread_id, key, job_id, user_id, job_info_lookup.getNodeCount(msg), weight);
#endif

	} else {
		q = iqt->second;
	}

	q->add(msg);

	if( (msg->op & 0xFFFFFF00) != IO_OP_MAGIC)	{
		printf("Stop here. Wrong msg!!!\n");
	}

//	printf("DBG> Put jobid %d OP %x\n", q->job_id, msg->op & 0xFF);
}
*/

void FairQueue::Update_Job_Weight(void) {
	MessageQueue *q_loc;
	std::unordered_map<int, float> uid_count;
	int i;

	switch (fairness_mode)	{
		case SIZE_FAIR:
			weight_sum = 0.0;
			for(i=0; i<nJob; i++)	{
				q_loc = all_queues[i];
				weight_sum += q_loc->weight;
			}

			for(i=0; i<nJob; i++)	{
				q_loc = all_queues[i];
				q_loc->dT_Reload = TIME_PER_CYCLE_MICROSEC * (q_loc->weight) / weight_sum;
				q_loc->T_Balance = q_loc->dT_Reload;	// charge the time balance for a new job
			}
			break;
		case JOB_FAIR:	// each slurm job has weight 1.  
		case USER_FAIR:
			weight_sum = 1.0*nJob;
			for(i=0; i<nJob; i++)	{
				q_loc->weight = 1.0;
				q_loc = all_queues[i];
				q_loc->dT_Reload = TIME_PER_CYCLE_MICROSEC * (q_loc->weight) / weight_sum;
				q_loc->T_Balance = q_loc->dT_Reload;	// charge the time balance for a new job
			}
			break;
		case USER_SIZE_FAIR:
			for(i=0; i<nJob; i++)	{
				q_loc = all_queues[i];
				if (uid_count.find(q_loc->user_id) == uid_count.end())	{
					uid_count[q_loc->user_id] = q_loc->weight;	// number of jobs from current user
				}
				else	{
					uid_count[q_loc->user_id] = uid_count[q_loc->user_id] + q_loc->weight;
				}
			}

			weight_sum = 0.0;
			for(i=0; i<nJob; i++)	{
				q_loc = all_queues[i];
				weight_sum += (q_loc->weight / uid_count[q_loc->user_id]);
			}

			for(i=0; i<nJob; i++)	{
				q_loc = all_queues[i];
				q_loc->dT_Reload = TIME_PER_CYCLE_MICROSEC * (q_loc->weight) / weight_sum;
				q_loc->T_Balance = q_loc->dT_Reload;	// charge the time balance for a new job
			}
			break;
		case USER_JOB_FAIR:
			for(i=0; i<nJob; i++)	{
				q_loc = all_queues[i];
				q_loc->weight = 1.0;	// All jobs of one individual user have the same weight.  
				if (uid_count.find(q_loc->user_id) == uid_count.end())	{
					uid_count[q_loc->user_id] = q_loc->weight;	// number of jobs from current user
				}
				else	{
					uid_count[q_loc->user_id] = uid_count[q_loc->user_id] + q_loc->weight;
				}
			}

			weight_sum = 0.0;
			for(i=0; i<nJob; i++)	{
				q_loc = all_queues[i];
				weight_sum += (q_loc->weight / uid_count[q_loc->user_id]);
			}

			for(i=0; i<nJob; i++)	{
				q_loc = all_queues[i];
				q_loc->dT_Reload = TIME_PER_CYCLE_MICROSEC * (q_loc->weight) / weight_sum;
				q_loc->T_Balance = q_loc->dT_Reload;	// charge the time balance for a new job
			}
			break;
		case GROUP_USER_SIZE_FAIR:
			break;
		default:
			break;
	}
}

void FairQueue::putMessage_TimeSharing(const IO_CMD_MSG *msg) {
	MessageQueue *q;
	static bool first_output = true;
	int job_id = job_info_lookup.getSlurmJobId(msg);
	int user_id = job_info_lookup.getUserId(msg);
	std::unordered_map<int, MessageQueue*>::const_iterator result_query;

	message_count++;
	if(fairness_mode == USER_FAIR)	{
		result_query = indexed_queues.find(user_id);
	}
	else	{
		result_query = indexed_queues.find(job_id);
	}

	if (result_query == indexed_queues.end()) {
		// create new message queue
		int weight = job_info_lookup.getNodeCount(msg);
		q = new MessageQueue(job_id, job_id, user_id, getTime(), weight);
		if(fairness_mode != USER_FAIR)	indexed_queues[job_id] = q;
		else	indexed_queues[user_id] = q;
		all_queues.push_back(q);

		nJob++;
		// Start the first job, set it as the active job. 
		if(nJob == 1)	{
			SetFirstJobActive();
		}

		Update_Job_Weight();

		// Need to update the reload time length for all jobs

#if REPORT_JOB_START_AND_END
		printf("FairQueue::putMessage_TimeSharing.newqueue time=%.2f threadid=%d jobid=%d userid=%d nodecount=%d weight=%d\n", 
					 getTime()/1000000., thread_id, job_id, user_id, job_info_lookup.getNodeCount(msg), weight);
#endif

	} else {
		q = result_query->second;
	}

	q->add(msg);

	if( (msg->op & 0xFFFFFF00) != IO_OP_MAGIC)	{
		printf("Stop here. Wrong msg!!!\n");
	}

//	printf("DBG> Put jobid %d OP %x\n", q->job_id, msg->op & 0xFF);
}

void FairQueue::reload(void) {
	if(indexed_queues.empty())	{
		IdxActiveJob = -1;
		return;
	}

	IdxActiveJob = -1;

	for (int i=0; i<all_queues.size(); i++) {	// loop all jobs and reload
		MessageQueue *q = all_queues[i];
		q->T_Balance += q->dT_Reload;	// charge the time balance for a new job
		if( (q->T_Balance > 0) && (IdxActiveJob == (-1)) )	{	// Find the first job with positive balance
			IdxActiveJob = i;
			q->T_Cycle_Start = getTime();
			break;
		}
	}

}
#endif  // #if 0


// unused remnants of time-sharing version
#if 0
bool FairQueue::getMessage_FromActiveJob(IO_CMD_MSG *msg) {
	long int t_Now;

	if (nJob == 0) return false;

//	assert( (IdxActiveJob>=0) && (IdxActiveJob <nJob) );
	MessageQueue *q = all_queues[IdxActiveJob];

	t_Now = (long int)getTime();
	q->T_Balance -= (t_Now - q->T_Cycle_Start);

	if(q->T_Balance <= 0)	{
		int nDone = 1;

		while(1)	{
			// Make the next job active
			SetNextJobActive();
			q = all_queues[IdxActiveJob];

			if(q->T_Balance <= 0)	nDone++;
			else	{
				q->T_Cycle_Start = t_Now;
				break;
			}

			if(nDone >= nJob)	{	// All jobs are done. Need to restart a new cycle. 
				IdxActiveJob = -1;
				while( IdxActiveJob == (-1) )	{	// Might need to recharge multiple times
					reload();
				}
				break;
			}
		}
//		assert( (IdxActiveJob>=0) && (IdxActiveJob <nJob) );
		q = all_queues[IdxActiveJob];
	}
	if (q->messages.empty()) {
//		q->idle_timestamp = t_Now;
		return false;
	}

	q->remove(msg);
	if( (msg->op & 0xFFFFFF00) != IO_OP_MAGIC)	{
		printf("Stop here.\n");
	}
//	printf("DBG> %d %x\n", q->job_id, msg->op & 0xFF);

	message_count--;
	
	if (q->messages.empty()) {
		// mark this queue idle and move it off the nonempty list
		q->idle_timestamp = t_Now;
	}

	return true;
}
#endif  // #if 0


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

	auto it = indexed_queues.begin();
	while (it != indexed_queues.end()) {
		MessageQueue *q = it->second;
		// printf("FairQueue::purgeIdle thread_id=%d job %d idle time %.6f, empty=%s\n",
		//			 thread_id, q->job_id, (now - q->idle_timestamp) / 1000000.,
		//			 q->messages.empty() ? "true" : "false");
		if (q->messages.empty() && q->idle_timestamp < too_old) {
#if REPORT_JOB_START_AND_END
			printf("FairQueue::purgeIdle time=%.2f thread_id=%d purge job %d, idle for %.2f sec\n",
						 now/1000000., thread_id, q->job_id, (now - q->idle_timestamp) / 1000000.);
#endif
			delete q;
			it = indexed_queues.erase(it);
		} else {
			it++;
		}
	}

}


// unused remnants of time-sharing version
#if 0
// Scans all message queues and removes those which have been idle for too long.
void FairQueue::purgeIdle() {
	long now = getTime();

	// printf("%.6f purgeIdle thread %d\n", getElapsed(), thread_id);
	long unsigned too_old = now - max_idle_sec * 1000000;

	auto it = indexed_queues.begin();
	while (it != indexed_queues.end()) {
		MessageQueue *q = it->second;
		/* printf("FairQueue::purgeIdle thread_id=%d job %d idle time %.6f, empty=%s\n",
					 thread_id, q->job_id, (now - q->idle_timestamp) / 1000000.,
					 q->messages.empty() ? "true" : "false"); */
		if (q->messages.empty() && q->idle_timestamp < too_old) {
			weight_sum -= q->weight;

			for(int i=0; i<nJob; i++)	{
				if(q == all_queues[i])	{
					if(i != (nJob - 1) )	{	// not the last element, then move the last element to this spot
						all_queues[i] = all_queues[nJob - 1];
					}
					break;
				}
			}

			nJob--;

#if REPORT_JOB_START_AND_END
			printf("FairQueue::purgeIdle time=%.2f thread_id=%d purge job %d, idle for %.2f sec\n",
						 now/1000000., thread_id, q->job_id, (now - q->idle_timestamp) / 1000000.);
#endif
			if(nJob > 0)	{
				if(IdxActiveJob == nJob)	{	// reaching the end. Rewind to the beginning
					IdxActiveJob = 0;
				}
			}
			else if(nJob == 0)	{
				IdxActiveJob = -1;
			}

			delete q;
			it = indexed_queues.erase(it);
		} else {
			it++;
		}
	}


	if(weight_sum > 0)	{
		for(int i=0; i<nJob; i++)	{
			MessageQueue *q = all_queues[i];
			q->dT_Reload = TIME_PER_CYCLE_MICROSEC * (q->weight) / weight_sum;
		}
	}

}
#endif // #if 0


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


void FairQueueRandom::addNonemptyQueue(MessageQueue *q) {
	nonempty_queues.push_back(q);
}


bool FairQueueRandom::getMessage(IO_CMD_MSG *msg) {
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


int FairQueueRandom::chooseRandomNonemptyQueue() {
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

	decision_log.log(nonempty_queues, nonempty_queues[choice_idx]->id);

	return choice_idx;
}
