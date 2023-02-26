

#ifndef __UCX_QP_COMMON
#define __UCX_QP_COMMON
enum FairnessMode {
	// tradition first in first out
	FIFO, 
	// Priority based on jobs such that the throughput of each job is proportional
	// to the number of nodes in the job.
	SIZE_FAIR,

	// Priority based on jobs such that each job gets equal throughput.
	JOB_FAIR,

	// Priority based on users such that each user gets equal throughput.
	USER_FAIR, 

	// user-then-size-fair
	USER_SIZE_FAIR, 

	// user-then-job-fair
	USER_JOB_FAIR,

	// group-user-size
	GROUP_USER_SIZE_FAIR
};

#endif