

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

inline int Align64_Int(int a)
{
	// return ( (a & 0x3F) ? (64 + (a & 0xFFFFFFC0) ) : (a) );

	// branch not needed
	return (a + 63) & ~63;
}
#define IB_DEVICE	"ib0"
#define DEFAULT_REM_BUFF_SIZE	(4096)

class ServerOptions {
public:
	ServerOptions() : fairness_mode(getDefaultFairnessMode()) {}

	// Returns true iff the command line arguments are successfully parsed.
	// defined in put_get_server.cpp
	bool parseCommandLineArgs(int argc, char **argv);

	// Prints help message explaining command line usage.
	// defined in put_get_server.cpp
	static void printHelp();

	// FairnessMode is defined in qp.h
	FairnessMode getFairnessMode() {return fairness_mode;}

	// use this to change the default fairness mode
	static FairnessMode getDefaultFairnessMode() {return FIFO;};

	static const char *fairnessModeToString(FairnessMode fairness_mode) {
		static char szFairnessModeString[16][64]={"fifo", "size-fair", "job-fair", "user-fair", "user-size-fair", "user-job-fair", "group-user-size-fair"};

		return szFairnessModeString[(int)fairness_mode];
//		return fairness_mode == SIZE_FAIR ? "size-fair"
//			: fairness_mode == JOB_FAIR ? "job-fair"
//			: "user-fair";
	}

private:
	FairnessMode fairness_mode;
};
#endif