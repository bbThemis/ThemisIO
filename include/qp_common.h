#ifndef __COMMON_PARAM
#define __COMMON_PARAM

#define IB_DEVICE	"ib0"
#define FS_PARAM_FILE	"myfs.param"

//#define MAX_FS_SERVER	(2)

#define MAX_INLINE_SIZE	(220)

//#define QP_PUT_TIMEOUT_MS	(1000)	// one second timeout
#define QP_PUT_TIMEOUT_MS	(3000)	// two seconds timeout
#define QP_WAIT_RESULT_TIMEOUT_MS	(3500)	// one second timeout

#define MAX_FS_SERVER	(128)
#define MAX_QP_PER_PROCESS	(512)	// 256. Assume half of 512 is used. 
#define MAX_QP_PER_NODE		(256)	// hard limit per node. Maybe not large enough. QP is needed for every file server!!!

#define T_FREQ_CHECK_ALARM_HB	(1)		// once every second to check whether we need to send out heart beat
//#define T_FREQ_ALARM_HB			(1500)	// update every 15 seconds
#define T_FREQ_ALARM_HB			(1500000)	// update every 15 seconds

#define TAG_SUBMIT_JOB_INFO	(0x78000000)
#define TAG_EXCH_QP_INFO	(0x78780000)
#define TAG_EXCH_MEM_INFO	(0x78780001)
#define TAG_GLOBAL_ADDR_INFO	(0x78780002)
#define TAG_DONE			(0x78787878)

#define MAX_HOSTNAME_LEN	(24)	// short name. 
#define MAX_EXENAME_LEN		(16)	// short name. 

typedef	struct	{
	uint32_t comm_tag;
	uint32_t nnode;
	uint32_t jobid;
	uint32_t cip;	// client ip
	uint32_t ctid;	// client thred id
	uint32_t cuid;	// client user id
	uint32_t cgid;	// client user group id
    float rate; // Rule_Lustre paper param
	char szClientHostName[MAX_HOSTNAME_LEN];
	char szClientExeName[MAX_EXENAME_LEN];
}JOB_INFO_DATA;

// Need jobid, cip and ctid to calculate a hash that is used to determine the index of queue. 

typedef	struct	{
	uint32_t comm_tag;
	uint32_t key;
	unsigned long int addr;
}IB_MEM_DATA, *PIB_MEM_DATA;

typedef	struct	{
	uint32_t comm_tag;
	uint32_t lid;
	uint32_t qp_n;
	uint32_t psn;
}QPAIR_EXCH_DATA;

typedef	struct	{
	uint32_t comm_tag;
	uint32_t pad;
	uint64_t addr_NewMsgFlag;
	uint64_t addr_TimeHeartBeat;
	uint64_t addr_IO_Cmd_Msg;
}GLOBAL_ADDR_DATA;

typedef	struct{
	JOB_INFO_DATA	JobInfo;
	QPAIR_EXCH_DATA	qp;
	IB_MEM_DATA		ib_mem;
}DATA_SEND_BY_CLIENT;

typedef	struct{
	QPAIR_EXCH_DATA	qp;
	IB_MEM_DATA		ib_mem;
	GLOBAL_ADDR_DATA global_addr;
}DATA_SEND_BY_SERVER;

#endif

