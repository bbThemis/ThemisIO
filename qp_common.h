#ifndef __COMMON_PARAM
#define __COMMON_PARAM

#define IB_DEVICE	"ib0"
#define FS_PARAM_FILE	"myfs.param"

#define MAX_FS_SERVER	(128)
#define MAX_QP_PER_PROCESS	(512)	// 256. Assume half of 512 is used. 
#define MAX_QP_PER_NODE		(256)	// hard limit per node. Maybe not large enough. QP is needed for every file server!!!

#define T_FREQ_CHECK_ALARM_HB	(1)		// once every second to check whether we need to send out heart beat
#define T_FREQ_ALARM_HB			(15)	// update every 15 seconds

#define TAG_SUBMIT_JOB_INFO	(0x78000000)
#define TAG_EXCH_QP_INFO	(0x78780000)
#define TAG_EXCH_MEM_INFO	(0x78780001)
#define TAG_GLOBAL_ADDR_INFO	(0x78780002)
#define TAG_DONE			(0x78787878)

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

#endif

