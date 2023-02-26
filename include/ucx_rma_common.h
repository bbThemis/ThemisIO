#ifndef __UCX_RMA_COMMON
#define __UCX_RMA_COMMON
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <uct/api/uct.h>

#define MAX_UCP_RKEY_SIZE (50)
#define MAX_UCP_ADDR_LEN (250)

#define T_FREQ_ALARM_HB			(1500000)	// update every 15 seconds

#define TAG_SUBMIT_JOB_INFO	(0x78000000)
#define TAG_EXCH_UCX_INFO	(0x78780000)
#define TAG_EXCH_MEM_INFO	(0x78780001)
#define TAG_GLOBAL_ADDR_INFO	(0x78780002)
#define TAG_DONE			(0x78787878)

#define UCX_MAX_HOSTNAME_LEN	(24)	// short name. 
#define UCX_MAX_EXENAME_LEN		(16)	// short name. 
#define UCX_PUT_TIMEOUT_MS	(3000)	// two seconds timeout
#define UCX_WAIT_RESULT_TIMEOUT_MS	(3500)	// one second timeout

#define MAX_FS_UCX_SERVER (128)
#define MAX_UCX_PER_PROCESS	(512)	// 256. Assume half of 512 is used. 

#define UCX_QUEUE_SIZE	(1)

#define UCX_FS_PARAM_FILE "ucx_myfs.param"
void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status);
typedef	struct	{
	uint32_t comm_tag;
	uint32_t nnode;
	uint32_t jobid;
	uint32_t cip;	// client ip
	uint32_t ctid;	// client thred id
	uint32_t cuid;	// client user id
	uint32_t cgid;	// client user group id
	char szClientHostName[UCX_MAX_HOSTNAME_LEN];
	char szClientExeName[UCX_MAX_EXENAME_LEN];
}UCX_JOB_INFO_DATA;

typedef	struct	{
    uint32_t comm_tag;
	char rkey_buffer[MAX_UCP_RKEY_SIZE];
    size_t rkey_buffer_size = 0;
    unsigned long int addr;

	ucp_rkey_h rkey; // only for pal_remote_mem on clients
}UCX_IB_MEM_DATA, *PUCX_IB_MEM_DATA;

typedef	struct	{
    uint32_t comm_tag;
	char peer_address[MAX_UCP_ADDR_LEN];
	size_t peer_address_length = 0;
}UCX_EXCH_DATA;


typedef	struct	{
	uint32_t comm_tag;
	uint64_t addr_NewMsgFlag;
	uint64_t addr_TimeHeartBeat;
	uint64_t addr_IO_Cmd_Msg;
}UCX_GLOBAL_ADDR_DATA;

typedef	struct{
	UCX_JOB_INFO_DATA	JobInfo;
	UCX_EXCH_DATA	ucx;
	UCX_IB_MEM_DATA		ib_mem;
}UCX_DATA_SEND_BY_CLIENT;

typedef	struct{
	UCX_EXCH_DATA	ucx;
	UCX_IB_MEM_DATA		ib_mem;
	UCX_GLOBAL_ADDR_DATA global_addr;
}UCX_DATA_SEND_BY_SERVER;
#endif

