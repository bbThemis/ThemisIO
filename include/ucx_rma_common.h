#ifndef __UCX_RMA_COMMON
#define __UCX_RMA_COMMON
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <uct/api/uct.h>

#define MAX_HOSTNAME_LEN	(24)	// short name. 
#define MAX_EXENAME_LEN		(16)	// short name. 
#define UCX_PUT_TIMEOUT_MS	(3000)	// two seconds timeout
void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status);
// typedef	struct	{
// 	uint32_t comm_tag;
// 	uint32_t nnode;
// 	uint32_t jobid;
// 	uint32_t cip;	// client ip
// 	uint32_t ctid;	// client thred id
// 	uint32_t cuid;	// client user id
// 	uint32_t cgid;	// client user group id
// 	char szClientHostName[MAX_HOSTNAME_LEN];
// 	char szClientExeName[MAX_EXENAME_LEN];
// }JOB_INFO_DATA;


#endif

