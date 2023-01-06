#ifndef __IBVERBS_CLIENT
#define __IBVERBS_CLIENT

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

//export SLURM_JOBID="12345"
//export SLURM_NNODES=1

// SLURM_JOBID=2339541. 4 bytes
// IP. 4 bytes
// tid. 4 bytes

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <assert.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <signal.h>
#include <sys/syscall.h>
#include<signal.h>
#include <sys/time.h>
#include <malloc.h>
#include <netinet/tcp.h>

#include "utility.h"
#include "dict.h"
#include "qp_common.h"
#include "io_ops_common.h"

#include "rpc.h"


#define PORT 8888

#define SIZE_IO_REDIRECT_HT	(4096)

#define DEF_CQ_MOD	(100)
//#define QUEUE_SIZE	(128)
#define QUEUE_SIZE	(1)
#define CTX_POLL_BATCH		(16)

char szExeName[128];

//__thread int _tid=0;
static pthread_mutex_t process_lock;	// for this process

static pthread_mutex_t ht_qp_lock;
static CHASHTABLE_INT *pHT_qp=NULL;
static struct elt_Int *elt_list_qp = NULL;
static int *ht_table_qp=NULL;
static int jobid = 0, nnode_this_job=0;
static int bDebug=0;

typedef	struct	{
	char fNameStdin[256], fNameStdout[256], fNameStderr[256];
}IO_REDIRECT_REC;

CHASHTABLE_INT *pHT_IO_Redirect=NULL;
struct elt_Int *elt_list_IO_Redirect = NULL;
int *ht_table_IO_Redirect=NULL;
IO_REDIRECT_REC *pIO_Redirect_List=NULL;

//char szStdin[256]="";
//char szStdout[256]="";
//char szStderr[256]="";
int fd_stdin=-1, fd_stdout=-1, fd_stderr=-1, Is_in_shell=0;


//atic __thread unsigned char __attribute__((aligned(16))) rem_buff[DATA_COPY_THRESHOLD_SIZE + 4096], loc_buff[DATA_COPY_THRESHOLD_SIZE + 4096]; 
static __thread unsigned char *rem_buff=NULL, *loc_buff=NULL; 
static __thread struct ibv_mr *mr_rem = NULL, *mr_loc = NULL;


void Finalize_Client();

// file server info is stored at /dev/shm/myfs. Use bcast_dir to share this file across nodes. 
typedef	struct	{
	char szIP[16];
	int port;
	char mercuryAddr[MAX_MERCURY_ADDR_LEN];
	int nQP;	// number of active queue pairs on this node
	pthread_mutex_t fs_qp_lock;	// 40 bytes
}FS_SEVER;

typedef	struct	{
	int Init_Start;
	int Init_Done;
	int nFSServer;
	int nNUMAPerNode;
	int myip;
	int pad[3];
	FS_SEVER FS_List[MAX_FS_SERVER];
	pthread_mutex_t lock_IO_Redirect;
}FSSERVERLIST;

FSSERVERLIST *pFileServerList, FileServerListLocal;	// pFileServerList in shared memory


/*
typedef	struct	{
	uint32_t comm_tag;
	uint32_t jobid;	// slurm job id
	uint32_t myip;	// int of the ip of current node
	uint32_t tid;	// thread id
	uint32_t uid;	// user id
	uint32_t gid;	// group id
}SUBMIT_JOB_DATA;
*/

typedef void (*org_sighandler)(int sig, siginfo_t *siginfo, void *ptr);
static org_sighandler org_segv=NULL, org_term=NULL, org_int=NULL;

class	CLIENT_QUEUEPAIR	{
private:
	struct ibv_cq *send_complete_queue = NULL;
//	struct ibv_cq *recv_complete_queue = NULL;
	
	QPAIR_EXCH_DATA qp_my_data, qp_pal_data;
	IB_MEM_DATA my_remote_mem;
	
	uint64_t nPut=0, nPut_Done=0;
	uint64_t nGet=0, nGet_Done=0;
	pthread_mutex_t qp_put_get_lock;
	int sock = 0;
	
	void IB_modify_qp(void);
	void IB_CreateQueuePair(void);
	void Setup_Socket(char szServerIP[]);
	
public:
	static struct ibv_pd* pd_;
	static struct ibv_device** dev_list_;
	static struct ibv_context* context_;
	static struct ibv_port_attr port_attr_;
	static int Done_IB_PD_Init;
	static void Init_IB_Env(void);

	IB_MEM_DATA pal_remote_mem;
	struct ibv_mr *mr_rem_thread = NULL, *mr_loc_thread = NULL;
	struct ibv_mr *mr_loc_qp_Obj = NULL;
	struct ibv_qp *queue_pair = NULL;
	time_t qp_heart_beat_t = 0;
	uint64_t remote_addr_new_msg;	// the address of remote buffer to notify a new message
	uint64_t remote_addr_heart_beat;	// the address of remote buffer to write heart beat time info.
	uint64_t remote_addr_IO_CMD;	// the address of remote buffer to IO requests.
	int qp_put_get_locked;
	int tid = 0;
	int Idx_fs = -1;
	
	void Close_QueuePair(void);
	void Setup_QueuePair(int IdxServer, char loc_buff[], size_t size_loc_buff, char rem_buff[], size_t size_rem_buff);
	static struct ibv_mr* IB_RegisterBuf_RW_Local_Remote(void* buf, size_t len);
	int IB_Put(void* loc_buf, uint32_t lkey, void* rem_buf, uint32_t rkey, size_t len);
	int IB_Get(void* loc_buf, uint32_t lkey, void* rem_buf, uint32_t rkey, size_t len);
};

static __thread CLIENT_QUEUEPAIR *pClient_qp[MAX_FS_SERVER];
static CLIENT_QUEUEPAIR *pClient_qp_List[MAX_QP_PER_PROCESS];

extern int mpi_rank;


void test_connect(hg_handle_t handle, void* buffer);
hg_return_t test_connect_cb(const struct hg_cb_info *info);

void Take_ShortName(char szHostName[])
{
	int i=0;
	
	while(szHostName[i])    {
		if(szHostName[i] == '.')        {
			szHostName[i] = 0;
			if(i >= (MAX_HOSTNAME_LEN))	printf("ERROR> ClientHostName = %s is TOO long!\n", szHostName);
			return;
		}
		i++;
	}
}

void Get_Exe_Name(char szName[])
{
	FILE *fIn;
	char szPath[1024], *ReadLine;
	int i, nLen;
	
//	sprintf(szPath, "/proc/self/cmdline", pid);
	fIn = fopen("/proc/self/cmdline", "r");
	if(fIn == NULL)	{
		printf("Fail to open file: %s\nQuit\n", szPath);
		exit(1);
	}
	
	ReadLine = fgets(szPath, 255, fIn);
	fclose(fIn);
	
	if(ReadLine == NULL)	{
		printf("Fail to determine the executable file name.\nQuit\n");
		exit(1);
	}

	nLen = strlen(szPath);
	for(i=nLen-1; i>=0; i--)	{	// extract short name!!!
		if(szPath[i] == '/')	{
			break;
		}
	}
	strcpy(szName, szPath + i + 1);

	szName[MAX_EXENAME_LEN-1] = 0;
}

inline void Allocate_loc_rem_buff(void)
{
	int i;

	if(rem_buff == NULL)	{
		rem_buff = (unsigned char *)memalign(64, 2*(IO_RESULT_BUFFER_SIZE + 4096));
		assert(rem_buff != NULL);
		mr_rem = CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote(rem_buff, IO_RESULT_BUFFER_SIZE + 4096);
		assert(mr_rem != NULL);
		loc_buff = rem_buff + IO_RESULT_BUFFER_SIZE + 4096;
		mr_loc = CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote(loc_buff, IO_RESULT_BUFFER_SIZE + 4096);
		assert(mr_loc != NULL);

		for(i=0; i<MAX_FS_SERVER; i++)	{
			if(pClient_qp[i])	{
				pClient_qp[i]->mr_loc_thread = mr_loc;
				pClient_qp[i]->mr_rem_thread = mr_rem;
			}
		}
	}
/*
	if(rem_buff == NULL)	{
		rem_buff = (unsigned char *)memalign(64, IO_RESULT_BUFFER_SIZE + 4096);
//		rem_buff = (unsigned char *)memalign(64, DATA_COPY_THRESHOLD_SIZE + 4096);
		assert(rem_buff != NULL);
		mr_rem = CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote(rem_buff, IO_RESULT_BUFFER_SIZE + 4096);
		assert(mr_rem != NULL);
		loc_buff = (unsigned char *)memalign(64, IO_RESULT_BUFFER_SIZE + 4096);
//		loc_buff = (unsigned char *)memalign(64, DATA_COPY_THRESHOLD_SIZE + 4096);
		assert(loc_buff != NULL);
		mr_loc = CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote(loc_buff, IO_RESULT_BUFFER_SIZE + 4096);
		assert(mr_loc != NULL);
	}
*/
}

void CLIENT_QUEUEPAIR::Setup_QueuePair(int IdxServer, char loc_buff[], size_t size_loc_buff, char rem_buff[], size_t size_rem_buff)
{
	int idx;
//	GLOBAL_ADDR_DATA Global_Addr_Data;
	unsigned long long t;
	struct timeval tm1, tm2;
//	JOB_INFO_DATA JobInfo;
	DATA_SEND_BY_SERVER data_to_recv;
	DATA_SEND_BY_CLIENT data_to_send;
	char szHostName[64];

#ifdef SYS_gettid
	tid = syscall(SYS_gettid);
#else
	tid = gettid();
#endif
//	printf("DBG> tid = %d\n", tid);

//	JobInfo.comm_tag = TAG_SUBMIT_JOB_INFO;
//	JobInfo.nnode = nnode_this_job;
//	JobInfo.jobid = jobid;
//	JobInfo.cip = pFileServerList->myip;	// client IP
//	JobInfo.ctid = tid;	// client thred id

	Idx_fs = IdxServer;
	
//	dev_list_ = NULL;
//	context_ = NULL;
//	pd_ = NULL;
//	Done_IB_PD_Init = 0;

	send_complete_queue = NULL;
//	recv_complete_queue = NULL;
	nPut = nPut_Done = 0;
	nGet = nGet_Done = 0;
	sock = 0;
	mr_loc_qp_Obj = NULL;
	queue_pair = NULL;
	qp_heart_beat_t = 0;
	
//	gettimeofday(&tm1, NULL);
    if(pthread_mutex_init(&qp_put_get_lock, NULL) != 0) { 
        printf("\n mutex qp_put_get_lock init failed\n"); 
        exit(1);
    }
	qp_put_get_locked = 0;

	Setup_Socket(FileServerListLocal.FS_List[IdxServer].szIP);
//	gettimeofday(&tm2, NULL);
//	t = 1000000 * (tm2.tv_sec - tm1.tv_sec) + (tm2.tv_usec - tm1.tv_usec);
//	printf("DBG> rank = %d t(Setup_Socket) = %lld\n", mpi_rank, t);
	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


//	Init_IB_Env();
	IB_CreateQueuePair();

	const char *fake_user_id = 0;
	gethostname(szHostName, 63);
	Take_ShortName(szHostName);
//	Get_Exe_Name(szExeName);
	data_to_send.JobInfo.comm_tag = TAG_SUBMIT_JOB_INFO;
	data_to_send.JobInfo.nnode = nnode_this_job;
	data_to_send.JobInfo.jobid = jobid;
	data_to_send.JobInfo.cip = pFileServerList->myip;
	data_to_send.JobInfo.ctid = tid;
	data_to_send.JobInfo.cuid = getuid();
	fake_user_id = getenv("THEMIS_FAKE_USERID");
	if (fake_user_id)
		sscanf(fake_user_id, "%u", &data_to_send.JobInfo.cuid);
	data_to_send.JobInfo.cgid = getgid();
	memcpy(data_to_send.JobInfo.szClientHostName, szHostName, MAX_HOSTNAME_LEN);
	memcpy(data_to_send.JobInfo.szClientExeName, szExeName, MAX_EXENAME_LEN);

	data_to_send.qp.comm_tag = TAG_EXCH_QP_INFO;
	data_to_send.qp.lid = qp_my_data.lid ;
	data_to_send.qp.psn = qp_my_data.psn;
	data_to_send.qp.qp_n = qp_my_data.qp_n;
	
//	qp_my_data.comm_tag = TAG_EXCH_QP_INFO;
//	write(sock, &(qp_my_data), sizeof(uint32_t)*4);
//	read(sock, &(qp_pal_data), sizeof(uint32_t)*4);
//	assert(qp_pal_data.comm_tag == TAG_EXCH_QP_INFO);
//	printf("(%d, %d, %d) (%d, %d, %d)\n", qp_my_data.lid, qp_my_data.qp_n, qp_my_data.psn, qp_pal_data.lid, qp_pal_data.qp_n, qp_pal_data.psn);
	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	
	mr_loc_qp_Obj = IB_RegisterBuf_RW_Local_Remote((void*)this, sizeof(CLIENT_QUEUEPAIR));
	
	if(loc_buff == NULL)	Allocate_loc_rem_buff();
	mr_loc_thread = mr_loc;
	mr_rem_thread = mr_rem;

//	mr_loc = IB_RegisterBuf_RW_Local_Remote((void*)loc_buff, size_loc_buff);
//	assert(mr_loc != NULL);
//	mr_rem = IB_RegisterBuf_RW_Local_Remote((void*)rem_buff, size_rem_buff);
//	assert(mr_rem != NULL);
	my_remote_mem.addr = (unsigned long int)(mr_rem->addr);
	my_remote_mem.key = mr_rem->rkey;

	data_to_send.ib_mem.comm_tag = TAG_EXCH_MEM_INFO;
	data_to_send.ib_mem.addr = my_remote_mem.addr;
	data_to_send.ib_mem.key = my_remote_mem.key;

//	my_remote_mem.comm_tag = TAG_EXCH_MEM_INFO;
//	write(sock, &(my_remote_mem), 2*sizeof(int) + sizeof(long int));

//	read(sock, &(pal_remote_mem), 2*sizeof(int) + sizeof(long int));
//	assert(pal_remote_mem.comm_tag == TAG_EXCH_MEM_INFO);

//	write(sock, &(JobInfo), sizeof(JOB_INFO_DATA));	// submit job info

	write(sock, &(data_to_send), sizeof(DATA_SEND_BY_CLIENT));	// submit job info
	read(sock, &(data_to_recv), sizeof(DATA_SEND_BY_SERVER));

	remote_addr_new_msg = data_to_recv.global_addr.addr_NewMsgFlag;
	remote_addr_heart_beat = data_to_recv.global_addr.addr_TimeHeartBeat;
	remote_addr_IO_CMD = data_to_recv.global_addr.addr_IO_Cmd_Msg;

	qp_pal_data.comm_tag = data_to_recv.qp.comm_tag;
	qp_pal_data.lid = data_to_recv.qp.lid;
	qp_pal_data.psn = data_to_recv.qp.psn;
	qp_pal_data.qp_n = data_to_recv.qp.qp_n;

	pal_remote_mem.comm_tag = data_to_recv.ib_mem.comm_tag;
	pal_remote_mem.addr = data_to_recv.ib_mem.addr;
	pal_remote_mem.key = data_to_recv.ib_mem.key;

//	printf("(%d, %d, %d) (%d, %d, %d)\n", qp_my_data.lid, qp_my_data.qp_n, qp_my_data.psn, qp_pal_data.lid, qp_pal_data.qp_n, qp_pal_data.psn);
//	printf("(%ld, %d) (%ld, %d)\n", my_remote_mem.addr, my_remote_mem.key, pal_remote_mem.addr, pal_remote_mem.key);

	if(bDebug)	printf("INFO> tid = %d Server (%d, %d, %d) Client (%d, %d, %d)\n", tid, qp_pal_data.lid, qp_pal_data.qp_n, qp_pal_data.psn, qp_my_data.lid, qp_my_data.qp_n, qp_my_data.psn);
	IB_modify_qp();

//	read(sock, &(Global_Addr_Data), sizeof(GLOBAL_ADDR_DATA));
//	assert(Global_Addr_Data.comm_tag == TAG_GLOBAL_ADDR_INFO);
//	remote_addr_new_msg = Global_Addr_Data.addr_NewMsgFlag;
//	remote_addr_heart_beat = Global_Addr_Data.addr_TimeHeartBeat;
//	remote_addr_IO_CMD = Global_Addr_Data.addr_IO_Cmd_Msg;

	close(sock);

	pthread_mutex_lock(&ht_qp_lock);

//	pthread_mutex_lock(&(pFileServerList->FS_List[IdxServer].fs_qp_lock));
//	pFileServerList->FS_List[IdxServer].nQP ++;
//	pthread_mutex_unlock(&(pFileServerList->FS_List[IdxServer].fs_qp_lock));

	fetch_and_add(&(pFileServerList->FS_List[IdxServer].nQP), 1);

	idx = pHT_qp->DictInsertAuto(tid, &elt_list_qp, &ht_table_qp);
	pClient_qp_List[idx] = this;
	pClient_qp[IdxServer] = this;
	
	nPut = 0;
	nGet = 0;
	nPut_Done = 0;
	nGet_Done = 0;
		
	pthread_mutex_unlock(&ht_qp_lock);
}

void CLIENT_QUEUEPAIR::Setup_Socket(char szServerIP[])
{
    struct sockaddr_in serv_addr; 
	int one = 1;
	struct timeval tm1, tm2;	// tm1.tv_sec
	unsigned long long t;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) { 
        printf("\n Socket creation error \n"); 
        return; 
    } 

    if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)	perror("setsockopt(2) error");
	
    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(PORT); 
	
    // Convert IPv4 and IPv6 addresses from text to binary form 
    if(inet_pton(AF_INET, szServerIP, &serv_addr.sin_addr)<=0)  { 
        printf("\nInvalid address/ Address not supported \n"); 
        return; 
    }
	
//	gettimeofday(&tm1, NULL);
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) { 
        printf("\nConnection Failed \n"); 
        return;
    }
//	gettimeofday(&tm2, NULL);
//	t = 1000000 * (tm2.tv_sec - tm1.tv_sec) + (tm2.tv_usec - tm1.tv_usec);
//	printf("DBG> Rank = %d t_connect = %lld\n", mpi_rank, t);

    if (setsockopt(sock, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)	perror("setsockopt(2) error");
}

struct ibv_device** CLIENT_QUEUEPAIR::dev_list_=NULL;
struct ibv_context* CLIENT_QUEUEPAIR::context_=NULL;
struct ibv_pd* CLIENT_QUEUEPAIR::pd_=NULL;
struct ibv_port_attr CLIENT_QUEUEPAIR::port_attr_={};
int CLIENT_QUEUEPAIR::Done_IB_PD_Init = 0;

void CLIENT_QUEUEPAIR::Init_IB_Env(void)
{
	int ret, Found_IB=0;
	int nDevices;
	struct ibv_device_attr device_attr;
	struct timeval tm1, tm2;	// tm1.tv_sec
	unsigned long long t;

	pthread_mutex_lock(&process_lock);
	
	if(Done_IB_PD_Init == 0)	{
		dev_list_ = ibv_get_device_list(&nDevices);
		
		if (!dev_list_) {
			int errno_backup = errno;
			fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_get_device_list (errno=%d).\n", __FILE__, __LINE__, errno_backup);
			exit(1);
		}

		for (int i = 0; i < nDevices; i++) {
			if (!dev_list_[i]) {
				continue;
			}
			context_ = ibv_open_device(dev_list_[i]);
			if (!context_)	continue;

			ret = ibv_query_device(context_, &device_attr);
			ret = ibv_query_port(context_, 1, &port_attr_);
			if (ret != 0 || port_attr_.lid == 0) {
//				fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_query_port.\n", __FILE__, __LINE__);
//				exit(1);
				ibv_close_device(context_);
				continue;
			}

			Found_IB = 1;
			break;
		}
		
		if (!Found_IB) {
			fprintf(stderr, "Error occured at %s:L%d. Failure: No HCA can use.\n", __FILE__, __LINE__);
			exit(1);
		}

		pd_ = ibv_alloc_pd(context_);
		if (!pd_) {
			fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_alloc_pd.\n", __FILE__, __LINE__);
			exit(1);
		}
		Done_IB_PD_Init = 1;
	}
	pthread_mutex_unlock(&process_lock);
	
	ibv_free_device_list(dev_list_);
	dev_list_ = NULL;

//	nPut = 0;
//	nGet = 0;
//	nPut_Done = 0;
//	nGet_Done = 0;
}


void CLIENT_QUEUEPAIR::Close_QueuePair(void)
{
	int IdxServer;
	struct timeval tm1, tm2;
	unsigned long long t;

	IdxServer = Idx_fs;
	pthread_mutex_lock(&(pFileServerList->FS_List[IdxServer].fs_qp_lock));
	pFileServerList->FS_List[IdxServer].nQP --;
	pthread_mutex_unlock(&(pFileServerList->FS_List[IdxServer].fs_qp_lock));

	// release queues
//	gettimeofday(&tm1, NULL);
	if (queue_pair)	{
		ibv_destroy_qp(queue_pair);
		queue_pair = NULL;

		pthread_mutex_destroy(&qp_put_get_lock);
		qp_put_get_locked = 0;
	}
//    if (recv_complete_queue != NULL)
//		ibv_destroy_cq(recv_complete_queue);
    if (send_complete_queue != NULL)
		ibv_destroy_cq(send_complete_queue);
	
	// release memory region which is nonblocking-io but not freed.
	if(mr_rem)	{
		mr_rem = mr_loc = NULL;
		ibv_dereg_mr(mr_rem_thread);
		ibv_dereg_mr(mr_loc_thread);
	}
	ibv_dereg_mr(mr_loc_qp_Obj);

//	gettimeofday(&tm2, NULL);
//	t = 1000000 * (tm2.tv_sec - tm1.tv_sec) + (tm2.tv_usec - tm1.tv_usec);
//	printf("DBG> t_release = %lld\n", t);
}

void CLIENT_QUEUEPAIR::IB_modify_qp(void)
{
	int ret;
	
	struct ibv_qp_attr init_attr = {};
	init_attr.qp_state = IBV_QPS_INIT;
	init_attr.port_num = 1;
	//  init_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE;
	init_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
	
	ret = ibv_modify_qp(
		queue_pair, &init_attr,
		IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
	if (ret != 0) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_modify_qp(1).\n", __FILE__, __LINE__);
		exit(1);
	}
	
	struct ibv_qp_attr rtr_attr = {};
	rtr_attr.qp_state = IBV_QPS_RTR;
	rtr_attr.path_mtu = IBV_MTU_4096;
	rtr_attr.dest_qp_num = qp_pal_data.qp_n;
	rtr_attr.rq_psn = qp_pal_data.psn;
	rtr_attr.max_dest_rd_atomic = 1;	// Ref to ctx_set_out_reads() in simple.c!!!!!!!!!!!!!!!!
	
	// retry_speed faster
	rtr_attr.min_rnr_timer = 12;	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	
	// retry_speed slower
	// rtr_attr.min_rnr_timer = 0;
	
	rtr_attr.ah_attr.is_global = 0;
	rtr_attr.ah_attr.dlid = qp_pal_data.lid;
	rtr_attr.ah_attr.sl = 0;
	rtr_attr.ah_attr.src_path_bits = 0;
	rtr_attr.ah_attr.port_num = 1;
	
	ret = ibv_modify_qp(queue_pair, &rtr_attr,
		IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
		IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
		IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
	if (ret != 0) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_modify_qp(2).\n", __FILE__, __LINE__);
		exit(1);
	}
	
	struct ibv_qp_attr rts_attr = {};
	rts_attr.qp_state = IBV_QPS_RTS;
	rts_attr.path_mtu = IBV_MTU_4096;	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	rts_attr.timeout = 14;		// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	rts_attr.retry_cnt = 7;
	rts_attr.rnr_retry = 7;
	rts_attr.sq_psn = qp_my_data.psn;
	rts_attr.max_rd_atomic = 1;	// Ref to ctx_set_out_reads() in simple.c!!!!!!!!!!!!!!!!
	rts_attr.ah_attr.dlid = qp_pal_data.lid; // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	
	ret = ibv_modify_qp(queue_pair, &rts_attr,
		IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
		IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
		IBV_QP_MAX_QP_RD_ATOMIC);
	if (ret != 0) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_modify_qp(3).\n", __FILE__, __LINE__);
		exit(1);
	}
}

void CLIENT_QUEUEPAIR::IB_CreateQueuePair(void)
{
	send_complete_queue = ibv_create_cq(context_, QUEUE_SIZE, NULL, NULL, 0);
//	recv_complete_queue = ibv_create_cq(context_, QUEUE_SIZE, NULL, NULL, 0);
	
	if (!send_complete_queue) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_create_cq of send cq.\n", __FILE__, __LINE__);
		exit(1);
	}
	
//	if (!recv_complete_queue) {
//		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_create_cq of recv cq.\n", __FILE__, __LINE__);
//		exit(1);
//	}
	
	uint32_t my_psn = random() % 0xFFFFFF;
	
	struct ibv_qp_init_attr qp_init_attr = {};
	qp_init_attr.qp_type = IBV_QPT_RC;
	qp_init_attr.send_cq = send_complete_queue;
	qp_init_attr.recv_cq = send_complete_queue;
	
	qp_init_attr.cap.max_send_wr = QUEUE_SIZE;
	qp_init_attr.cap.max_recv_wr = 1;
	qp_init_attr.cap.max_send_sge = 1;
	qp_init_attr.cap.max_recv_sge = 1;
	qp_init_attr.sq_sig_all = 1;
	//  qp_init_attr.sq_sig_all = 0;	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	//  qp_init_attr.sq_sig_all = 1;	// 0 - In every Work Request submitted to the Send Queue, the user must decide whether to generate a Work Completion for successful 
	//     completions or not
	// 1 - All Work Requests that will be submitted to the Send Queue will always generate a Work Completion !!!!!!!!!!
	qp_init_attr.cap.max_inline_data = 220;
	
	queue_pair = ibv_create_qp(pd_, &qp_init_attr);
	
	if (!queue_pair) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_create_qp().\n", __FILE__, __LINE__);
		exit(1);
	}
	
	qp_my_data.lid = port_attr_.lid;
	qp_my_data.qp_n = queue_pair->qp_num;
	qp_my_data.psn = my_psn;
}

struct ibv_mr* CLIENT_QUEUEPAIR::IB_RegisterBuf_RW_Local_Remote(void* buf, size_t len)
{
	struct ibv_mr* mr_buf = ibv_reg_mr(pd_, buf, len, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
	if (mr_buf == 0) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_reg_mr on RW_Local_Remote.\n", __FILE__, __LINE__);
		perror("ibv_reg_mr().");
		exit(1);
	}
	
	return mr_buf;
}

int CLIENT_QUEUEPAIR::IB_Put(void* loc_buf, uint32_t lkey, void* rem_buf, uint32_t rkey, size_t len)
{
	int ne, ret;
	
	if(queue_pair == NULL)	return 1;
	
	struct ibv_sge sge = {};
	sge.addr = (uint64_t)(uintptr_t)loc_buf;
	sge.length = len;
	sge.lkey = lkey;
	
	struct ibv_send_wr write_wr = {};
	struct ibv_wc wc = {};
	
	memset(&write_wr, 0, sizeof(struct ibv_send_wr));
	write_wr.wr_id = 0;
	write_wr.sg_list = &sge;
	write_wr.num_sge = 1;
	write_wr.opcode = IBV_WR_RDMA_WRITE;
	write_wr.wr.rdma.rkey = rkey;
	write_wr.wr.rdma.remote_addr = (uint64_t)rem_buf;
	
	write_wr.send_flags = 2;
	if(len <= 220)  write_wr.send_flags |= IBV_SEND_INLINE;

	struct ibv_send_wr* bad_wr;

	pthread_mutex_lock(&qp_put_get_lock);
	qp_put_get_locked = 1;

	ret = ibv_post_send(queue_pair, &write_wr, &bad_wr);
	if (ret != 0) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_post_send in Put(). ret = %d\n", __FILE__, __LINE__, ret);
		exit(1);
	}
	
	nPut++;
	
	if ( (nPut%DEF_CQ_MOD) == (DEF_CQ_MOD - 1)) {
		write_wr.send_flags |= IBV_SEND_SIGNALED;
	}
	
	if( (nPut - nPut_Done) >= QUEUE_SIZE ) {
		memset(&wc, 0, sizeof(struct ibv_wc));
		
		while(1)	{
			ne = ibv_poll_cq(send_complete_queue, 1, &wc);
//			ne = ibv_poll_cq(send_complete_queue, CTX_POLL_BATCH, &wc);
			if (ne == 1) {
				nPut_Done +=1;
				//				nPut_Done += DEF_CQ_MOD;
				break;
			}
			else if(ne == 0)	{	// equal 0. Message not sent out yet!!!!!!!!!!!!!
			}
			else if (ne < 0) {
				fprintf(stderr, "poll CQ failed %d\n",ne);
				exit(1);
				return 1;
			}
			else	{
//				fprintf(stderr, "ibv_poll_cq() return code is %d\n",ne);
//				exit(1);
//				return 1;
				fprintf(stderr, "ibv_poll_cq() return code is %d. nPut = %ld\n",ne, nPut);
				break;
			}
		}
	}
	qp_put_get_locked = 0;
	pthread_mutex_unlock(&qp_put_get_lock);
	
	return 0;
}

int CLIENT_QUEUEPAIR::IB_Get(void* loc_buf, uint32_t lkey, void* rem_buf, uint32_t rkey, size_t len)
{
	int ne, ret;
	
	if(queue_pair == NULL)	return 1;
	
	struct ibv_sge sge = {};
	sge.addr = (uint64_t)(uintptr_t)loc_buf;
	sge.length = len;
	sge.lkey = lkey;
	
	struct ibv_send_wr write_wr = {};
	struct ibv_wc wc[CTX_POLL_BATCH];
	
	memset(&write_wr, 0, sizeof(struct ibv_send_wr));
	write_wr.wr_id = 0;
	write_wr.sg_list = &sge;
	write_wr.num_sge = 1;
	write_wr.opcode = IBV_WR_RDMA_READ;
	write_wr.wr.rdma.rkey = rkey;
	write_wr.wr.rdma.remote_addr = (uint64_t)rem_buf;
	
	//	if ( nGet_Done % DEF_CQ_MOD == 0 ) {
	//		write_wr.send_flags = 0;
	//	}
	//	if ( (nGet % DEF_CQ_MOD) == (DEF_CQ_MOD - 1)) {
	write_wr.send_flags = 2;
	//	}
	
	struct ibv_send_wr* bad_wr;

	pthread_mutex_lock(&qp_put_get_lock);
	qp_put_get_locked = 1;
	ret = ibv_post_send(queue_pair, &write_wr, &bad_wr);
	if (ret != 0) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ibv_post_send in Get(). ret = %d\n", __FILE__, __LINE__, ret);
		exit(1);
	}
	
	nGet++;
	
	if ( (nGet%DEF_CQ_MOD) == (DEF_CQ_MOD - 1)) {
		write_wr.send_flags |= IBV_SEND_SIGNALED;
	}
	
	if( (nGet - nGet_Done) >= QUEUE_SIZE ) {
		memset(wc, 0, sizeof(struct ibv_wc)*CTX_POLL_BATCH);
		
		while(1)	{
			ne = ibv_poll_cq(send_complete_queue, CTX_POLL_BATCH, wc);
			if (ne == 1) {
				//				nGet_Done += DEF_CQ_MOD;
				nGet_Done += 1;
				break;
				
			}
			else if(ne == 0)	{	// equal 0. Message not sent out yet!!!!!!!!!!!!!
			}
			else if (ne < 0) {
				fprintf(stderr, "poll CQ failed %d\n",ne);
				exit(1);
				return 1;
			}
			else	{
				fprintf(stderr, "ibv_poll_cq() return code is %d\n",ne);
				exit(1);
				return 1;
			}
			
		}
	}
	
	qp_put_get_locked = 0;
	pthread_mutex_unlock(&qp_put_get_lock);

	return 0;
}

static struct timespec tim1, tim2;

static void Take_a_Short_Nap(int nsec)
{
    tim1.tv_sec = 0;
    tim1.tv_nsec = nsec;
    nanosleep(&tim1, &tim2);
}

static void Read_FS_Param(void)
{
	char szFileName[128], *szServerConf=NULL;
	FILE *fIn;
	int i, j, nItems;
	
	sprintf(szFileName, "/dev/shm/%s", FS_PARAM_FILE);
	fIn = fopen(szFileName, "r");
	if(fIn == NULL)	{
		szServerConf = getenv("MYFS_CONF");
		if(szServerConf == NULL)	{
			printf("ERROR> Failed to open file %s and get env MYFS_CONF\nQuit\n", szFileName);
			exit(1);
		}
		else	{
			fIn = fopen(szServerConf, "r");
			if(fIn == NULL)	{
				printf("ERROR> Failed to open file %s and file %s\nQuit\n", szFileName, szServerConf);
				exit(1);
			}
		}
	}
	
	nItems = fscanf(fIn, "%d%d", &(pFileServerList->nFSServer), &(pFileServerList->nNUMAPerNode));
	if(nItems != 2)	{
		printf("ERROR> Failed to read nFSServer and nNUMAPerNode.\n");
		fclose(fIn);
		exit(1);
	}
	
	for(i=0; i<pFileServerList->nFSServer; i++)	{
		nItems = fscanf(fIn, "%s%d", pFileServerList->FS_List[i].szIP, &(pFileServerList->FS_List[i].port));
		if(nItems != 2)	{
			printf("ERROR> Failed to read ip port information of file server.\nQuit\n");
			fclose(fIn);
			exit(1);
		}
		if (pthread_mutex_init(&(pFileServerList->FS_List[i].fs_qp_lock), NULL) != 0) { 
			printf("\n mutex init failed\n"); 
			exit(1);
		}
		pFileServerList->FS_List[i].nQP = 0;
	}
	
	fclose(fIn);

}

static void Read_FS_Mercury_Param(void)
{
	printf("DBG> Read_FS_Mercury_Param\n");
	fflush(stdout);
	char mercuryFileName[128], *mercuryServerConf=NULL;
	FILE *fIn;
	int i, j, nItems;
	// TODO: /dev/shm/????
	sprintf(mercuryFileName, "./%s", MERCURY_FS_PARAM_FILE);
	fIn = fopen(mercuryFileName, "r");
	if(fIn == NULL)	{
		mercuryServerConf = getenv("MERCURY_MYFS_CONF");
		if(mercuryServerConf == NULL)	{
			printf("ERROR> Failed to open file %s and get env MERCURY_MYFS_CONF\nQuit\n", mercuryFileName);
			exit(1);
		}
		else	{
			fIn = fopen(mercuryServerConf, "r");
			if(fIn == NULL)	{
				printf("ERROR> Failed to open file %s and file %s\nQuit\n", mercuryFileName, mercuryServerConf);
				exit(1);
			}
		}
	}
	
	nItems = fscanf(fIn, "%d%d", &(pFileServerList->nFSServer), &(pFileServerList->nNUMAPerNode));
	if(nItems != 2)	{
		printf("ERROR> Failed to read nFSServer and nNUMAPerNode.\n");
		fclose(fIn);
		exit(1);
	}
	
	for(i=0; i<pFileServerList->nFSServer; i++)	{
		nItems = fscanf(fIn, "%s", pFileServerList->FS_List[i].mercuryAddr);
		if(nItems != 2)	{
			printf("ERROR> Failed to read mercury address information of file server.\nQuit\n");
			fclose(fIn);
			exit(1);
		}
		// if (pthread_mutex_init(&(pFileServerList->FS_List[i].fs_qp_lock), NULL) != 0) { 
		// 	printf("\n mutex init failed\n"); 
		// 	exit(1);
		// }
		// pFileServerList->FS_List[i].nQP = 0;
	}
	
	fclose(fIn);

}

static int QueryLocalIP(void)
{
	int fd;
	struct ifreq ifr;
	unsigned char *pIP, c;
	
	fd = socket(AF_INET, SOCK_DGRAM, 0);
	ifr.ifr_addr.sa_family = AF_INET;
	strcpy(ifr.ifr_name, IB_DEVICE);
	ioctl(fd, SIOCGIFADDR, &ifr);
	close(fd);
	
	pIP = (unsigned char *)(&(((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr));
	c = pIP[3];
	pIP[3] = pIP[0];
	pIP[0] = c;
	c = pIP[2];
	pIP[2] = pIP[1];
	pIP[1] = c;
//	printf("%s\n", inet_ntoa(((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr));
	
	return *((int*)pIP);
}

//__attribute__((constructor)) void Init_Client()
void Init_Client()
{
	int i, shm_fd, To_Init=0, nSizeHT_IO_Redirect, nSizeofShm;
	char mutex_name[]="shm_fs_param";
	void *p_shm;
	char *szEnvJobID=NULL, *szEnvNNode=NULL, *szEnvDebug=NULL;
	
	CLIENT_QUEUEPAIR::Init_IB_Env();

	pHT_qp = (CHASHTABLE_INT *)malloc(CHASHTABLE_INT::GetStorageSize(MAX_QP_PER_PROCESS));
	pHT_qp->DictCreate(MAX_QP_PER_PROCESS, &elt_list_qp, &ht_table_qp);	// init hash table
	
	for(i=0; i<MAX_QP_PER_PROCESS; i++)	{
		pClient_qp_List[i] = NULL;
	}
	for(i=0; i<MAX_FS_SERVER; i++)	{
		pClient_qp[i] = NULL;
	}
	
    if (pthread_mutex_init(&ht_qp_lock, NULL) != 0) { 
        printf("\n mutex init failed\n"); 
        exit(1);
    }
    if(pthread_mutex_init(&process_lock, NULL) != 0) { 
        printf("\n mutex process_lock init failed\n"); 
        exit(1);
    }
	
	nSizeHT_IO_Redirect = CHASHTABLE_INT::GetStorageSize(SIZE_IO_REDIRECT_HT);
	nSizeofShm = sizeof(FSSERVERLIST) + nSizeHT_IO_Redirect + sizeof(IO_REDIRECT_REC)*SIZE_IO_REDIRECT_HT;

	shm_fd = shm_open(mutex_name, O_RDWR, 0664);
	
	if(shm_fd < 0) {	// failed
		shm_fd = shm_open(mutex_name, O_RDWR | O_CREAT | O_EXCL, 0664);	// create 
		
		if(shm_fd == -1)	{
			if(errno == EEXIST)	{	// file exists
				shm_fd = shm_open(mutex_name, O_RDWR, 0664); // try openning file again
				if(shm_fd == -1)    {
					printf("Fail to create file with shm_open().\n");
					exit(1);
				}
			}
			else	{
				printf("DBG> Unexpected error in Init_Client()!\nerrno = %d\n\n", errno);
			}
			Take_a_Short_Nap(300);
		}
		else {
			if (ftruncate(shm_fd, nSizeofShm) != 0) {
				perror("ftruncate");
			}
			To_Init = 1;
		}
	}
	else	{
		Take_a_Short_Nap(300);
	}
	
	p_shm = mmap(NULL, nSizeofShm, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
	if (p_shm == MAP_FAILED) {
		perror("mmap");
	}

	pFileServerList = (FSSERVERLIST *)p_shm;
	pHT_IO_Redirect = (CHASHTABLE_INT *)((char*)p_shm + sizeof(FSSERVERLIST));
	pIO_Redirect_List = (IO_REDIRECT_REC *)((char*)p_shm + sizeof(FSSERVERLIST) + nSizeHT_IO_Redirect);
	if(To_Init == 0)	{
		while(1)	{
			if(pFileServerList->Init_Done == 1)	{
				break;
			}
			Take_a_Short_Nap(500);
		}
		memcpy(&FileServerListLocal, pFileServerList, sizeof(int)*8);
		memcpy(&(FileServerListLocal.FS_List), &(pFileServerList->FS_List), sizeof(FS_SEVER)*pFileServerList->nFSServer);
		pHT_IO_Redirect->DictCreate(0, &elt_list_IO_Redirect, &ht_table_IO_Redirect);	// init hash table
	}
	else	{
		memset(p_shm, 0, sizeof(FSSERVERLIST));
		pFileServerList->Init_Start = 1;
		Read_FS_Param();
		// Read_FS_Mercury_Param();
		memcpy(&FileServerListLocal, pFileServerList, sizeof(int)*8);
		memcpy(&(FileServerListLocal.FS_List), &(pFileServerList->FS_List), sizeof(FS_SEVER)*pFileServerList->nFSServer);
		pFileServerList->myip = QueryLocalIP();

		if(pthread_mutex_init(&(pFileServerList->lock_IO_Redirect), NULL) != 0) { 
			printf("\n mutex pFileServerList->lock_IO_Redirect init failed\n"); 
			exit(1);
		}
		pHT_IO_Redirect->DictCreate(SIZE_IO_REDIRECT_HT, &elt_list_IO_Redirect, &ht_table_IO_Redirect);	// init hash table
		memset(pIO_Redirect_List, 0, sizeof(IO_REDIRECT_REC)*SIZE_IO_REDIRECT_HT);
		
		pFileServerList->Init_Done = 1;
//		printf("ip = %x\n", pFileServerList->myip);
	}

	if(jobid == 0)	{
		szEnvJobID = getenv("THEMIS_FAKE_JOBID");
		if (!szEnvJobID)
			szEnvJobID = getenv("SLURM_JOBID");
		if(szEnvJobID == NULL)	{
			printf("Waring: Fail to call getenv(\"SLURM_JOBID\")\n");
		}
		else	{
			jobid = atoi(szEnvJobID);
		}
	}
	if(nnode_this_job == 0)	{
		szEnvNNode = getenv("THEMIS_FAKE_NNODES");
		if (!szEnvNNode)
			szEnvNNode = getenv("SLURM_NNODES");
		if(szEnvNNode == NULL)	{
			printf("Waring: Fail to call getenv(\"SLURM_NNODES\")\n");
		}
		else	{
			nnode_this_job = atoi(szEnvNNode);
		}
	}
//	printf("DBG> JobID = %d NNode = %d\n", jobid, nnode_this_job);
	
	szEnvDebug = getenv("MYFS_DEBUG");
	if(szEnvDebug)	{
		if(atoi(szEnvDebug) == 1)	bDebug = 1;
	}
}


static void sigsegv_handler(int sig, siginfo_t *siginfo, void *ptr);

static void Update_Queue_Pair_HeartBeat_Time(void);

static void Update_Queue_Pair_HeartBeat_Time(void)
{
	printf("DEBUG> Update_Queue_Pair_HeartBeat_Time\n");
	int i;
	struct timeval tm1;	// tm1.tv_sec
	uint64_t t_cur;

	gettimeofday(&tm1, NULL);
	t_cur = tm1.tv_sec;

	for(i=0; i<MAX_QP_PER_PROCESS; i++)	{
		if(pClient_qp_List[i])	{
			if(pClient_qp_List[i]->queue_pair)	{
				if( ( (t_cur - pClient_qp_List[i]->qp_heart_beat_t) > T_FREQ_ALARM_HB ) && (pClient_qp_List[i]->qp_put_get_locked == 0) )	{// out of dataed and not locked
					// update local heart beat time stamp
					pClient_qp_List[i]->qp_heart_beat_t = t_cur;
					// Write new heart beat!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//					printf("DBG> Addr pClient_qp_List[i]->remote_addr_heart_beat = %p\n", (void *)(pClient_qp_List[i]->remote_addr_heart_beat));
//					printf("DBG> Write heart beat %ld\n", t_cur);
					pClient_qp_List[i]->IB_Put(&(pClient_qp_List[i]->qp_heart_beat_t), pClient_qp_List[i]->mr_loc_qp_Obj->lkey, (void *)(pClient_qp_List[i]->remote_addr_heart_beat), pClient_qp_List[i]->pal_remote_mem.key, sizeof(time_t));
				}
			}
		}
	}
}

void sigalarm_handler(int signum)
{
//  printf("Inside handler function\n");
  Update_Queue_Pair_HeartBeat_Time();

  alarm(T_FREQ_CHECK_ALARM_HB);
}

static void Close_QP(int argc, void *param)
{
	Finalize_Client();
	//	CLIENT_QUEUEPAIR *pQP = (CLIENT_QUEUEPAIR *)param;
	//	printf("In Close_QP(). pQP = %p\n", pQP);
	//	if(Client_qp.queue_pair)	Client_qp.Close_QueuePair();
}


static void sigsegv_handler(int sig, siginfo_t *siginfo, void *uc)
{
	char szMsg[256];
	char szHostName[128];

	Finalize_Client();
	
	gethostname(szHostName, 63);

	//	if(Client_qp.queue_pair)	Client_qp.Close_QueuePair();
	sprintf(szMsg, "Got signal %d (SIGSEGV) Host %s tid = %d\n", siginfo->si_signo, szHostName, syscall(SYS_gettid));
	write(STDERR_FILENO, szMsg, strlen(szMsg));
	fsync(STDERR_FILENO);
	sleep(300);

	if(org_segv)	org_segv(sig, siginfo, uc);
	else	exit(1);
}

static void sigterm_handler(int sig, siginfo_t *siginfo, void *uc)
{
	char szMsg[256];
	//	if(Client_qp.queue_pair)	Client_qp.Close_QueuePair();
	Finalize_Client();
	sprintf(szMsg, "Got signal %d (SIGTERM)\n", siginfo->si_signo);
	write(STDERR_FILENO, szMsg, strlen(szMsg));
	if(org_term)	org_term(sig, siginfo, uc);
	else	exit(0);
}

static void sigint_handler(int sig, siginfo_t *siginfo, void *uc)
{
	char szMsg[256];
	//	if(Client_qp.queue_pair)	Client_qp.Close_QueuePair();
	Finalize_Client();
	sprintf(szMsg, "Got signal %d (SIGINT)\n", siginfo->si_signo);
	write(STDERR_FILENO, szMsg, strlen(szMsg));
	if(org_int)	org_int(sig, siginfo, uc);
	else	exit(0);
}

//typedef int (*org_sigaction)(int signum, const struct sigaction *act, struct sigaction *oldact); 
//org_sigaction p_sigaction=NULL;

//extern "C" int sigaction(int signum, const struct sigaction *act, struct sigaction *oldact)
//{
//	if(p_sigaction==NULL)	p_sigaction = (org_sigaction)dlsym(RTLD_NEXT,"sigaction");

//	if(signum != SIGSEGV)	return p_sigaction(signum, act, oldact);
//	else	return 0;
//}

static void Setup_Signal_QueuePair(void)
{
	struct sigaction act, old_action;
	
    // Set up sigsegv handler
    memset (&act, 0, sizeof(act));
    act.sa_flags = SA_SIGINFO;
	
    act.sa_sigaction = sigsegv_handler;
    if (sigaction(SIGSEGV, &act, &old_action) == -1) {
//    if (sigaction(SIGSEGV, &act, &old_action) == -1) {
        perror("Error: sigaction");
        exit(1);
    }
	if( (old_action.sa_handler != SIG_DFL) && (old_action.sa_handler != SIG_IGN) )	{
		org_segv = old_action.sa_sigaction;
	}
	
    act.sa_sigaction = sigterm_handler;
    if (sigaction(SIGTERM, &act, 0) == -1) {
        perror("Error: sigaction");
        exit(1);
    }
	if( (old_action.sa_handler != SIG_DFL) && (old_action.sa_handler != SIG_IGN) )	{
		org_term = old_action.sa_sigaction;
	}
	
    act.sa_sigaction = sigint_handler;
    if (sigaction(SIGINT, &act, 0) == -1) {
        perror("Error: sigaction");
        exit(1);
    }
	if( (old_action.sa_handler != SIG_DFL) && (old_action.sa_handler != SIG_IGN) )	{
		org_int = old_action.sa_sigaction;
	}
	
	on_exit(Close_QP, NULL);

//	signal(SIGALRM, sigalarm_handler); // Register signal handler
//	alarm(T_FREQ_CHECK_ALARM_HB);  // Scheduled alarm after 2 seconds
}

#endif

