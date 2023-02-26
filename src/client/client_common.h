#ifndef _CLIENT_COMMON_H
#define _CLIENT_COMMON_H
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

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
#include "qp_common.h"


char szExeName[128];
#define SIZE_IO_REDIRECT_HT	(4096)
static int jobid = 0, nnode_this_job=0;
int fd_stdin=-1, fd_stdout=-1, fd_stderr=-1, Is_in_shell=0;
static int bDebug=0;

typedef	struct	{
	char szIP[16];
	int port;
	int nQP;	// number of active queue pairs on this node
	pthread_mutex_t fs_qp_lock;	// 40 bytes
}FS_SEVER;

typedef	struct	{
	char fNameStdin[256], fNameStdout[256], fNameStderr[256];
}IO_REDIRECT_REC;

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
static struct timespec tim1, tim2;
static void Take_a_Short_Nap(int nsec)
{
    tim1.tv_sec = 0;
    tim1.tv_nsec = nsec;
    nanosleep(&tim1, &tim2);
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
#endif