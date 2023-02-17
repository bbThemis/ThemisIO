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

typedef	struct	{
	char szIP[16];
	int port;
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

#endif