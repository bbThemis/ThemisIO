// gcc -g -o client put_get_client.cpp ../dict.cpp ../xxhash.cpp -libverbs -lpthread -lrt

#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <execinfo.h>

#include "qp_client.h"

//static struct timeval tm1, tm2;

int main(int argc, char **argv) {
	char *my_addr=NULL, *rem_addr=NULL;
	unsigned char TagNewMsg=0x80;

	Setup_Signal_QueuePair();

	my_addr = (char*)malloc(4096);
	rem_addr = (char*)malloc(4096);

	pClient_qp[0] = (CLIENT_QUEUEPAIR *)malloc(sizeof(CLIENT_QUEUEPAIR));

	pClient_qp[0]->Setup_QueuePair(0, my_addr, 4096, rem_addr, 4096);
//	pClient_qp->Setup_QueuePair(argv[1], my_addr, 4096, rem_addr, 4096);
	printf("cmp addr %p %p\n", pClient_qp[0]->mr_loc->addr, my_addr);
	
	my_addr[0] = TagNewMsg;

	for(int i=0; i<1000; i++)	{
		printf("DBG> Send a new request.\n");
		pClient_qp[0]->IB_Put(my_addr, pClient_qp[0]->mr_loc->lkey, (void*)(pClient_qp[0]->remote_addr_new_msg + sizeof(char)*pClient_qp[0]->Idx_fs), pClient_qp[0]->pal_remote_mem.key, 1);
		sleep(3);
	}
	
	
	return 0;
}
