#define _GNU_SOURCE

#include <sched.h>
#include <sys/sysinfo.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/syscall.h>

#include "corebinding.h"

void CORE_BINDING::Init_Core_Binding(void)	// init 
{
	cpu_set_t core_set;
	int i;
	
	nNumCores = get_nprocs();
	nThreadWithBinding = 0;
	
	CPU_ZERO(&core_set);
	sched_getaffinity(0, sizeof(cpu_set_t), &core_set);	// 0 means current process
	
	nCoresAV = 0;
//	printf("Cores available");
	for(i=0; i<nNumCores; i++) {
        if(CPU_ISSET(i, &core_set))	{
//			printf(" %d", i);
			CoresAVList[nCoresAV] = i;
			nCoresAV++;
        }
    }
//	printf("\n");

    if(pthread_mutex_init(&lock, NULL) != 0) { 
        printf("\n mutex lock init failed in CORE_BINDING::Init_Core_Binding()\n"); 
        exit(1);
    }
}

void CORE_BINDING::Bind_This_Thread(void)	// Automatically inc nThreadWithBinding and set core binding for given thread
{
	cpu_set_t core_set;
	int tid;

#ifdef SYS_gettid
	tid = syscall(SYS_gettid);
#else
	tid = gettid();
#endif

	CPU_ZERO(&core_set);

	pthread_mutex_lock(&lock);
	CPU_SET(CoresAVList[nThreadWithBinding % nCoresAV], &core_set);

	if (sched_setaffinity(tid, sizeof(cpu_set_t), &core_set) < 0)	{
		printf("Failed to get tid %d's affinity.\nQuit\n", tid);
		perror("sched_getaffinity.");
		exit(1);
	}
	nThreadWithBinding++;
	pthread_mutex_unlock(&lock);
}




