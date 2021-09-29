#ifndef __QUEUE_FREE_MEMORY
#define __QUEUE_FREE_MEMORY

#include <pthread.h>

void* Func_thread_Free_Memory(void *pParam);	// a thread running to release all memory pages not needed any more

class CQUEUE_FREE_MEM {
public:
	volatile long int front, back;	// 16 bytes
	void **pMemList=NULL;		// 8 bytes
	pthread_mutex_t lock;	// 40 bytes

	volatile long int front_ex_p, back_ex_p;	// 16 bytes
	void **pMemList_Ex_Pointer=NULL;		// 8 bytes
	pthread_mutex_t lock_Ex_Pointer;	// 40 bytes

	volatile long int front_Dir_Entry, back_Dir_Entry;	// 16 bytes
	void **pMemList_Dir_Entry=NULL;		// 8 bytes
	pthread_mutex_t lock_Dir_Entry;	// 40 bytes

	void Enqueue(void **pMemToFree, int nEntries);
	void Enqueue_Ex_Pointer(void *pMemToFree_Ex_Pointer);
	void Enqueue_Dir_Entry(void *pMemToFree_Dir_Entry);
};

#endif
