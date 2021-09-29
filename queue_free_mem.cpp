#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <assert.h>
#include <string.h>

#include "queue_free_mem.h"
#include "corebinding.h"
#include "buddy.h"
#include "ncx_slab.h"

#define MAX_SIZE_MEM_TO_FREE	(64*1024*1024L)
#define LEN_QUEUE_FREE_MEM		(MAX_SIZE_MEM_TO_FREE / sizeof(void *))
#define LEN_QUEUE_FREE_MEM_M1	(LEN_QUEUE_FREE_MEM - 1)

#define MAX_SIZE_EX_POINTER_TO_FREE	(64*1024*1024L)
#define LEN_QUEUE_FREE_EX_POINTER		(MAX_SIZE_EX_POINTER_TO_FREE / sizeof(void *))
#define LEN_QUEUE_FREE_EX_POINTER_M1	(LEN_QUEUE_FREE_EX_POINTER - 1)

#define MAX_SIZE_DIR_ENTRY_TO_FREE	(64*1024*1024L)
#define LEN_QUEUE_FREE_DIR_ENTRY		(MAX_SIZE_DIR_ENTRY_TO_FREE / sizeof(void *))
#define LEN_QUEUE_FREE_DIR_ENTRY_M1	(LEN_QUEUE_FREE_DIR_ENTRY - 1)

//#define MAX_SIZE_CALL_RETURN_TO_FREE	(8*1024*1024L)
//#define LEN_QUEUE_FREE_CALL_RETURN		(MAX_SIZE_CALL_RETURN_TO_FREE / sizeof(void *))
//#define LEN_QUEUE_FREE_CALL_RETURN_M1	(LEN_QUEUE_FREE_CALL_RETURN - 1)

#define PACKED_FREE_MEM_OP_SIZE	(64L)	// This is used to minimize the times of locking memory allocator
#define PACKED_FREE_EX_POINTER_OP_SIZE	(240L)	// This is used to minimize the times of locking memory allocator
#define PACKED_FREE_DIR_ENTRY_OP_SIZE	(480L)	// This is used to minimize the times of locking memory allocator
//#define PACKED_FREE_CALL_RETURN_OP_SIZE	(120L)	// This is used to minimize the times of locking memory allocator

extern CORE_BINDING CoreBinding;
extern CMEM_ALLOCATOR *pMem_Allocator;
extern ncx_slab_pool_t *sp_ExtraPointers;
extern ncx_slab_pool_t *sp_DirEntryHashTableBuff;

CQUEUE_FREE_MEM CQueue_Free_Mem;

void* Func_thread_Free_Memory(void *pParam)	// a thread running to release all memory pages not needed any more
{
	void *pMemListToFree[PACKED_FREE_MEM_OP_SIZE+1];
	long int front_mod, front_mod_max, nCopied, idx, idx_mod, front_ex_p_Save, front_Dir_Entry_Save;

	CoreBinding.Bind_This_Thread();

	// init
	CQueue_Free_Mem.front = 0;
	CQueue_Free_Mem.back = -1;

	CQueue_Free_Mem.front_ex_p = 0;
	CQueue_Free_Mem.back_ex_p = -1;

	if(pthread_mutex_init(&(CQueue_Free_Mem.lock), NULL) != 0) { 
		printf("\n mutex lock init failed for CQueue_Free_Mem.lock in Func_thread_Free_Memory()\n"); 
		exit(1);
	}
	if(pthread_mutex_init(&(CQueue_Free_Mem.lock_Ex_Pointer), NULL) != 0) { 
		printf("\n mutex lock init failed for CQueue_Free_Mem.lock_Ex_Pointer in Func_thread_Free_Memory()\n"); 
		exit(1);
	}
	if(pthread_mutex_init(&(CQueue_Free_Mem.lock_Dir_Entry), NULL) != 0) { 
		printf("\n mutex lock init failed for CQueue_Free_Mem.lock_Dir_Entry in Func_thread_Free_Memory()\n"); 
		exit(1);
	}
//	if(pthread_mutex_init(&(CQueue_Free_Mem.lock_Call_Return), NULL) != 0) { 
//		printf("\n mutex lock init failed for CQueue_Free_Mem.lock_Call_Return in Func_thread_Free_Memory()\n"); 
//		exit(1);
//	}

	pMemListToFree[PACKED_FREE_MEM_OP_SIZE] = NULL;	// The end of the list

	CQueue_Free_Mem.pMemList = (void **)memalign(64, MAX_SIZE_MEM_TO_FREE);
	assert(CQueue_Free_Mem.pMemList != NULL);

	CQueue_Free_Mem.pMemList_Ex_Pointer = (void **)memalign(64, MAX_SIZE_EX_POINTER_TO_FREE);
	assert(CQueue_Free_Mem.pMemList_Ex_Pointer != NULL);
	memset(CQueue_Free_Mem.pMemList_Ex_Pointer, 0, MAX_SIZE_EX_POINTER_TO_FREE);

	CQueue_Free_Mem.pMemList_Dir_Entry = (void **)memalign(64, MAX_SIZE_DIR_ENTRY_TO_FREE);
	assert(CQueue_Free_Mem.pMemList_Dir_Entry != NULL);
	memset(CQueue_Free_Mem.pMemList_Dir_Entry, 0, MAX_SIZE_DIR_ENTRY_TO_FREE);

//	CQueue_Free_Mem.pMemList_Call_Return = (void **)memalign(64, MAX_SIZE_CALL_RETURN_TO_FREE);
//	assert(CQueue_Free_Mem.pMemList_Call_Return != NULL);
//	memset(CQueue_Free_Mem.pMemList_Call_Return, 0, MAX_SIZE_CALL_RETURN_TO_FREE);

	// start the loop to check whethere there exist memory blocks to free
	while(1)	{
		if( (CQueue_Free_Mem.back) >= (CQueue_Free_Mem.front+PACKED_FREE_MEM_OP_SIZE-1) )	{	// A queue that is not empty.
			pthread_mutex_lock(&(CQueue_Free_Mem.lock));
			if( (CQueue_Free_Mem.back) >= (CQueue_Free_Mem.front+PACKED_FREE_MEM_OP_SIZE-1) )	{	// double check
				front_mod = CQueue_Free_Mem.front & LEN_QUEUE_FREE_MEM_M1;
				front_mod_max = front_mod + PACKED_FREE_MEM_OP_SIZE;
				if( front_mod_max <=  LEN_QUEUE_FREE_MEM )	{
					memcpy(pMemListToFree, &(CQueue_Free_Mem.pMemList[front_mod]), sizeof(void*)*PACKED_FREE_MEM_OP_SIZE);
				}
				else	{	// Two memcpy
					nCopied = LEN_QUEUE_FREE_MEM - front_mod;
					memcpy(pMemListToFree, &(CQueue_Free_Mem.pMemList[front_mod]), sizeof(void*)*nCopied);
					memcpy(&(pMemListToFree[nCopied]), &(CQueue_Free_Mem.pMemList[0]), sizeof(void*)*(PACKED_FREE_MEM_OP_SIZE - nCopied));
				}
				CQueue_Free_Mem.front += PACKED_FREE_MEM_OP_SIZE;
				pthread_mutex_unlock(&(CQueue_Free_Mem.lock));

				pMem_Allocator->Mem_Batch_Free(pMemListToFree);
			}
			else	{
				pthread_mutex_unlock(&(CQueue_Free_Mem.lock));
			}
		}

		if( (CQueue_Free_Mem.back_ex_p) >= (CQueue_Free_Mem.front_ex_p+PACKED_FREE_EX_POINTER_OP_SIZE-1) )	{	// A queue that is not empty.
			pthread_mutex_lock(&(CQueue_Free_Mem.lock_Ex_Pointer));
			if( (CQueue_Free_Mem.back_ex_p) >= (CQueue_Free_Mem.front_ex_p+PACKED_FREE_EX_POINTER_OP_SIZE-1) )	{	// double check
				front_ex_p_Save = CQueue_Free_Mem.front_ex_p;
				CQueue_Free_Mem.front_ex_p += PACKED_FREE_EX_POINTER_OP_SIZE;

				pthread_mutex_unlock(&(CQueue_Free_Mem.lock_Ex_Pointer));
				
				if (pthread_mutex_lock(&(sp_ExtraPointers->mutex)) != 0) {
					perror("mutex_lock");
					exit(2);
				}
				
				for(idx=0; idx<PACKED_FREE_EX_POINTER_OP_SIZE; idx++)	{
					idx_mod = (front_ex_p_Save + idx) & LEN_QUEUE_FREE_EX_POINTER_M1;
					if(CQueue_Free_Mem.pMemList_Ex_Pointer[idx_mod])	ncx_slab_free_locked(sp_ExtraPointers, CQueue_Free_Mem.pMemList_Ex_Pointer[idx_mod]);
				}
				
				if (pthread_mutex_unlock(&(sp_ExtraPointers->mutex)) != 0) {
					perror("mutex_lock");
					exit(2);
				}
			}
			else	{
				pthread_mutex_unlock(&(CQueue_Free_Mem.lock_Ex_Pointer));
			}
		}

		if( (CQueue_Free_Mem.back_Dir_Entry) >= (CQueue_Free_Mem.front_Dir_Entry+PACKED_FREE_DIR_ENTRY_OP_SIZE-1) )	{	// A queue that is not empty.
			pthread_mutex_lock(&(CQueue_Free_Mem.lock_Dir_Entry));
			if( (CQueue_Free_Mem.back_Dir_Entry) >= (CQueue_Free_Mem.front_Dir_Entry+PACKED_FREE_DIR_ENTRY_OP_SIZE-1) )	{	// double check
				front_Dir_Entry_Save = CQueue_Free_Mem.front_Dir_Entry;
				CQueue_Free_Mem.front_Dir_Entry += PACKED_FREE_DIR_ENTRY_OP_SIZE;

				pthread_mutex_unlock(&(CQueue_Free_Mem.lock_Dir_Entry));
				
				if (pthread_mutex_lock(&(sp_DirEntryHashTableBuff->mutex)) != 0) {
					perror("mutex_lock");
					exit(2);
				}
				
				for(idx=0; idx<PACKED_FREE_DIR_ENTRY_OP_SIZE; idx++)	{
					idx_mod = (front_Dir_Entry_Save + idx) & LEN_QUEUE_FREE_DIR_ENTRY_M1;
					if(CQueue_Free_Mem.pMemList_Dir_Entry[idx_mod])	ncx_slab_free_locked(sp_DirEntryHashTableBuff, CQueue_Free_Mem.pMemList_Dir_Entry[idx_mod]);
				}
				
				if (pthread_mutex_unlock(&(sp_DirEntryHashTableBuff->mutex)) != 0) {
					perror("mutex_lock");
					exit(2);
				}
			}
			else	{
				pthread_mutex_unlock(&(CQueue_Free_Mem.lock_Dir_Entry));
			}
		}

	}

	return NULL;
}

void CQUEUE_FREE_MEM::Enqueue(void **pMemToFree, int nEntries)
{
	long int back_mod, back_mod_max, nCopied, Len_Queue_Free_Mem;
	
	Len_Queue_Free_Mem = LEN_QUEUE_FREE_MEM;
	while(1)	{
		if (pthread_mutex_lock(&lock) != 0) {
			perror("pthread_mutex_lock");
			exit(2);
		}
		if(( (back + nEntries - 1) - front ) < Len_Queue_Free_Mem)	{	// have enough space.
			back_mod = (back + 1) & LEN_QUEUE_FREE_MEM_M1;
			back_mod_max = back_mod + nEntries;
			if( back_mod_max <=  LEN_QUEUE_FREE_MEM )	{	// no rewind
				memcpy(&(pMemList[back_mod]), (void*)(pMemToFree), sizeof(void*)*nEntries);
			}
			else	{	// Two memcpy
				nCopied = LEN_QUEUE_FREE_MEM - back_mod;
				memcpy(&(pMemList[back_mod]), (void*)(pMemToFree), sizeof(void*)*nCopied);
				memcpy(&(pMemList[0]), (void*)(&(pMemToFree[nCopied])), sizeof(void*)*(nEntries - nCopied));	// rewind
			}
			back += nEntries;

			if (pthread_mutex_unlock(&lock) != 0) {
				perror("pthread_mutex_lock");
				exit(2);
			}
			break;
		}
		else	{
			if (pthread_mutex_unlock(&lock) != 0) {
				perror("pthread_mutex_lock");
				exit(2);
			}
			while( ( (back + nEntries - 1) - front ) >= Len_Queue_Free_Mem )      {       // Queue is full. block until queue has enough space
				printf("CQUEUE_FREE_MEM is FULL.\n");
			}
		}
	}
}

void CQUEUE_FREE_MEM::Enqueue_Ex_Pointer(void *pMemToFree_Ex_Pointer)
{
	long int back_mod, Len_Queue_Free_Ex_Pointer;
	
	Len_Queue_Free_Ex_Pointer = LEN_QUEUE_FREE_EX_POINTER;
	while(1)	{
		if (pthread_mutex_lock(&lock_Ex_Pointer) != 0) {
			perror("pthread_mutex_lock");
			exit(2);
		}
		if(( back_ex_p - front_ex_p ) < Len_Queue_Free_Ex_Pointer)	{	// have enough space.
			back_mod = (back_ex_p + 1) & LEN_QUEUE_FREE_EX_POINTER_M1;
			pMemList_Ex_Pointer[back_mod] = pMemToFree_Ex_Pointer;
			back_ex_p ++;

			if (pthread_mutex_unlock(&lock_Ex_Pointer) != 0) {
				perror("pthread_mutex_lock");
				exit(2);
			}
			break;
		}
		else	{
			if (pthread_mutex_unlock(&lock_Ex_Pointer) != 0) {
				perror("pthread_mutex_lock");
				exit(2);
			}
			while( ( back_ex_p - front_ex_p ) >= Len_Queue_Free_Ex_Pointer )      {       // Queue is full. block until queue has enough space
				printf("CQUEUE_FREE_MEM is FULL.\n");
			}
		}
	}
}

void CQUEUE_FREE_MEM::Enqueue_Dir_Entry(void *pMemToFree_Dir_Entry)
{
	long int back_mod, Len_Queue_Free_Dir_Entry;
	
	Len_Queue_Free_Dir_Entry = LEN_QUEUE_FREE_DIR_ENTRY;
	while(1)	{
		if (pthread_mutex_lock(&lock_Dir_Entry) != 0) {
			perror("pthread_mutex_lock");
			exit(2);
		}
		if(( back_Dir_Entry - front_Dir_Entry ) < Len_Queue_Free_Dir_Entry)	{	// have enough space.
			back_mod = (back_Dir_Entry + 1) & LEN_QUEUE_FREE_DIR_ENTRY_M1;
			pMemList_Dir_Entry[back_mod] = pMemToFree_Dir_Entry;
			back_Dir_Entry ++;

			if (pthread_mutex_unlock(&lock_Dir_Entry) != 0) {
				perror("pthread_mutex_lock");
				exit(2);
			}
			break;
		}
		else	{
			if (pthread_mutex_unlock(&lock_Dir_Entry) != 0) {
				perror("pthread_mutex_lock");
				exit(2);
			}
			while( ( back_Dir_Entry - front_Dir_Entry ) >= Len_Queue_Free_Dir_Entry )      {       // Queue is full. block until queue has enough space
				printf("Len_Queue_Free_Dir_Entry is FULL.\n");
			}
		}
	}
}
/*
void CQUEUE_FREE_MEM::Enqueue_Call_Return(void *pMemToFree_Call_Return)
{
	long int back_mod, Len_Queue_Free_Call_Return;
	
	Len_Queue_Free_Call_Return = LEN_QUEUE_FREE_CALL_RETURN;
	while(1)	{
		if (pthread_mutex_lock(&lock_Call_Return) != 0) {
			perror("pthread_mutex_lock");
			exit(2);
		}
		if(( back_Call_Return - front_Call_Return ) < Len_Queue_Free_Call_Return)	{	// have enough space.
			back_mod = (back_Call_Return + 1) & LEN_QUEUE_FREE_CALL_RETURN_M1;
			pMemList_Call_Return[back_mod] = pMemToFree_Call_Return;
			back_Call_Return ++;

			if (pthread_mutex_unlock(&lock_Call_Return) != 0) {
				perror("pthread_mutex_lock");
				exit(2);
			}
			break;
		}
		else	{
			if (pthread_mutex_unlock(&lock_Call_Return) != 0) {
				perror("pthread_mutex_lock");
				exit(2);
			}
			while( ( back_Call_Return - front_Call_Return ) >= Len_Queue_Free_Call_Return )      {       // Queue is full. block until queue has enough space
				printf("Len_Queue_Free_Call_Return is FULL.\n");
			}
		}
	}
}
*/

