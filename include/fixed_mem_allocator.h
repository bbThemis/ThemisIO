#ifndef __FIXED_MEM_ALLOCATOR
#define __FIXED_MEM_ALLOCATOR

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

class CFIXEDSIZE_MEM_ALLOCATOR {
public:
	char *pAddr=NULL, *pBlockAddr=NULL, *pBlockEndAddr=NULL, *pFlagUsed=NULL;
	int nSizeofBlock, nNumofBlocks, FirstAV, LastUsed;
	pthread_mutex_t lock;	// 40 bytes

	int Query_MemSize(int nByteofBlock, int nMaxBlocks)	{
		nSizeofBlock = nByteofBlock;
		nNumofBlocks = nMaxBlocks;
		return (sizeof(char)*nMaxBlocks + nByteofBlock*nMaxBlocks);
	};
	void Init_Memory_Pool(char *pBuff)	{
		pAddr = pBuff;
		pFlagUsed = pBuff;
		pBlockAddr = pBuff + sizeof(char)*nNumofBlocks;
		pBlockEndAddr = pBlockAddr + nSizeofBlock*nNumofBlocks;
		memset(pFlagUsed, 0, sizeof(char)*nNumofBlocks);
		if(pthread_mutex_init(&lock, NULL) != 0) {
			printf("\n mutex create_new_lock init failed\n");
			exit(1);
		}
		FirstAV = 0;
		LastUsed = -1;
	};
	char* Allocate_a_Block(void)
	{
		int i, idx=-1;
		char szHostName[64];

		if (pthread_mutex_lock(&lock) != 0) {
			perror("pthread_mutex_lock in Free_a_Block().");
			exit(2);
		}
		if(FirstAV < 0) {
			pthread_mutex_unlock(&lock);
                        gethostname(szHostName, 63);
                        printf("DBG> No free block available. Host %s pid = %d\n", szHostName, getpid());
                        fflush(stdout);
                        sleep(300);
                        exit(2);
 
			return NULL;
		}
		for(i=FirstAV; i<nNumofBlocks; i++)	{
			if(pFlagUsed[i] == 0)	{	// unused
				idx = i;
				pFlagUsed[idx] = 1;	// set flag as used
//				if(idx > LastUsed)	{
//					LastUsed = idx;
//				}
				FirstAV = -1;
				for(i=idx+1; i<nNumofBlocks; i++)	{
					if(pFlagUsed[i] == 0)	{
						FirstAV = i;
						break;
					}
				}
				break;
			}
		}
		
		char* returnPtr = NULL;
		if(idx>=0)	returnPtr = (pBlockAddr + idx*nSizeofBlock);
		if (pthread_mutex_unlock(&lock) != 0) {
			perror("pthread_mutex_lock in Free_a_Block().");
			exit(2);
		}
		return returnPtr;
	};

	void Free_a_Block(char *pBlock)	{
		int idx;

		if (pthread_mutex_lock(&lock) != 0) {
			perror("pthread_mutex_lock in Free_a_Block().");
			exit(2);
		}
		if( (pBlock>=pBlock) && (pBlock < pBlockEndAddr) )	{// within allocated pool
			idx = (int)(pBlock - pBlockAddr) / nSizeofBlock;
			pFlagUsed[idx] = 0;
			if(idx < FirstAV)	{
				FirstAV = idx;
			}
//			if(idx == LastUsed)	{
//			}
		}
		else	{
			printf("ERROR> Out of range. %p\n", pBlock);
		}
		if (pthread_mutex_unlock(&lock) != 0) {
			perror("pthread_mutex_lock in Free_a_Block().");
			exit(2);
		}
	};

};

#endif
