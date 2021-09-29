#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <sys/time.h>

#include "unique_thread.h"

CCreatedUniqueThread Unique_Thread;

void CCreatedUniqueThread::Init_UniqueThread(void)
{
	struct timeval tm;

	nThreadAccu = 0;
	memset(IsTokenRedeemable, 0, sizeof(char)*MAX_SIZE_REDEEMABLE_LIST);

    if(pthread_mutex_init(&lock, NULL) != 0) { 
        printf("\n CCreatedUniqueThread lock init failed\n"); 
        exit(1);
    }
}

int	CCreatedUniqueThread::Apply_A_Token(void)
{
	int nToken;

	pthread_mutex_lock(&lock);
	nThreadAccu++;
	nToken = nThreadAccu;
	IsTokenRedeemable[nToken & MAX_SIZE_REDEEMABLE_LIST_M1] = 1;
	pthread_mutex_unlock(&lock);

	return nToken;
}

int CCreatedUniqueThread::Redeem_A_Token(int nToken)
{
	int Success, idx;

	pthread_mutex_lock(&lock);
	idx = nToken & MAX_SIZE_REDEEMABLE_LIST_M1;
	if(IsTokenRedeemable[idx])	{
		IsTokenRedeemable[idx] = 0;	// clear the flag
		Success = 1;
	}
	else	Success = 0;
	pthread_mutex_unlock(&lock);

	return Success;
}

