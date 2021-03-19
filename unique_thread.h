#ifndef __UNIQUE_THREAD_H__
#define __UNIQUE_THREAD_H__

#include <pthread.h>
#include <assert.h>

#define MAX_SIZE_REDEEMABLE_LIST			(4096)
#define MAX_SIZE_REDEEMABLE_LIST_M1		(MAX_SIZE_REDEEMABLE_LIST-1)

class	CCreatedUniqueThread	{
private:
	int nThreadAccu;
	pthread_mutex_t lock;
	char IsTokenRedeemable[MAX_SIZE_REDEEMABLE_LIST];

public:
	void Init_UniqueThread(void);
	int	Apply_A_Token(void);
	int Redeem_A_Token(int nToken);
};


#endif
