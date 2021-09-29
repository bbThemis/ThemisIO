#ifndef __COREBINDING
#define __COREBINDING

#include <pthread.h>

#define MAX_NUM_CORE	(256)

class	CORE_BINDING	{
private:
	int nNumCores;	// the number of cores
	int nThreadWithBinding = 0;
	int nCoresAV;	// the number of available cores for this process
	int CoresAVList[MAX_NUM_CORE];	// the list of available cores this process
	pthread_mutex_t lock;

public:
	void Init_Core_Binding(void);	// init 
	void Bind_This_Thread(void);	// Automatically inc nThreadWithBinding and set core binding for given thread
};

#endif
