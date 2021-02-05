#include <stdio.h>
#include "io_queue.h"

extern int nFSServer;

int nJobQueue, FirstAV_Queue, IdxLastQueue;	// number of queues for job processing. First avaialble cell to a new queue creation. The index of last queue. Used for looping. 
int ListJobID[MAX_NUM_QUEUE];
IO_QUEUE IO_Queue_List[MAX_NUM_QUEUE];


void Init_QueueList(void)
{
	int i;

	for(i=0; i<MAX_NUM_QUEUE; i++)	{
		ListJobID[i] = INVALID_JOBID;
	}

	//Create the first job queue for all inter-file-server requests about existance of file/directory
	nJobQueue = 1;

	ListJobID[0] = SERVER_JOBID;	// the first queue is special. It is dedicated for inter-server communication.
	IO_Queue_List[0].IdxWorker = -1;
	IO_Queue_List[0].nQP = nFSServer - 1;

	FirstAV_Queue = 1;
	IdxLastQueue = 0;
}

int Create_A_Queue(int jobid)
{
	int idx;

	idx = FindFirstAvailableSpaceForQueue();
	if(idx > 0)	{	// Success in find a available queue for the new jobid
		ListJobID[idx] = jobid;
		IO_Queue_List[idx].IdxWorker = -1;
		IO_Queue_List[idx].nQP = 1;	// create the queue on first QP
		IO_Queue_List[idx].jobid = jobid;


		return idx;
	}
	else	{
		printf("ERROR> Fail to allocate an IO queue for job %d\nQuit\n", jobid);
		return -1;
	}
}

int Query_Jobid_In_Queue(int jobid)
{
	int i;

	for(i=0; i<=IdxLastQueue; i++)	{
		if(jobid == ListJobID[i])	{
			return i;
		}
	}
	return (-1);	// Does not exist!!!
}

int FindFirstAvailableSpaceForQueue(void)	// Search from FirstAV_Queue 
{
	int i, idx = -1, Done=0;

	if(FirstAV_Queue < 0)	{
		return FirstAV_Queue;
	}
	idx = FirstAV_Queue;
	if(FirstAV_Queue > IdxLastQueue)	{
		IdxLastQueue = FirstAV_Queue;
	}
	FirstAV_Queue = -1;

	for(i=idx+1; i<MAX_NUM_QUEUE; i++)	{
		if(ListJobID[i] == INVALID_JOBID)	{
			FirstAV_Queue = i;	// update FirstAV_Queue
			Done = 1;
			break;
		}
	}
	if(FirstAV_Queue < 0)	{
		printf("WARNING> All space for queues are used.\n");
	}

	return idx;
}

void Free_A_Queue(int idx)
{
	int i;

	ListJobID[idx] = INVALID_JOBID;
	if(idx < FirstAV_Queue)	{
		FirstAV_Queue = idx;
	}
	if(idx == IdxLastQueue)	{	// Need to update IdxLastQueue
		for(i=idx-1; i>=0; i--)	{
			if(ListJobID[idx] > 0)	{
				IdxLastQueue = i;
				break;
			}
		}
	}

	nJobQueue--;
}

