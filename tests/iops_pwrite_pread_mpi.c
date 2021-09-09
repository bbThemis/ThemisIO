#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include <sys/time.h>
#include <assert.h>
#include <mpi.h>

#define N_LETTER        (52)
#define T_WINDOW        (1.0)
#define OUT_OF_SPACE    (-111)


double T_Max=180.0;     // Max benchmark time in seconds
int world_rank;
int world_size;
char szName[64]="/myfs";

ssize_t read_all(int fd, void *buf, size_t count);
ssize_t write_all(int fd, const void *buf, size_t count);


ssize_t read_all(int fd, void *buf, size_t count)       // always read from the very beginning!!!
{
	ssize_t ret, nBytes=0;
	
	while (count != 0 && (ret = pread(fd, buf, count, nBytes)) != 0) {
		if (ret == -1) {
			if (errno == EINTR)
				continue;
			perror ("read");
			break;
		}
		nBytes += ret;
		count -= ret;
		buf += ret;
	}
	return nBytes;
}

ssize_t write_all(int fd, const void *buf, size_t count)        // always read from the very beginning!!!
{
	ssize_t ret, nBytes=0;
	void *p_buf;
	
	p_buf = (void *)buf;
	while (count != 0 && (ret = pwrite(fd, p_buf, count, nBytes)) != 0) {
		if (ret == -1) {
			if (errno == EINTR)     {
				continue;
			}
			else if (errno == ENOSPC)       {       // out of space. Quit immediately!!!
				return OUT_OF_SPACE;
			}
			
			perror ("write");
			break;
		}
		nBytes += ret;
		count -= ret;
		p_buf += ret;
	}
	return nBytes;
}
int main(int argc, char *argv[])
{
	long int i;
	static struct timeval tm1, tm2, tm0;
	unsigned long long int time;
	long int file_size, nBytesRead;
	struct stat file_stat;
	double t1, t2, d_time, d_time_OP, d_time_Accu=0.0;
	long int nIter=0, nIter_Sum, nIter_Sum_Save, nBytes_RW=0;
	char szNameLocal[256];
	int fd, nByste_RW=64*1024;
	char *pData=NULL;
	
	if(argc == 2)   {
		T_Max = atof(argv[1]);
	}
	else if(argc == 3)      {
		T_Max = atof(argv[1]);
		strcpy(szName, argv[2]);
	}
	else if(argc == 4)      {
		T_Max = atof(argv[1]);
		strcpy(szName, argv[2]);
		nByste_RW = atoi(argv[3]);
	}
	else    {
		printf("Usage: io_bench [t_max] [dir_path] [nBytes_RW]\n");
		exit(1);
	}
	
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
	
	if(world_rank < 10)     {
		sprintf(szNameLocal, "%s/test.0000000%d", szName, world_rank);
	}
	else if (world_rank < 100)      {
		sprintf(szNameLocal, "%s/test.000000%d", szName, world_rank);
	}
	else if (world_rank < 1000)     {
		sprintf(szNameLocal, "%s/test.00000%d", szName, world_rank);
	}
	else if (world_rank < 1000)     {
		sprintf(szNameLocal, "%s/test.0000%d", szName, world_rank);
	}
	else if (world_rank < 10000)    {
		sprintf(szNameLocal, "%s/test.000%d", szName, world_rank);
	}
	else if (world_rank < 100000)   {
		sprintf(szNameLocal, "%s/test.00%d", szName, world_rank);
	}
	else if (world_rank < 1000000)  {
		sprintf(szNameLocal, "%s/test.0%d", szName, world_rank);
	}
	else    {
		sprintf(szNameLocal, "%s/test.%d", szName, world_rank);
	}
	
	srand(world_rank*17 + 101);
	
	MPI_Barrier(MPI_COMM_WORLD);
	
	gettimeofday(&tm1, NULL);
	tm0.tv_sec = tm1.tv_sec;
	tm0.tv_usec = tm1.tv_usec;
	
	nIter = 0;
	nIter_Sum_Save = nIter_Sum = 0;
	
	pData = (char*)malloc(nByste_RW);
	assert(pData != NULL);
	
	fd = open(szNameLocal, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR);
	assert(fd >= 0);
	
	while(d_time_Accu < T_Max)   {
		//start benchmark work
		write_all(fd, pData, nByste_RW);
		read_all(fd, pData, nByste_RW);
		//end benchmark work
		
		nIter++;
		if(nIter % 1 == 0)    {
			gettimeofday(&tm2, NULL);
			d_time_OP = (tm2.tv_sec - tm1.tv_sec) + (tm2.tv_usec - tm1.tv_usec)*0.000001;
			if(d_time_OP >= T_WINDOW)       {
				tm1.tv_sec = tm2.tv_sec;
				tm1.tv_usec = tm2.tv_usec;
				MPI_Reduce(&nIter, &nIter_Sum, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
				gettimeofday(&tm2, NULL);
				d_time = (tm2.tv_sec - tm1.tv_sec)*1000000 + (tm2.tv_usec - tm1.tv_usec);
				d_time_Accu = (tm2.tv_sec - tm0.tv_sec) + (tm2.tv_usec - tm0.tv_usec)*0.000001;
				if(world_rank == 0)     printf("T = %ld    Bandwidth = %6.3lf GB/s    %6.3lf M IOPS     T_Reduce = %5.0lf us     nOP_Accu = %6.2lf\n",
					tm2.tv_sec, (nIter_Sum-nIter_Sum_Save)*0.000001*nByste_RW*2*0.001/d_time_OP, (nIter_Sum-nIter_Sum_Save)*0.000001/d_time_OP, d_time, nIter_Sum*0.000001);
//					d_time_Accu, (nIter_Sum-nIter_Sum_Save)*0.000001*nByste_RW*2*0.001/d_time_OP, (nIter_Sum-nIter_Sum_Save)*0.000001/d_time_OP, d_time, nIter_Sum*0.000001);
				tm1.tv_sec = tm2.tv_sec;
				tm1.tv_usec = tm2.tv_usec;
				nIter_Sum_Save = nIter_Sum;
			}
		}
	}
	
	close(fd);
	free(pData);
	
	MPI_Finalize();
	
	return 0;
}
