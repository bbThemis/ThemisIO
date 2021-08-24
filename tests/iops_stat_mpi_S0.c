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

double T_Max=180.0;     // Max benchmark time in seconds
//int N_ITER=2000000;
int world_rank;
int world_size;
char szLetters[52];
char szName[64]="/myfs/";

int main(int argc, char *argv[])
{
	long int i;
	static struct timeval tm1, tm2, tm0;
	unsigned long long int time;
	long int file_size, nBytesRead;
	struct stat file_stat;
	double t1, t2, d_time, d_time_OP, d_time_Accu=0.0;
	long int nIter=0, nIter_Sum, nIter_Sum_Save;
	
	if(argc == 2)   {
		T_Max = atof(argv[1]);
	}
	else if(argc == 3)      {
		T_Max = atof(argv[1]);
		strcpy(szName, argv[2]);
	}
	else    {
		printf("Usage: io_bench [t_max] [dir_path]\n");
		exit(1);
	}
	
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
	
//	N_ITER /= world_size;
	
	for(i=0; i<26; i++)     {
		szLetters[i] = 'a' + i;
	}
	for(i=26; i<52; i++)    {
		szLetters[i] = 'A' + (i-26);
	}
	srand(world_rank*17 + 101);
	
	MPI_Barrier(MPI_COMM_WORLD);
	
	gettimeofday(&tm1, NULL);
	tm0.tv_sec = tm1.tv_sec;
	tm0.tv_usec = tm1.tv_usec;
	
	nIter = 0;
	nIter_Sum_Save = nIter_Sum = 0;
	while(d_time_Accu < T_Max)   {
		//start benchmark work
		for(i=6; i<26; i++)     {
			szName[i] = szLetters[rand()%52];
		}
//		szName[i] = 0;
		strcpy(szName+i, "_S0");
		stat(szName, &file_stat);
		//end benchmark work
		
		nIter++;
		if(nIter % 100 == 0)    {
			gettimeofday(&tm2, NULL);
			d_time_OP = (tm2.tv_sec - tm1.tv_sec) + (tm2.tv_usec - tm1.tv_usec)*0.000001;
			if(d_time_OP >= T_WINDOW)       {
				tm1.tv_sec = tm2.tv_sec;
				tm1.tv_usec = tm2.tv_usec;
				MPI_Reduce(&nIter, &nIter_Sum, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
				gettimeofday(&tm2, NULL);
				d_time = (tm2.tv_sec - tm1.tv_sec)*1000000 + (tm2.tv_usec - tm1.tv_usec);
				d_time_Accu = (tm2.tv_sec - tm0.tv_sec) + (tm2.tv_usec - tm0.tv_usec)*0.000001;
				if(world_rank == 0)     printf("T = %5.1lf    %6.3lf M IOPS     T_Reduce = %5.0lf us     nOP_Accu = %6.2lf\n", d_time_Accu, (nIter_Sum-nIter_Sum_Save)*0.000001/T_WINDOW, d_time, nIter_Sum*0.000001);
				tm1.tv_sec = tm2.tv_sec;
				tm1.tv_usec = tm2.tv_usec;
				nIter_Sum_Save = nIter_Sum;
			}
		}
	}
	
	MPI_Finalize();
	
	return 0;
}

