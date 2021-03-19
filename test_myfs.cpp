#include "test_myfs.h"

char szFilePath[256]="test_data";


static void Create_All_Dir(void)
{
	FILE *fIn;
	char *ReadLine, szLine[256], szCmd[256], szName[256];
	int nLen, i;

	sprintf(szName, "%s/dir.list", szFilePath);
	fIn = fopen(szName, "r");
	if(fIn == NULL)	{
		printf("Warning> Fail to open file: %s\nQuit.\n", szName);
		return;
	}
	while(1)	{
		if(feof(fIn))	{
			break;
		}
		ReadLine = fgets(szLine, 256, fIn);
		if(ReadLine == NULL)	{
			break;
		}
		nLen = strlen(szLine);
		if(szLine[nLen-1] == 0xA)	{
			szLine[nLen-1] = 0;
			nLen--;
		}
		if(szLine[nLen-2] == 0xD)	{
			szLine[nLen-2] = 0;
			nLen--;
		}

		sprintf(szCmd, "mkdir -p %s/%s &> /dev/null", szRoot, szLine);
		system(szCmd);
	}
	fclose(fIn);

	sprintf(szName, "%s/rw", szRoot);
	strcpy(szRW_Home_Dir, szName);
	mkdir(szName, S_IRWXU);

	sprintf(szRW_Data_Dir, "%s/rwdata", szRoot);	// the dir that holds files
	mkdir(szRW_Data_Dir, S_IRWXU);

	for(i=0; i<100; i++)	{
		if(i<10)	{
			sprintf(szName, "%s/rwdata/_0%d", szRoot, i);
			if(mkdir(szName, S_IRWXU))	{
				if(errno != EEXIST)	{
					printf("Error in creating directory: %s errno = %d\nQuit\n", szName, errno);
					exit(1);
				}
			}
		}
		else	{
			sprintf(szName, "%s/rwdata/_%d", szRoot, i);
			if(mkdir(szName, S_IRWXU))	{
				if(errno != EEXIST)	{
					printf("Error in creating directory: %s errno = %d\nQuit\n", szName, errno);
					exit(1);
				}
			}
		}
	}

}

