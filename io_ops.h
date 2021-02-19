#ifndef __IO_OPS
#define __IO_OPS

#include "io_queue.h"

//#define SIZE_FUNC_RETURN_BUFF	(64*1024)

void RW_Open(IO_CMD_MSG *pOP_Msg);
void RW_Close(IO_CMD_MSG *pOP_Msg);
void RW_Opendir(IO_CMD_MSG *pOP_Msg);
void RW_Read(IO_CMD_MSG *pOP_Msg);
void RW_PRead(IO_CMD_MSG *pOP_Msg);
void RW_Seek(IO_CMD_MSG *pOP_Msg);
void RW_Stat(IO_CMD_MSG *pOP_Msg);
void RW_LStat(IO_CMD_MSG *pOP_Msg);
void RW_FStat(IO_CMD_MSG *pOP_Msg);
void RW_Dir_Exist(IO_CMD_MSG *pOP_Msg);

#endif
