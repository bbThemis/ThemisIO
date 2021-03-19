#ifndef __IO_OPS
#define __IO_OPS

#include "io_queue.h"

void RW_Open(IO_CMD_MSG *pRF_Op_Msg);
void RW_Close(IO_CMD_MSG *pRF_Op_Msg);
void RW_Opendir(IO_CMD_MSG *pRF_Op_Msg);
void RW_Read(IO_CMD_MSG *pRF_Op_Msg);
void RW_Write(IO_CMD_MSG *pRF_Op_Msg);
void RW_PRead(IO_CMD_MSG *pRF_Op_Msg);
void RW_Seek(IO_CMD_MSG *pRF_Op_Msg);
void RW_Stat(IO_CMD_MSG *pRF_Op_Msg);
void RW_LStat(IO_CMD_MSG *pRF_Op_Msg);
void RW_FStat(IO_CMD_MSG *pRF_Op_Msg);
void RW_Dir_Exist(IO_CMD_MSG *pRF_Op_Msg);
void RW_Unlink(IO_CMD_MSG *pRF_Op_Msg);

void RW_Truncate(IO_CMD_MSG *pRF_Op_Msg);
void RW_Ftruncate(IO_CMD_MSG *pRF_Op_Msg);
void RW_Mkdir(IO_CMD_MSG *pRF_Op_Msg);
void RW_Remove_Dir(IO_CMD_MSG *pRF_Op_Msg);

void RW_Utimes(IO_CMD_MSG *pRF_Op_Msg);
void RW_Futimens(IO_CMD_MSG *pRF_Op_Msg);
void RW_Disconnect_QP(IO_CMD_MSG *pRF_Op_Msg);

void RW_Print_Mem(void);

#endif
