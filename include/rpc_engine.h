#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_log.h>
#include <mercury_macros.h>
#include <pthread.h>


#ifndef RPC_ENGINE_H
#define RPC_ENGINE_H

typedef	struct	{
	hg_context_t *hg_context;
    hg_class_t *hg_class;
    pthread_t hg_progress_tid;
}MERCURY_DATA;

#define ADDR_BUF_SIZE 64
#define ADDR_TOTAL_SIZE 96
#define MERCURY_CONFIG_FILE_NAME "myport.cfg"

void
hg_engine_init(MERCURY_DATA* pMercury_Data, hg_bool_t listen, const char *local_addr);
void
hg_engine_finalize(MERCURY_DATA* pMercury_Data);
void
hg_engine_print_self_addr(MERCURY_DATA* pMercury_Data, char buf[], hg_size_t buf_size);
void
hg_engine_addr_lookup(MERCURY_DATA* pMercury_Data, const char *name, hg_addr_t *addr);
void
hg_engine_addr_free(MERCURY_DATA* pMercury_Data, hg_addr_t addr);
void
hg_engine_create_handle(MERCURY_DATA* pMercury_Data, hg_addr_t addr, hg_id_t id, hg_handle_t *handle);

void clear_config();
hg_return_t
set_config(const char *addr_name, bool append);


extern bool hg_progress_shutdown_flag;



#endif /* EXAMPLE_RPC_ENGINE_H */