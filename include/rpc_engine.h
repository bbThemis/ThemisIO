#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_log.h>
#include <mercury_macros.h>
#include <pthread.h>


#ifndef RPC_ENGINE_H
#define RPC_ENGINE_H


#define ADDR_BUF_SIZE 64
#define LOCAL_ADDR_SIZE 32

extern CIO_QUEUE IO_Queue_List[MAX_NUM_QUEUE];

class RPC_ENGINE {
public:
    hg_context_t **hg_contexts;
    hg_class_t *hg_class;
    pthread_t* hg_progress_tids;
    int context_cnt;
    int max_num_qp;
    char local_addr[LOCAL_ADDR_SIZE];
    hg_bool_t listen;
    RPC_ENGINE(hg_bool_t listen, const char *local_addr, int context_cnt, int max_num_qp);
	~RPC_ENGINE(void);
    void
    hg_engine_init();
    void
    hg_engine_finalize();
    void
    hg_engine_print_self_addr(char buf[], hg_size_t buf_size);
    void
    hg_engine_addr_lookup(const char *name, hg_addr_t *addr);
    void
    hg_engine_addr_free(hg_addr_t addr);
    void
    hg_register_rpc();
    void
    hg_engine_create_handle(int context_id, hg_addr_t addr, hg_id_t id, hg_handle_t *handle);
    void 
    hg_init_memory();
    void 
    hg_free_memory();
};



extern bool hg_progress_shutdown_flag;



#endif /* EXAMPLE_RPC_ENGINE_H */