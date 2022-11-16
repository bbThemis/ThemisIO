#include "rpc_engine.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

#include <mercury_atomic.h>
#include <mercury_time.h>
#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_request.h>
#include <mercury_util.h>
#include <mercury_core_types.h>



bool hg_progress_shutdown_flag = false;
void * hg_progress_fn(void *arg);

RPC_ENGINE::RPC_ENGINE(hg_bool_t listen, const char *local_addr, int context_cnt, int max_num_qp) {
    this->max_num_qp = max_num_qp;
    this->context_cnt = context_cnt;
    this->listen = listen;
    strcpy(this->local_addr, local_addr);

}

RPC_ENGINE::~RPC_ENGINE() {
    hg_engine_finalize();
}

void
RPC_ENGINE::hg_engine_init()
{
    int ret;

    HG_Set_log_level("warning");
    /* boilerplate HG initialization steps */
    hg_class = HG_Init(local_addr, listen);
    assert(hg_class);
    
    hg_contexts = (hg_context_t **)malloc(context_cnt * sizeof(hg_context_t *));
    hg_progress_tids = (pthread_t *)malloc(context_cnt * sizeof(pthread_t));
    for(int i = 0; i < context_cnt; i++) {
        hg_contexts[i] = HG_Context_create_id(hg_class, i);
        assert(hg_contexts[i]);
    }
    for(int i = 0; i < context_cnt; i++) {
        /* start up thread to drive progress */
        ret = pthread_create(&hg_progress_tids[i], NULL, hg_progress_fn, (void *)&hg_contexts[i]);
        assert(ret == 0);
    }
    
    (void) ret;

    return;
}


void
RPC_ENGINE::hg_engine_finalize()
{
    int ret;

    /* tell progress thread to wrap things up */
    hg_progress_shutdown_flag = true;

    /* wait for it to shutdown cleanly */
    for(int i = 0; i < context_cnt; i++) {
        ret = pthread_join(hg_progress_tids[i], NULL);
        assert(ret == 0);
    }
    
    (void) ret;
    hg_free_memory();
    free(hg_contexts);
    free(hg_progress_tids);
    return;
}

/* dedicated thread function to drive Mercury progress */
void * hg_progress_fn(void *arg)
{
    hg_return_t ret;
    unsigned int actual_count;
    hg_context_t **hg_contexts = (hg_context_t **)arg;
    while (!hg_progress_shutdown_flag) {
        do {
            ret = HG_Trigger(*hg_contexts, 0, 1, &actual_count);
        } while (
            (ret == HG_SUCCESS) && actual_count && !hg_progress_shutdown_flag);

        if (!hg_progress_shutdown_flag)
            HG_Progress(*hg_contexts, 100);
    }
    
    return (NULL);
}


void
RPC_ENGINE::hg_engine_print_self_addr(char buf[], hg_size_t buf_size)
{
    hg_return_t ret;
    hg_addr_t addr;
    

    ret = HG_Addr_self(hg_class, &addr);
    assert(ret == HG_SUCCESS);
    (void) ret;

    ret = HG_Addr_to_string(hg_class, buf, &buf_size, addr);
    assert(ret == HG_SUCCESS);
    (void) ret;

    // printf("svr address string: \"%s\"\n", buf);

    ret = HG_Addr_free(hg_class, addr);
    assert(ret == HG_SUCCESS);
    (void) ret;

    return;
}

void
RPC_ENGINE::hg_engine_addr_lookup(const char *name, hg_addr_t *addr)
{
    hg_return_t ret;
    ret = HG_Addr_lookup2(hg_class, name, addr);
    assert(ret == HG_SUCCESS);
    (void) ret;

    return;
}

void
RPC_ENGINE::hg_engine_addr_free(hg_addr_t addr)
{
    hg_return_t ret;

    ret = HG_Addr_free(hg_class, addr);
    assert(ret == HG_SUCCESS);
    (void) ret;

    return;
}

void
RPC_ENGINE::hg_engine_create_handle(int context_id, hg_addr_t addr, hg_id_t id, hg_handle_t *handle)
{
    hg_return_t ret;

    ret = HG_Create(hg_contexts[context_id], addr, id, handle);
    assert(ret == HG_SUCCESS);
    (void) ret;

    return;
}

void
RPC_ENGINE::hg_register_rpc()
{
    // hg_test_rpc_null_id_g = MERCURY_REGISTER(
    //     pMercury_Data->hg_class, "hg_test_rpc_null", void, void, hg_test_rpc_null_cb);
    
}

void RPC_ENGINE::hg_init_memory() {
    
}

void RPC_ENGINE::hg_free_memory() {

}



// void clear_config() {
//     FILE *config = NULL;
//     hg_return_t ret;

//     config = fopen(MERCURY_CONFIG_FILE_NAME, "w");
//     fclose(config);
// }


// hg_return_t
// set_config(const char *addr_name, bool append)
// {
//     FILE *config = NULL;
//     hg_return_t ret = HG_SUCCESS;

//     config = fopen(MERCURY_CONFIG_FILE_NAME, append ? "a" : "w");
//     if(config == NULL) {
//         ret = HG_NOENTRY;
//         fprintf(stderr, "Could not open config file from: %s", MERCURY_CONFIG_FILE_NAME);
//     }

//     fprintf(config, "%s\n", addr_name);
//     fprintf(stdout, "set_config %s\n", addr_name);
//     fclose(config);
//     return ret;
// }
