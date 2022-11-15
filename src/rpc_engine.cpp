#include "rpc_engine.h"

#include <assert.h>
#include <stdio.h>
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
static void * hg_progress_fn(void *foo);
void
hg_engine_init(MERCURY_DATA* pMercury_Data, hg_bool_t listen, const char *local_addr)
{
    int ret;

    HG_Set_log_level("warning");

    /* boilerplate HG initialization steps */
    pMercury_Data->hg_class = HG_Init(local_addr, listen);
    assert(pMercury_Data->hg_class);

    pMercury_Data->hg_context = HG_Context_create(pMercury_Data->hg_class);
    assert(pMercury_Data->hg_context);

    /* start up thread to drive progress */
    ret = pthread_create(&pMercury_Data->hg_progress_tid, NULL, hg_progress_fn, pMercury_Data);
    assert(ret == 0);
    (void) ret;

    return;
}


void
hg_engine_finalize(MERCURY_DATA* pMercury_Data)
{
    int ret;

    /* tell progress thread to wrap things up */
    hg_progress_shutdown_flag = true;

    /* wait for it to shutdown cleanly */
    ret = pthread_join(pMercury_Data->hg_progress_tid, NULL);
    assert(ret == 0);
    (void) ret;

    return;
}

/* dedicated thread function to drive Mercury progress */
static void *
hg_progress_fn(void *arg)
{
    hg_return_t ret;
    unsigned int actual_count;
    MERCURY_DATA* pMercury_Data = (MERCURY_DATA*)arg;

    while (!hg_progress_shutdown_flag) {
        do {
            ret = HG_Trigger(pMercury_Data->hg_context, 0, 1, &actual_count);
        } while (
            (ret == HG_SUCCESS) && actual_count && !hg_progress_shutdown_flag);

        if (!hg_progress_shutdown_flag)
            HG_Progress(pMercury_Data->hg_context, 100);
    }

    return (NULL);
}


void
hg_engine_print_self_addr(MERCURY_DATA* pMercury_Data, char buf[], hg_size_t buf_size)
{
    hg_return_t ret;
    hg_addr_t addr;
    

    ret = HG_Addr_self(pMercury_Data->hg_class, &addr);
    assert(ret == HG_SUCCESS);
    (void) ret;

    ret = HG_Addr_to_string(pMercury_Data->hg_class, buf, &buf_size, addr);
    assert(ret == HG_SUCCESS);
    (void) ret;

    // printf("svr address string: \"%s\"\n", buf);

    ret = HG_Addr_free(pMercury_Data->hg_class, addr);
    assert(ret == HG_SUCCESS);
    (void) ret;

    return;
}

void
hg_engine_addr_lookup(MERCURY_DATA* pMercury_Data, const char *name, hg_addr_t *addr)
{
    hg_return_t ret;
    ret = HG_Addr_lookup2(pMercury_Data->hg_class, name, addr);
    assert(ret == HG_SUCCESS);
    (void) ret;

    return;
}

void
hg_engine_addr_free(MERCURY_DATA* pMercury_Data, hg_addr_t addr)
{
    hg_return_t ret;

    ret = HG_Addr_free(pMercury_Data->hg_class, addr);
    assert(ret == HG_SUCCESS);
    (void) ret;

    return;
}

void
hg_engine_create_handle(MERCURY_DATA* pMercury_Data, hg_addr_t addr, hg_id_t id, hg_handle_t *handle)
{
    hg_return_t ret;

    ret = HG_Create(pMercury_Data->hg_context, addr, id, handle);
    assert(ret == HG_SUCCESS);
    (void) ret;

    return;
}


void clear_config() {
    FILE *config = NULL;
    hg_return_t ret;

    config = fopen(MERCURY_CONFIG_FILE_NAME, "w");
    fclose(config);
}


hg_return_t
set_config(const char *addr_name, bool append)
{
    FILE *config = NULL;
    hg_return_t ret = HG_SUCCESS;

    config = fopen(MERCURY_CONFIG_FILE_NAME, append ? "a" : "w");
    if(config == NULL) {
        ret = HG_NOENTRY;
        fprintf(stderr, "Could not open config file from: %s", MERCURY_CONFIG_FILE_NAME);
    }

    fprintf(config, "%s\n", addr_name);
    fprintf(stdout, "set_config %s\n", addr_name);
    fclose(config);
    return ret;
}
