#include "rpc.h"
#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <assert.h>

hg_id_t hg_test_connect_id;



/* callback/handler triggered upon receipt of rpc bulk write request */
hg_return_t test_connect_handler(hg_handle_t handle) {
    fprintf(stdout, "server calls test_connect_handler\n");
    hg_return_t ret;
    struct rpc_test_connect_state *rpc_test_connect_state_p;
    const struct hg_info *hgi;

    /* set up state structure */
    rpc_test_connect_state_p = (rpc_test_connect_state *)malloc(sizeof(*rpc_test_connect_state_p));
    assert(rpc_test_connect_state_p);

    rpc_test_connect_state_p->handle = handle;
    /* decode input */
    ret = HG_Get_input(handle, &rpc_test_connect_state_p->in);
    assert(ret == HG_SUCCESS);
    // fprintf(stdout, "in.offset=%d in.size=%d\n", rpc_write_state_p->in.offset, rpc_write_state_p->in.size);
    /* This includes allocating a target buffer for bulk transfer */
    // TODO: Get target buffer using lustre apis
    rpc_test_connect_state_p->buffer = malloc(rpc_test_connect_state_p->in.size);
    assert(rpc_test_connect_state_p->buffer);

    /* register local target buffer for bulk access */
    hgi = HG_Get_info(handle);
    assert(hgi);
    ret = HG_Bulk_create(hgi->hg_class, 1, &rpc_test_connect_state_p->buffer,
        &rpc_test_connect_state_p->in.size, HG_BULK_WRITE_ONLY,
        &rpc_test_connect_state_p->bulk_handle);
    assert(ret == 0);

    /* initiate bulk transfer from client to server */
    ret = HG_Bulk_transfer(hgi->context, test_connect_handler_cb, rpc_test_connect_state_p,
        HG_BULK_PULL, hgi->addr, rpc_test_connect_state_p->in.bulk_handle, 0,
        rpc_test_connect_state_p->bulk_handle, 0, rpc_test_connect_state_p->in.size, HG_OP_ID_IGNORE);
    assert(ret == HG_SUCCESS);
    return HG_SUCCESS;
}

hg_return_t
test_connect_handler_cb(const struct hg_cb_info *info)
{
    
    struct rpc_test_connect_state *rpc_test_connect_state_p = (rpc_test_connect_state *)info->arg;
    hg_return_t ret;

    assert(info->ret == HG_SUCCESS);
    
    // TEST USE
    fprintf(stdout, "rpc_test_connect_state_p buffer: %s strlen: %d\n", rpc_test_connect_state_p->buffer, strlen((char*)rpc_test_connect_state_p->buffer));
    assert(strcmp((char*)rpc_test_connect_state_p->buffer, "Hello world!\n") == 0);
    hg_test_connect_out_t out;
    out.ret = rpc_test_connect_state_p->in.size;
    // fprintf(stdout, "server calls hg_bulk_write_handler_cb\n");
    // assert(rpc_write_state_p->handle->core_handle);
    ret = HG_Respond(rpc_test_connect_state_p->handle, NULL, NULL, &out);
    // fprintf(stdout, "out.ret=%d, in.size=%d\n", out.ret, rpc_write_state_p->in.size);
    assert(ret == HG_SUCCESS);
    (void) ret;
    
    HG_Bulk_free(rpc_test_connect_state_p->bulk_handle);
    // HG_Bulk_free(rpc_write_state_p->in.bulk_handle);

    HG_Free_input(rpc_test_connect_state_p->handle, &rpc_test_connect_state_p->in);
    HG_Destroy(rpc_test_connect_state_p->handle);
    // TEST USE
    free(rpc_test_connect_state_p->buffer);
    free(rpc_test_connect_state_p);

    return HG_SUCCESS;
}



void bulk_read() {}

hg_return_t bulk_read_cb(const struct hg_cb_info *info) {
    return HG_SUCCESS;
}


hg_return_t hg_bulk_read_handler(hg_handle_t handle) {
    return HG_SUCCESS;
}




