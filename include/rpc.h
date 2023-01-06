
#ifndef RPC_H
#define RPC_H
#include <mercury.h>
#include <mercury_core_types.h>
#include <mercury_macros.h>
#include <pthread.h>


extern hg_id_t hg_test_connect_id;


MERCURY_GEN_PROC(hg_test_connect_in_t, ((hg_bulk_t)(bulk_handle))((hg_size_t)(size)))
MERCURY_GEN_PROC(hg_test_connect_out_t, ((int32_t)(ret)))

/*
    In client side, in.bulk_handle == bulk_handle
    In server side, in.bulk_handle != bulk_handle
*/
struct rpc_test_connect_state {
    void *buffer;
    hg_bulk_t bulk_handle;
    hg_handle_t handle;
    hg_test_connect_in_t in;
};


hg_return_t test_connect_handler(hg_handle_t handle);
hg_return_t test_connect_handler_cb(const struct hg_cb_info *info);




#endif /* RPC_H */