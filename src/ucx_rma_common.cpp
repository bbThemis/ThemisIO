#include "ucx_rma_common.h"

extern int mpi_rank;
void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status)
{
    printf("mpi_rank %d: error handling callback was invoked with status %d (%s)\n",
           mpi_rank, status, ucs_status_string(status));
}
