#!/bin/bash

# export LD_PRELOAD=/opt/apps/gcc/8.3.0/lib64/libasan.so
# export ASAN_OPTIONS=verify_asan_link_order=0

# disables spinlocks in Infiniband code
export MLX5_SINGLE_THREADED=0

./server "$@"
