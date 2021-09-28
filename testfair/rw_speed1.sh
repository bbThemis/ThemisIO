#!/bin/bash

# run with  IBRUN_TASKS_PER_NODE=4 ibrun -n 4 -o 4 ./rw_speed1.sh

export LD_PRELOAD=/home1/06333/aroraish/TBD/client/wrapper.so
export MYFS_CONF=/home1/06333/aroraish/TBD/myfs.param

export THEMIS_FAKE_JOBID=8001
export THEMIS_FAKE_NNODES=8
export THEMIS_FAKE_USERID=101

./rw_speed -time=60 -iosize=1m -filesize=100m -tag=job1 "$@"

