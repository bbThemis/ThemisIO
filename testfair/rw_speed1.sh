#!/bin/bash

# run with  IBRUN_TASKS_PER_NODE=4 ibrun -n 4 ./rw_speed1.sh

export LD_PRELOAD=/home1/07811/edk/ThemisIO/client/wrapper.so
export MYFS_CONF=/home1/07811/edk/ThemisIO/myfs.param

export THEMIS_FAKE_JOBID=1001
export THEMIS_FAKE_NNODES=10
export THEMIS_FAKE_USERID=101

./rw_speed -time=10 -iosize=1m -filesize=1g -tag=job1 "$@"

