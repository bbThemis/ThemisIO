#!/bin/bash

# run with  IBRUN_TASKS_PER_NODE=4 ibrun -n 4 -o 4 ./rw_speed1.sh

export LD_PRELOAD=/home1/06333/aroraish/trial/ThemisIO/wrapper.so
export MYFS_CONF=/home1/06333/aroraish/trial/ThemisIO/myfs.param

export THEMIS_FAKE_JOBID=1001
export THEMIS_FAKE_NNODES=1
export THEMIS_FAKE_USERID=101
export TOKEN_RATE=200

ppn=56

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 1)) -o $(($ppn * 1)) \
    ./rw_speed -time=60 -iosize=1m -filesize=100m -tag=job1 

#sleep 15

#export THEMIS_FAKE_JOBID=1002
#export THEMIS_FAKE_NNODES=1
#export THEMIS_FAKE_USERID=101

#IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 1)) -o $(($ppn * 2)) \
#    ./rw_speed -time=30 -iosize=1m -filesize=100m -tag=job2  &> rw_speed2.out &

wait
cat rw_speed1_a.out
#cat rw_speed2.out
