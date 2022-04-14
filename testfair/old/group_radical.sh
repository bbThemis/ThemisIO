#!/bin/bash

# run with  IBRUN_TASKS_PER_NODE=4 ibrun -n 4 -o 4 ./rw_speed1.sh

export LD_PRELOAD=/home1/06333/aroraish/trial/ThemisIO/wrapper.so
export MYFS_CONF=/home1/06333/aroraish/trial/ThemisIO/myfs.param

export THEMIS_FAKE_JOBID=1001
export THEMIS_FAKE_NNODES=1
export THEMIS_FAKE_USERID=101
export THEMIS_FAKE_GROUPID=70

ppn=56
offset=1
nodes=1

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * $nodes)) -o $(($ppn * $offset)) \
    ./rw_speed -time=120 -iosize=1m -filesize=100m -tag=job1  &> rw_speed1.out &

sleep 15

offset=$(($offset + $nodes))
nodes=2

export THEMIS_FAKE_JOBID=2002
export THEMIS_FAKE_NNODES=2
export THEMIS_FAKE_USERID=102
export THEMIS_FAKE_GROUPID=71

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * $nodes)) -o $(($ppn * $offset)) \
    ./rw_speed -time=60 -iosize=1m -filesize=100m -tag=job2  &> rw_speed2.out &

offset=$(($offset + $nodes))
nodes=3

export THEMIS_FAKE_JOBID=4003
export THEMIS_FAKE_NNODES=3
export THEMIS_FAKE_USERID=102
export THEMIS_FAKE_GROUPID=71

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * $nodes)) -o $(($ppn * $offset)) \
    ./rw_speed -time=60 -iosize=1m -filesize=100m -tag=job3  &> rw_speed3.out &

offset=$(($offset + $nodes))
nodes=2

export THEMIS_FAKE_JOBID=6004
export THEMIS_FAKE_NNODES=2
export THEMIS_FAKE_USERID=102
export THEMIS_FAKE_GROUPID=71

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * $nodes)) -o $(($ppn * $offset)) \
    ./rw_speed -time=60 -iosize=1m -filesize=100m -tag=job4  &> rw_speed4.out &

offset=$(($offset + $nodes))
nodes=3

export THEMIS_FAKE_JOBID=3005
export THEMIS_FAKE_NNODES=3
export THEMIS_FAKE_USERID=103
export THEMIS_FAKE_GROUPID=71

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * $nodes)) -o $(($ppn * $offset)) \
    ./rw_speed -time=60 -iosize=1m -filesize=100m -tag=job5  &> rw_speed5.out &

offset=$(($offset + $nodes))
nodes=2

export THEMIS_FAKE_JOBID=7006
export THEMIS_FAKE_NNODES=2
export THEMIS_FAKE_USERID=103
export THEMIS_FAKE_GROUPID=71

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * $nodes)) -o $(($ppn * $offset)) \
    ./rw_speed -time=60 -iosize=1m -filesize=100m -tag=job6  &> rw_speed6.out &

offset=$(($offset + $nodes))
nodes=1

export THEMIS_FAKE_JOBID=5007
export THEMIS_FAKE_NNODES=1
export THEMIS_FAKE_USERID=104
export THEMIS_FAKE_GROUPID=71

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * $nodes)) -o $(($ppn * $offset)) \
    ./rw_speed -time=60 -iosize=1m -filesize=100m -tag=job7  &> rw_speed7.out &

offset=$(($offset + $nodes))
nodes=2

export THEMIS_FAKE_JOBID=2008
export THEMIS_FAKE_NNODES=2
export THEMIS_FAKE_USERID=104
export THEMIS_FAKE_GROUPID=71

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * $nodes)) -o $(($ppn * $offset)) \
    ./rw_speed -time=60 -iosize=1m -filesize=100m -tag=job8  &> rw_speed8.out &



wait
cat rw_speed1.out
cat rw_speed2.out
cat rw_speed3.out
cat rw_speed4.out
cat rw_speed5.out
cat rw_speed6.out
cat rw_speed7.out
cat rw_speed8.out
