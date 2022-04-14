#!/bin/bash

ppn=56
iosize=1m

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 1)) -o $(($ppn * 1)) \
   ../tests/themis_client.sh -r 200 -u 101 -j 1001 -n 1 \
   ./rw_speed -time=60 -iosize=$iosize -filesize=100m -tag=job1 &> rw_speed1.out &

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 1)) -o $(($ppn * 2)) \
  ../tests/themis_client.sh -r 300 -u 102 -j 1002 -n 1 -s 15  \
  ./rw_speed -time=30 -iosize=$iosize -filesize=100m -tag=job2 &> rw_speed2.out &

wait
cat rw_speed1.out
cat rw_speed2.out