#!/bin/bash

ppn=1
iosize=5m

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 1)) -o $(($ppn * 1)) \
   ../tests/themis_client.sh -u 101 -j 1001 -n 1 \
   ./rw_speed_write -time=15 -iosize=$iosize -filesize=10m -tag=job1 &> rw_speed1.write.out &
wait
cat rw_speed1.write.out
 
IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 1)) -o $(($ppn * 1)) \
   ../tests/themis_client.sh -u 101 -j 1001 -n 1 \
   ./rw_speed_read -time=15 -iosize=$iosize -filesize=10m -tag=job1 &> rw_speed1.read.out &


wait
cat rw_speed1.read.out