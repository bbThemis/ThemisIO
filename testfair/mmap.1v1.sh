#!/bin/bash

ppn=56
iosize=1m

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 1)) -o $(($ppn * 1)) \
   ../tests/themis_client.sh -u 101 -j 1001 -n 1 \
   ./mmap_test -time=60 -iosize=$iosize -filesize=100m -tag=job1 &> mmap_speed1.out &

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 1)) -o $(($ppn * 2)) \
   ../tests/themis_client.sh -u 102 -j 1002 -n 1 -s 15 \
   ./mmap_test -time=30 -iosize=$iosize -filesize=100m -tag=job2 &> mmap_speed2.out &

wait
cat mmap_speed1.out
cat mmap_speed2.out