#!/bin/bash

ppn=56
iosize=1m

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 2)) -o $(($ppn * 1)) \
   ../tests/themis_client.sh -u 101 -j 1001 -n 1 \
   ./rw_speed -time=60 -iosize=$iosize -filesize=100m -tag=job1
