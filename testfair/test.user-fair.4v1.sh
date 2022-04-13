#!/bin/bash

# 3 jobs, 2 users
# user 1
#   job 1, 2 nodes
#   job 2, 2 nodes
# user 2
#   job 3, 1 node

ppn=56
iosize=1m
filesize=10m

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 2)) -o $(($ppn * 1)) \
   ../tests/themis_client.sh -u 101 -j 2001 -n 2 \
   ./rw_speed -time=60 -iosize=$iosize -filesize=$filesize -tag=job1 &> rw_speed1.out &

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 2)) -o $(($ppn * 3)) \
   ../tests/themis_client.sh -u 101 -j 2002 -n 2 -s 10 \
   ./rw_speed -time=40 -iosize=$iosize -filesize=$filesize -tag=job2 &> rw_speed2.out &

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 1)) -o $(($ppn * 5)) \
   ../tests/themis_client.sh -u 102 -j 1003 -n 1 -s 20 \
   ./rw_speed -time=20 -iosize=$iosize -filesize=$filesize -tag=job3 &> rw_speed3.out &

wait
cat rw_speed1.out
cat rw_speed2.out
cat rw_speed3.out

# sed 's/# //' < test.user-fair.4v1.sh | grep ^rw_speed.job | ./format_throughput_slices.py > test.user-fair.4v1.txt

# rw_speed.job1 time=2021-09-02:14:45:17 nn=2 np=112 user=101 jobid=2001 mbps=12636.909 mbps_rank_stddev=5.826 mbps_1sec_time_slices=(7626.0,22079.0,22453.0,22079.0,22573.0,22068.0,22368.0,22400.0,22400.0,21689.0,13798.0,9397.0,9391.0,9938.0,10058.0,10061.0,10422.0,10724.0,10738.0,10759.0,10983.0,9620.0,5233.0,5427.0,5471.0,5458.0,5430.0,5534.0,5560.0,5526.0,5397.0,5497.0,5414.0,5464.0,5544.0,5439.0,5460.0,5506.0,5393.0,5310.0,5425.0,6407.0,11183.0,9903.0,11083.0,11084.0,11098.0,10974.0,11036.0,12160.0,21251.0,21913.0,22106.0,22525.0,22121.0,22279.0,22405.0,22012.0,21436.0,20928.0,100.0)
# rw_speed.job2 time=2021-09-02:14:45:07 nn=2 np=112 user=101 jobid=2002 mbps=7805.111 mbps_rank_stddev=0.867 mbps_1sec_time_slices=(3818.0,9211.0,9157.0,9713.0,9878.0,9807.0,10214.0,10503.0,10562.0,10542.0,10806.0,10315.0,5160.0,5380.0,5424.0,5418.0,5362.0,5463.0,5521.0,5485.0,5378.0,5422.0,5388.0,5393.0,5492.0,5403.0,5407.0,5455.0,5374.0,5343.0,5303.0,5399.0,10966.0,9694.0,10866.0,10915.0,10905.0,10804.0,10800.0,10697.0,90.0)
# rw_speed.job3 time=2021-09-02:14:44:59 nn=1 np=56 user=102 jobid=1003 mbps=10777.394 mbps_rank_stddev=28.933 mbps_1sec_time_slices=(9921.0,10971.0,11031.0,10902.0,10860.0,10853.0,10991.0,10964.0,10984.0,10709.0,10761.0,10890.0,10821.0,10808.0,10642.0,10834.0,10916.0,10767.0,10834.0,9957.0,51.0)


# server output
# FairQueue::reportDecisionLog rank 0 time=20.00 101:115720
# FairQueue::reportDecisionLog rank 0 time=25.00 101:132929
# FairQueue::reportDecisionLog rank 0 time=30.00 101:114849
# FairQueue::reportDecisionLog rank 0 time=35.00 101:125026
# FairQueue::reportDecisionLog rank 0 time=40.00 101:47885;101,102:40152,40730
# FairQueue::reportDecisionLog rank 0 time=45.00 101:244;101,102:65331,65440
# FairQueue::reportDecisionLog rank 0 time=50.00 101:235;101,102:64882,64827
# FairQueue::reportDecisionLog rank 0 time=55.01 101:214;101,102:64581,64610
# FairQueue::reportDecisionLog rank 0 time=60.01 101:79809;101,102:23158,23308
# FairQueue::reportDecisionLog rank 0 time=65.01 101:131317
# FairQueue::reportDecisionLog rank 0 time=70.01 101:131205
# FairQueue::reportDecisionLog rank 0 time=75.01 101:130731
# FairQueue::reportDecisionLog rank 0 time=80.01 101:4349
