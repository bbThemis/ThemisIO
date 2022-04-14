#!/bin/bash

ppn=56
iosize=1m
filesize=10m

offset=1
nodes=1

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * $nodes)) -o $(($ppn * $offset)) \
   ../tests/themis_client.sh -u 101 -j $((1000*$nodes + 1)) -n $nodes \
   ./rw_speed -time=60 -iosize=$iosize -filesize=$filesize -tag=job1 &> rw_speed1.out &

offset=$(($offset + $nodes))
nodes=4

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * $nodes)) -o $(($ppn * $offset)) \
   ../tests/themis_client.sh -u 102 -j $((1000*$nodes + 2)) -n $nodes -s 15 \
   ./rw_speed -time=30 -iosize=$iosize -filesize=$filesize -tag=job2 &> rw_speed2.out &

wait
cat rw_speed1.out
cat rw_speed2.out

# sed 's/# //' < test.4v1.sh | grep ^rw_speed.job | head -2 | ./format_throughput_slices.py > test.job-fair.4v1.txt
# sed 's/# //' < test.4v1.sh | grep ^rw_speed.job | tail -2 | ./format_throughput_slices.py > test.size-fair.4v1.txt

# server in job-fair mode

# rw_speed.job1 time=2021-09-02:14:57:23 nn=1 np=56 user=871101 jobid=3481282 mbps=16136.317 mbps_rank_stddev=44.072 mbps_1sec_time_slices=(10096.0,19626.0,21091.0,22028.0,22139.0,22077.0,22119.0,22462.0,21718.0,21370.0,20741.0,22449.0,22163.0,21306.0,20528.0,8769.0,9059.0,9439.0,10285.0,9515.0,9747.0,10197.0,10467.0,10648.0,9900.0,9839.0,10454.0,10371.0,10755.0,10544.0,10560.0,10499.0,10580.0,10533.0,10671.0,10861.0,10768.0,11024.0,10973.0,10907.0,10810.0,10938.0,11052.0,13833.0,22098.0,21345.0,22418.0,22683.0,21730.0,21141.0,21673.0,20790.0,21038.0,21100.0,21941.0,20932.0,20845.0,21638.0,22072.0,21903.0,53.0)
# rw_speed.job2 time=2021-09-02:14:57:07 nn=4 np=224 user=871101 jobid=3481282 mbps=10364.526 mbps_rank_stddev=0.541 mbps_1sec_time_slices=(0.0,4707.0,8646.0,9674.0,10117.0,10130.0,9430.0,10195.0,10303.0,10562.0,10292.0,9837.0,10160.0,10409.0,10557.0,10545.0,10690.0,10467.0,10672.0,10676.0,10728.0,10807.0,10768.0,10979.0,10945.0,11058.0,10798.0,10758.0,10875.0,10965.0,193.0)


# server output

# FairQueue::reportDecisionLog rank 0 time=275.01 1001:15028
# FairQueue::reportDecisionLog rank 0 time=280.01 1001:128613
# FairQueue::reportDecisionLog rank 0 time=285.01 1001:129776
# FairQueue::reportDecisionLog rank 0 time=290.01 1001:102145;4002:45;1001,4002:10166,10039
# FairQueue::reportDecisionLog rank 0 time=295.01 4002:253;1001,4002:57858,58446
# FairQueue::reportDecisionLog rank 0 time=300.01 4002:238;1001,4002:61117,60981
# FairQueue::reportDecisionLog rank 0 time=305.01 4002:246;1001,4002:62989,62460
# FairQueue::reportDecisionLog rank 0 time=310.01 4002:230;1001,4002:63757,63961
# FairQueue::reportDecisionLog rank 0 time=315.01 4002:252;1001,4002:65172,65041
# FairQueue::reportDecisionLog rank 0 time=320.01 1001:59499;4002:191;1001,4002:35884,35368
# FairQueue::reportDecisionLog rank 0 time=325.01 1001:131223
# FairQueue::reportDecisionLog rank 0 time=330.01 1001:126665
# FairQueue::reportDecisionLog rank 0 time=335.01 1001:104027


# server in size-fair mode

# rw_speed.job1 time=2021-09-02:15:06:31 nn=1 np=56 user=871101 jobid=3481282 mbps=13217.148 mbps_rank_stddev=35.136 mbps_1sec_time_slices=(8831.0,20016.0,21686.0,21610.0,22268.0,22305.0,22268.0,21986.0,22442.0,22145.0,22169.0,22443.0,22871.0,22023.0,19123.0,3589.0,3819.0,3820.0,3859.0,3826.0,3622.0,3723.0,3951.0,4151.0,4203.0,4358.0,4325.0,4338.0,4365.0,4357.0,4275.0,4448.0,4322.0,4355.0,4396.0,4332.0,4394.0,4276.0,4225.0,4414.0,4357.0,4446.0,4445.0,6548.0,21585.0,21592.0,20825.0,22546.0,22504.0,22141.0,21482.0,21402.0,22137.0,22535.0,22189.0,21759.0,21125.0,21697.0,21427.0,21810.0,53.0)
# rw_speed.job2 time=2021-09-02:15:06:15 nn=4 np=224 user=871101 jobid=3481282 mbps=16665.366 mbps_rank_stddev=2.220 mbps_1sec_time_slices=(0.0,13302.0,15276.0,15384.0,15486.0,15129.0,14630.0,15019.0,15932.0,16449.0,16782.0,17393.0,17450.0,17275.0,17383.0,17243.0,17647.0,17103.0,17426.0,17583.0,17327.0,17269.0,17223.0,17277.0,17453.0,17432.0,17315.0,17404.0,17215.0,17561.0,191.0)

# re-run with first job as the big one, size-fair

# rw_speed time=2021-09-04:23:08:43 np=224 nn=4 sim_nn=4 rank0host=c202-002.frontera.tacc.utexas.edu -prefix=/myfs/rw_speed -iosize=1048576 -filesize=10485760 -time=60.0 -tag=job1
# rw_speed.job1 time=2021-09-04:23:09:41 nn=4 np=224 user=871101 jobid=3487190 mbps=19147.123 mbps_rank_stddev=0.943 mbps_1sec_time_slices=(0.0,6700.0,16074.0,17158.0,18542.0,19446.0,21435.0,21525.0,21488.0,21844.0,21660.0,21300.0,20962.0,21061.0,21439.0,21719.0,21821.0,16771.0,16887.0,17964.0,17580.0,16984.0,17033.0,17008.0,17204.0,17250.0,17316.0,17172.0,17119.0,17328.0,17114.0,17145.0,17247.0,17072.0,17362.0,17494.0,17356.0,17701.0,17643.0,17588.0,17487.0,17610.0,17567.0,17411.0,17632.0,17579.0,18481.0,22188.0,22194.0,21853.0,21783.0,21984.0,21862.0,22053.0,22144.0,21835.0,21905.0,21956.0,21905.0,21936.0,201.0)
# rw_speed time=2021-09-04:23:08:58 np=56 nn=1 sim_nn=1 rank0host=c202-006.frontera.tacc.utexas.edu -prefix=/myfs/rw_speed -iosize=1048576 -filesize=10485760 -time=30.0 -tag=job2
# rw_speed.job2 time=2021-09-04:23:09:28 nn=1 np=56 user=871101 jobid=3487190 mbps=4342.118 mbps_rank_stddev=11.411 mbps_1sec_time_slices=(2469.0,4144.0,4489.0,4503.0,4296.0,4362.0,4260.0,4185.0,4310.0,4378.0,4401.0,4419.0,4250.0,4313.0,4179.0,4216.0,4411.0,4417.0,4309.0,4363.0,4398.0,4357.0,4337.0,4435.0,4346.0,4419.0,4388.0,4508.0,4470.0,4482.0,48.0)
