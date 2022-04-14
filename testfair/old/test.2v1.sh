#!/bin/bash

ppn=56
iosize=1m
filesize=10m

offset=1
nodes=2

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * $nodes)) -o $(($ppn * $offset)) \
   ../tests/themis_client.sh -u 101 -j $((1000*$nodes + 1)) -n $nodes \
   ./rw_speed -time=60 -iosize=$iosize -filesize=$filesize -tag=job1 &> rw_speed1.out &

offset=$(($offset + $nodes))
nodes=1

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * $nodes)) -o $(($ppn * $offset)) \
   ../tests/themis_client.sh -u 102 -j $((1000*$nodes + 2)) -n $nodes -s 15 \
   ./rw_speed -time=30 -iosize=$iosize -filesize=$filesize -tag=job2 &> rw_speed2.out &

wait
cat rw_speed1.out
cat rw_speed2.out

# sed 's/# //' < test.2v1.sh | grep ^rw_speed.job | head -2 | ./format_throughput_slices.py > test.job-fair.2v1.txt
# sed 's/# //' < test.2v1.sh | grep ^rw_speed.job | tail -2 | ./format_throughput_slices.py > test.size-fair.2v1.txt


# server job-fair mode 

# rw_speed.job1 time=2021-09-02:14:54:57 nn=1 np=56 user=871101 jobid=3481282 mbps=16101.540 mbps_rank_stddev=43.946 mbps_1sec_time_slices=(11549.0,20552.0,21056.0,20486.0,21574.0,21286.0,21190.0,21977.0,22478.0,20812.0,19925.0,20182.0,21696.0,21732.0,8892.0,9832.0,10566.0,10753.0,10979.0,10994.0,10633.0,10799.0,10865.0,10954.0,10797.0,10882.0,10833.0,11091.0,10881.0,10873.0,10612.0,10999.0,11108.0,10826.0,10770.0,10921.0,10797.0,10918.0,11083.0,10970.0,10780.0,10793.0,10821.0,16726.0,21551.0,21758.0,21913.0,21291.0,20684.0,21584.0,22023.0,21982.0,20961.0,21812.0,21426.0,21344.0,21724.0,22591.0,20852.0,21526.0,55.0)
# rw_speed.job2 time=2021-09-02:14:54:40 nn=2 np=112 user=871101 jobid=3481282 mbps=10782.954 mbps_rank_stddev=1.523 mbps_1sec_time_slices=(4133.0,9534.0,10196.0,10863.0,10864.0,10932.0,10988.0,10822.0,10858.0,10936.0,11002.0,10973.0,10825.0,10846.0,10954.0,10969.0,10752.0,10835.0,10953.0,10950.0,11039.0,10828.0,10915.0,10997.0,10885.0,10943.0,10986.0,10841.0,10811.0,10868.0,97.0)


# server output
# FairQueue::reportDecisionLog rank 0 time=130.00 1001:54434
# FairQueue::reportDecisionLog rank 0 time=135.00 1001:127021
# FairQueue::reportDecisionLog rank 0 time=140.00 1001:125134
# FairQueue::reportDecisionLog rank 0 time=145.00 1001:36599;2002:198;1001,2002:43314,43350
# FairQueue::reportDecisionLog rank 0 time=150.00 2002:245;1001,2002:65003,64997
# FairQueue::reportDecisionLog rank 0 time=155.00 2002:242;1001,2002:65112,65153
# FairQueue::reportDecisionLog rank 0 time=160.01 2002:245;1001,2002:65147,65041
# FairQueue::reportDecisionLog rank 0 time=165.01 2002:245;1001,2002:65094,65177
# FairQueue::reportDecisionLog rank 0 time=170.01 2002:228;1001,2002:65202,65059
# FairQueue::reportDecisionLog rank 0 time=175.01 1001:105188;2002:74;1001,2002:12762,12497
# FairQueue::reportDecisionLog rank 0 time=180.01 1001:128388
# FairQueue::reportDecisionLog rank 0 time=185.01 1001:129384
# FairQueue::reportDecisionLog rank 0 time=190.01 1001:64926

# server size-fair mode 

# rw_speed.job1 time=2021-09-02:15:02:29 nn=1 np=56 user=871101 jobid=3481282 mbps=14448.548 mbps_rank_stddev=39.138 mbps_1sec_time_slices=(9632.0,20255.0,21300.0,21841.0,21735.0,21392.0,21397.0,20885.0,21832.0,22430.0,22152.0,22108.0,21350.0,21334.0,6483.0,6645.0,7088.0,7281.0,7264.0,7197.0,7174.0,7236.0,7281.0,7322.0,7176.0,7406.0,7280.0,7218.0,7338.0,7276.0,7302.0,7287.0,7240.0,7105.0,7139.0,7115.0,7322.0,7185.0,7232.0,7219.0,7376.0,7031.0,7381.0,14182.0,22124.0,21543.0,22123.0,22143.0,22399.0,21947.0,21709.0,21818.0,22310.0,21857.0,21129.0,21169.0,21752.0,21700.0,21186.0,20975.0,52.0)
# rw_speed.job2 time=2021-09-02:15:02:13 nn=2 np=112 user=871101 jobid=3481282 mbps=14351.871 mbps_rank_stddev=1.919 mbps_1sec_time_slices=(5244.0,12851.0,13379.0,14452.0,14764.0,14310.0,14416.0,14619.0,14562.0,14523.0,14219.0,14748.0,14552.0,14579.0,14583.0,14761.0,14583.0,14426.0,14807.0,14387.0,14515.0,14390.0,14429.0,14763.0,14135.0,14484.0,14217.0,14341.0,14321.0,14832.0,96.0)

# server output
# FairQueue::reportDecisionLog rank 0 time=180.01 1001:9592
# FairQueue::reportDecisionLog rank 0 time=185.01 1001:127224
# FairQueue::reportDecisionLog rank 0 time=190.01 1001:130018
# FairQueue::reportDecisionLog rank 0 time=195.01 1001:81456;2002:28;1001,2002:14047,27699
# FairQueue::reportDecisionLog rank 0 time=105.01 1001:39471
# FairQueue::reportDecisionLog rank 0 time=200.01 2002:70;1001,2002:43035,85901
# FairQueue::reportDecisionLog rank 0 time=205.01 2002:80;1001,2002:43623,86856
# FairQueue::reportDecisionLog rank 0 time=210.01 2002:79;1001,2002:43569,87337
# FairQueue::reportDecisionLog rank 0 time=215.01 2002:64;1001,2002:43028,86718
# FairQueue::reportDecisionLog rank 0 time=220.01 2002:70;1001,2002:43367,86346
# FairQueue::reportDecisionLog rank 0 time=225.02 1001:59664;2002:40;1001,2002:23737,47364
# FairQueue::reportDecisionLog rank 0 time=230.02 1001:131911
# FairQueue::reportDecisionLog rank 0 time=235.02 1001:129845
# FairQueue::reportDecisionLog rank 0 time=240.02 1001:108648

# switch order so long-running job is the big one
# size-fair

# rw_speed time=2021-09-04:23:02:31 np=112 nn=2 sim_nn=2 rank0host=c202-002.frontera.tacc.utexas.edu -prefix=/myfs/rw_speed -iosize=1048576 -filesize=10485760 -time=60.0 -tag=job1
# rw_speed.job1 time=2021-09-04:23:03:30 nn=2 np=112 user=871101 jobid=3487190 mbps=18111.301 mbps_rank_stddev=3.962 mbps_1sec_time_slices=(8504.0,18345.0,20575.0,21697.0,22314.0,22195.0,22143.0,22120.0,22074.0,22148.0,21441.0,21176.0,21142.0,21326.0,20775.0,20756.0,20616.0,13790.0,14210.0,14271.0,14010.0,14683.0,14503.0,14863.0,14693.0,14482.0,14536.0,14742.0,14071.0,14595.0,14462.0,14686.0,14175.0,14743.0,14385.0,14789.0,14843.0,14245.0,14742.0,14387.0,14885.0,14931.0,14570.0,14384.0,14642.0,14669.0,18261.0,22483.0,22657.0,22305.0,22557.0,21974.0,22338.0,22386.0,22111.0,22644.0,22499.0,22211.0,22088.0,21926.0,96.0)
# rw_speed time=2021-09-04:23:02:47 np=56 nn=1 sim_nn=1 rank0host=c202-004.frontera.tacc.utexas.edu -prefix=/myfs/rw_speed -iosize=1048576 -filesize=10485760 -time=30.0 -tag=job2
# rw_speed.job2 time=2021-09-04:23:03:17 nn=1 np=56 user=871101 jobid=3487190 mbps=7244.552 mbps_rank_stddev=19.376 mbps_1sec_time_slices=(3663.0,7007.0,7308.0,7153.0,7134.0,7069.0,7315.0,7422.0,7099.0,7343.0,7289.0,7308.0,7087.0,7172.0,7309.0,7248.0,7307.0,7242.0,7461.0,7328.0,7238.0,7266.0,7218.0,7331.0,7343.0,7458.0,7297.0,7273.0,7426.0,7230.0,51.0)
