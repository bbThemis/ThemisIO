#!/bin/bash

ppn=56
iosize=1m

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 1)) -o $(($ppn * 1)) \
   ../tests/themis_client.sh -u 101 -j 1001 -n 1 -r 200 \
   ./rw_speed -time=40 -iosize=$iosize -filesize=100m -tag=job1 &> rw_speed1.out &

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 1)) -o $(($ppn * 2)) \
  ../tests/themis_client.sh -u 102 -j 1002 -n 1  \
  ./rw_speed -time=40 -iosize=$iosize -filesize=100m -tag=job2 &> rw_speed2.out &
  
IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 1)) -o $(($ppn * 3)) \
  ../tests/themis_client.sh -u 103 -j 1003 -n 1  \
  ./rw_speed -time=40 -iosize=$iosize -filesize=100m -tag=job3 &> rw_speed3.out &

IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 1)) -o $(($ppn * 4)) \
  ../tests/themis_client.sh -u 104 -j 1004 -n 1  \
  ./rw_speed -time=60 -iosize=$iosize -filesize=100m -tag=job4 &> rw_speed4.out &


wait
cat rw_speed1.out
cat rw_speed2.out
cat rw_speed3.out
cat rw_speed4.out

# server in job-fair mode

# rw_speed.job1 time=2021-09-02:12:42:12 nn=1 np=56 user=871101 jobid=3480910 mbps=15410.850 mbps_rank_stddev=47.901 mbps_1sec_time_slices=(7486.0,17167.0,15286.0,17413.0,17756.0,14309.0,17225.0,17917.0,18998.0,18509.0,18977.0,20335.0,19278.0,20043.0,19687.0,16660.0,9466.0,10809.0,9633.0,10061.0,10955.0,10176.0,10814.0,10586.0,10768.0,11123.0,11037.0,10580.0,11068.0,10681.0,11178.0,10871.0,10671.0,10921.0,10433.0,10366.0,11151.0,10492.0,10354.0,10128.0,10324.0,10198.0,10467.0,10540.0,10296.0,19768.0,22606.0,22824.0,22687.0,22802.0,22691.0,22799.0,22759.0,22676.0,22633.0,22635.0,22230.0,22363.0,22143.0,22117.0,54.0)
# rw_speed.job2 time=2021-09-02:12:41:57 nn=1 np=56 user=871101 jobid=3480910 mbps=10492.657 mbps_rank_stddev=27.079 mbps_1sec_time_slices=(4175.0,9672.0,10634.0,9412.0,10455.0,10701.0,10341.0,10865.0,10530.0,10784.0,11020.0,10835.0,10754.0,11306.0,10608.0,11234.0,11005.0,10478.0,10792.0,10200.0,10492.0,10811.0,10321.0,10583.0,10334.0,10170.0,10477.0,10249.0,10452.0,10330.0,56.0)


# server in size-fair mode
# rw_speed.job1 time=2021-09-02:15:00:12 nn=1 np=56 user=871101 jobid=3481282 mbps=14855.129 mbps_rank_stddev=49.017 mbps_1sec_time_slices=(6321.0,18231.0,15051.0,17560.0,15693.0,16884.0,17224.0,17731.0,18892.0,17547.0,19449.0,19396.0,18082.0,19358.0,19554.0,5977.0,7722.0,9965.0,10578.0,10723.0,10257.0,10271.0,10636.0,10275.0,10723.0,10966.0,9464.0,10645.0,10799.0,9999.0,10426.0,10227.0,10355.0,10531.0,10360.0,10532.0,10457.0,9946.0,10236.0,10661.0,10695.0,10087.0,10870.0,11001.0,13869.0,20849.0,20761.0,20406.0,21152.0,20727.0,21226.0,21660.0,20975.0,21829.0,21519.0,21890.0,21836.0,20763.0,21393.0,21307.0,55.0)
# rw_speed.job2 time=2021-09-02:14:59:56 nn=1 np=56 user=871101 jobid=3481282 mbps=10163.927 mbps_rank_stddev=23.744 mbps_1sec_time_slices=(2391.0,5693.0,10226.0,9859.0,11008.0,10941.0,9693.0,10705.0,10485.0,10658.0,11047.0,10294.0,10027.0,10994.0,10711.0,9750.0,10574.0,10047.0,10469.0,10700.0,10290.0,10817.0,9897.0,10073.0,10632.0,10446.0,10396.0,10414.0,10968.0,10692.0,56.0)

# 1v1 job-fair
# sed 's/# //' < test.1v1.sh | grep ^rw_speed.job | head -2 | ./format_throughput_slices.py > test.job-fair.1v1.txt

# 1v1 size-fair
# sed 's/# //' < test.1v1.sh | grep ^rw_speed.job | tail -2 | ./format_throughput_slices.py > test.size-fair.1v1.txt