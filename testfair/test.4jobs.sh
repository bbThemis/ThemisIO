ppn=56
nodes=16
iosize=1m
filesize=100m


mpiexec.hydra -hostfile ../hostfile_client1 -np $(($ppn * $nodes)) -ppn $ppn \
   ../tests/themis_client.sh -u 101 -j 1001 -n $nodes \
   ./rw_speed -time=60 -iosize=$iosize -filesize=$filesize -tag=job1 &> rw_speed1.out &



mpiexec.hydra -hostfile ../hostfile_client2 -np $(($ppn * $nodes)) -ppn $ppn \
   ../tests/themis_client.sh -u 102 -j 1002 -n $nodes \
   ./rw_speed -time=60 -iosize=$iosize -filesize=$filesize -tag=job2 &> rw_speed2.out &



mpiexec.hydra -hostfile ../hostfile_client3 -np $(($ppn * $nodes)) -ppn $ppn \
   ../tests/themis_client.sh -u 103 -j 1003 -n $nodes \
   ./rw_speed -time=60 -iosize=$iosize -filesize=$filesize -tag=job3 &> rw_speed3.out &




mpiexec.hydra -hostfile ../hostfile_client4 -np $(($ppn * $nodes)) -ppn $ppn \
   ../tests/themis_client.sh -u 104 -j 1004 -n $nodes \
   ./rw_speed -time=60 -iosize=$iosize -filesize=$filesize -tag=job4 &> rw_speed4.out &





wait
cat rw_speed1.out
cat rw_speed2.out
cat rw_speed3.out
cat rw_speed4.out
