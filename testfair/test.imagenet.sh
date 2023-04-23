# ppn=1
# iosize=1m

# IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 1)) -o $(($ppn * 2)) \
#    ../tests/themis_client.sh -u 101 -j 1001 -n 1 \
#    ./rw_speed -time=60 -iosize=$iosize -filesize=100m -tag=job1 &> rw_speed1.out &

# IBRUN_TASKS_PER_NODE=$ppn ibrun -n $(($ppn * 1)) -o $(($ppn * 3)) \
#    ../tests/themis_client.sh -u 102 -j 1002 -n 1 -s 15 \
#    ./rw_speed -time=30 -iosize=$iosize -filesize=100m -tag=job2 &> rw_speed2.out &

ibrun -n 1 -o 2 \
        ../tests/themis_client.sh -u 101 -j 1001 -n 1 \
        mkdir /myfs/imagenet




ibrun -n 1 -o 2 \
        ../tests/themis_client.sh -u 101 -j 1001 -n 1 \
        tar xf /scratch1/00946/zzhang/datasets/imagenet-small/imagenet-small.tar -C /myfs/imagenet



# ibrun -n 1 -o 1 \
#         ../tests/themis_client.sh -u 101 -j 1001 -n 1 \
#         ls /myfs/imagenet/ILSVRC2012_img_train/ -l \
        