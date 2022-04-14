#!/bin/bash

IOR=/home1/07811/edk/sw/ior-3.3.0/bin/ior
# PREFIX=/home1/07811/edk/scratch/ior
PREFIX=/myfs/ior

export MYFS_CONF="/home1/07811/edk/ThemisIO/myfs.param"
export LD_PRELOAD="/home1/07811/edk/ThemisIO/client/wrapper.so"

${IOR} -C -Q 1 -g -G 778000062 -k -e -o ${PREFIX} -t 1m -b 180m -F -w -D 60 -a POSIX

#  -i 3
# do 3 iterations
