#!/bin/bash

MYFS_CONF="/home1/07811/edk/ThemisIO/myfs.param"
LD_PRELOAD="/home1/07811/edk/ThemisIO/client/wrapper.so" 

./iops_pwrite_mpi 10 /myfs/ 1048576

