#!/bin/bash

export LD_PRELOAD=/home1/07811/edk/ThemisIO/client/wrapper.so
export MYFS_CONF=/home1/07811/edk/ThemisIO/myfs.param
./rw_speed -iosize=1m -filesize=1g -tag=test0 -time=10
