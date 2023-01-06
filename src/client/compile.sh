#!/bin/bash

gcc -I../../include/ -O3 -fPIC -c libudis86/decode.c -o decode.o
gcc -I../../include/ -O3 -fPIC -c ${PWD}/libudis86/itab.c -o itab.o
gcc -I../../include/ -O3 -fPIC -c ${PWD}/libudis86/syn-att.c -o syn-att.o
gcc -I../../include/ -O3 -fPIC -c ${PWD}/libudis86/syn.c -o syn.o 
gcc -I../../include/ -O3 -fPIC -c ${PWD}/libudis86/syn-intel.c -o syn-intel.o
gcc -I../../include/ -O3 -fPIC -c ${PWD}/libudis86/udis86.c -o udis86.o
g++ -I../../include/ -fPIC -g -O2 -c ../dict.cpp
g++ -I../../include/ -fPIC -g -O2 -c ../xxhash.cpp
g++ -I../../include/ -I${MERCURY_INCLUDE_DIRECTORY} -fPIC -g -O2 -c ../rpc_engine.cpp
g++ -I../../include/ -I${MERCURY_INCLUDE_DIRECTORY} -fPIC -g -O2 -c ../rpc.cpp

g++ -I../../include/ -I${MERCURY_INCLUDE_DIRECTORY} -g -O2 -c -fPIC wrapper.cpp
g++ -g -O2 -fPIC -shared -o ../../wrapper.so wrapper.o decode.o itab.o syn-att.o syn-intel.o syn.o udis86.o dict.o xxhash.o rpc_engine.o rpc.o -L${MERCURY_LIB_DIRECTORY} -L${FABRIC_LIB_DIRECTORY} -libverbs -lrt -lmercury -lmercury_util -lna -lfabric 
sed -i "s/xstaT64/xstat64/g" ../../wrapper.so
sed -i "s/openaT64/openat64/g" ../../wrapper.so
sed -i "s/staTfs64/statfs64/g" ../../wrapper.so
sed -i "s/staTvfs64/statvfs64/g" ../../wrapper.so
sed -i "s/fxstaTat64/fxstatat64/g" ../../wrapper.so

