#!/bin/bash

gcc -O3 -fPIC -c libudis86/decode.c -o decode.o
gcc -O3 -fPIC -c ${PWD}/libudis86/itab.c -o itab.o
gcc -O3 -fPIC -c ${PWD}/libudis86/syn-att.c -o syn-att.o
gcc -O3 -fPIC -c ${PWD}/libudis86/syn.c -o syn.o 
gcc -O3 -fPIC -c ${PWD}/libudis86/syn-intel.c -o syn-intel.o
gcc -O3 -fPIC -c ${PWD}/libudis86/udis86.c -o udis86.o
g++ -fPIC -g -O0 -c ../dict.cpp
g++ -fPIC -g -O0 -c ../xxhash.cpp

g++ -g -O0 -c -fPIC wrapper.cpp
g++ -g -O0 -fPIC -shared -o wrapper.so wrapper.o decode.o itab.o syn-att.o syn-intel.o syn.o udis86.o dict.o xxhash.o -libverbs -lrt
sed -i "s/xstaT64/xstat64/g" wrapper.so
sed -i "s/openaT64/openat64/g" wrapper.so
sed -i "s/staTfs64/statfs64/g" wrapper.so
sed -i "s/staTvfs64/statvfs64/g" wrapper.so
sed -i "s/fxstaTat64/fxstatat64/g" wrapper.so

