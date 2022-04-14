## GIFT
Switch to GIFT branch
1. Download and Build ThemisIO As in the original version
2. Build GIFT Coupoun Server
```
make -f mds_Makefile
```
3. Set GIFT interval
```
// Change microseconds in mds.cpp
const unsigned int microseconds = 500000; // Default 500 ms
```
4. Start an interactive 1 node job and start the GIFT Coupoun Server.
```
idev -N 1 -n 1 -t 1:00:00
./mds
```
4. Start an interactive three node job.
```
idev -N 3 -n 3 -t 1:00:00
```
5. Run the ThemisIO Server
```
ibrun -np 1 ./server --policy gift
```
6. In Client Shell, run ./test.1v1.sh
```
ssh cxxx-xxx
export MLX5_SINGLE_THREADED=0
cd testfair
bash test.1v1.sh
```