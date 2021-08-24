set terminal png size 1200,800 font "Arial,18"
set output 'size-fair.2vs1.png'

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
set title "Throughput of competing jobs\nSize-fair, 2 nodes vs 1 node"
plot \
 "size-fair.2vs1.txt" using 1:2 title "Job1, 2 nodes" with linespoints, \
 "size-fair.2vs1.txt" using 1:3 title "Job2, 1 node" with linespoints
