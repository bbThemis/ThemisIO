set terminal png size 1200,800 font "Arial,18"
set output 'test.job-fair.8v1.png'

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
set title "Throughput of competing jobs\nJob-fair, 1 node vs 8 nodes"
plot \
     "test.job-fair.8v1.txt" using 1:2 title "1 node" with linespoints, \
     "test.job-fair.8v1.txt" using 1:3 title "8 nodes" with linespoints
