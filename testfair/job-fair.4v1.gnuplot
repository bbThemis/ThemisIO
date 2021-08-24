set terminal png size 1200,800 font "Arial,18"
set output 'job-fair.4v1.png'

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
set title "Throughput of competing jobs\nJob-fair, 4 nodes vs 1 node"
plot \
 "job-fair.4vs1.txt" using 1:2 title "Job1, 4 nodes" with linespoints, \
 "job-fair.4vs1.txt" using 1:3 title "Job2, 1 node" with linespoints
