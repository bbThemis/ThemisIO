set terminal png size 1200,800 font "Arial,18"
set output 'test.job-fair.1v1.png'

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
set title "Throughput of competing jobs\nJob-fair, 1 node vs 1 node"
plot \
     "test.job-fair.1v1.txt" using 1:2 title "Job1, 1 node" with linespoints, \
     "test.job-fair.1v1.txt" using 1:3 title "Job2, 1 node" with linespoints