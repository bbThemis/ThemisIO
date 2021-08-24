set terminal png size 1200,800 font "Arial,18"
set output 'user-fair.8+8vs1.png'

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
set yrange [0:28000]
set title "Throughput of competing jobs\nUser-fair, 2 jobs vs 1"
plot \
 "user-fair.8+8vs1.txt" using 1:2 title "Job1, 8 nodes" with linespoints, \
 "user-fair.8+8vs1.txt" using 1:3 title "Job2, 8 nodes" with linespoints, \
 "user-fair.8+8vs1.txt" using 1:4 title "Job3, 1 node" with linespoints
