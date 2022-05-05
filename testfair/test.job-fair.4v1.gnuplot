set terminal pdf font "Times,18"
set output 'test.job-fair.4v2221.pdf'

set yrange [0:29000]

# solo median 21673
# competing median 10558


set arrow from 1,21673 to 59,21673 dt 2 lw 1 nohead
set label "21673 MB/s" at 16,(21673+1200) left font "Times,14"

set arrow from 5,10558 to 55,10558 dt 2 lw 1 nohead
set label "10558 MB/s" at 13,(10558+1200) right font "Times,14"

# set arrow from 5,7289 to 55,7289 dt 2 lw 1 nohead
# set label "7289 MB/s" at 6,(7289+1200) left font "Times,14"

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
# set title "Throughput of competing jobs\nJob-fair, 2 nodes vs 1 node"
plot \
     "test.job-fair.4v1.txt" using 1:2 title "4 nodes" with linespoints dt 1 pt 7 ps .5 linecolor rgb '#0060ad', \
     "test.job-fair.4v1.txt" using 1:3 title "1 node" with linespoints dt 1 pt 6 ps .5 linecolor rgb 'red'
