set terminal pdf font "Times,18"
set output 'test.job-fair.2v1.pdf'

set yrange [0:29000]

# solo median 21476
# competing median 10866


set arrow from 1,21476 to 59,21476 dt 2 lw 1 nohead
set label "21476 MB/s" at 16,(21476+1200) left font "Times,14"

set arrow from 5,10866 to 55,10866 dt 2 lw 1 nohead
set label "10866 MB/s" at 13,(10866+1200) right font "Times,14"

# set arrow from 5,7289 to 55,7289 dt 2 lw 1 nohead
# set label "7289 MB/s" at 6,(7289+1200) left font "Times,14"

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
# set title "Throughput of competing jobs\nJob-fair, 2 nodes vs 1 node"
plot \
     "test.job-fair.2v1.txt" using 1:2 title "2 nodes" with linespoints dt 1 pt 7 ps .5 linecolor rgb '#0060ad', \
     "test.job-fair.2v1.txt" using 1:3 title "1 node" with linespoints dt 1 pt 6 ps .5 linecolor rgb 'red'
