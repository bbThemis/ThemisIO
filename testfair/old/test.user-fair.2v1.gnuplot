set terminal pdf font "Times,18"
set output 'test.user-fair.2v1.pdf'

set yrange [0:33000]

# solo median 21669
# half median 10805
# quarter median 5443


set arrow from 1,21669 to 59,21669 dt 2 lw 1 nohead
set label "21669 MB/s" at 11,(21669+1200) left font "Times,14"

set arrow from 5,10805 to 55,10805 dt 2 lw 1 nohead
set label "10805 MB/s" at 13,(10805+1200) right font "Times,14"

set arrow from 5,5443 to 55,5443 dt 2 lw 1 nohead
set label "5443 MB/s" at 13,(5443+1200) right font "Times,14"

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
# set title "Throughput of competing jobs\nUser-fair, 2 nodes vs 1 node"
plot \
     "test.user-fair.2v1.txt" using 1:2 title "User A, job 1, 1 node" with linespoints dt 1 pt 7 ps .5 linecolor rgb '#0060ad', \
     "test.user-fair.2v1.txt" using 1:3 title "User A, job 2, 1 node" with linespoints dt 1 pt 6 ps .5 linecolor rgb 'red', \
     "test.user-fair.2v1.txt" using 1:4 title "User B, 1 node" with linespoints dt 1 pt 8 ps .5 linecolor rgb 'green'
