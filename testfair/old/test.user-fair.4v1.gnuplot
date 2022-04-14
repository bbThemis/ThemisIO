set terminal pdf font "Times,18"
set output 'test.user-fair.4v1.pdf'

set yrange [0:33000]

# solo median 22106
# half median 10802
# quarter median 5426


set arrow from 1,22106 to 59,22106 dt 2 lw 1 nohead
set label "22106 MB/s" at 11,(22106+1200) left font "Times,14"

set arrow from 5,10802 to 55,10802 dt 2 lw 1 nohead
set label "10802 MB/s" at 13,(10802+1200) right font "Times,14"

set arrow from 5,5426 to 55,5426 dt 2 lw 1 nohead
set label "5426 MB/s" at 13,(5426+1200) right font "Times,14"

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
# set title "Throughput of competing jobs\nUser-fair, 2 nodes vs 1 node"
plot \
     "test.user-fair.4v1.txt" using 1:2 title "User A, job 1, 2 nodes" with linespoints dt 1 pt 7 ps .5 linecolor rgb '#0060ad', \
     "test.user-fair.4v1.txt" using 1:3 title "User A, job 2, 2 nodes" with linespoints dt 1 pt 6 ps .5 linecolor rgb 'red', \
     "test.user-fair.4v1.txt" using 1:4 title "User B, 1 node" with linespoints dt 1 pt 8 ps .5 linecolor rgb 'green'
