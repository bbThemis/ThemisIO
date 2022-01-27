set terminal pdf font "Times,18"
set output 'test.size-fair.2v1.pdf'

set yrange [0:29000]

# solo median 22088
# competing 2 node median 14595
# competing 1 node median 7289

set arrow from 1,22088 to 59,22088 dt 2 lw 1 nohead
set label "22088 MB/s" at 20,(22088+1200) left font "Times,14"

set arrow from 5,14595 to 55,14595 dt 2 lw 1 nohead
set label "14595 MB/s" at 6,(14595+1200) left font "Times,14"

set arrow from 5,7289 to 55,7289 dt 2 lw 1 nohead
set label "7289 MB/s" at 6,(7289+1200) left font "Times,14"

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
# set title "Throughput of competing jobs\nSize-fair, 2 nodes vs 1 node"
plot \
     "test.size-fair.2v1.txt" using 1:2 title "2 nodes" with linespoints dt 1 pt 7 ps .5 linecolor rgb '#0060ad', \
     "test.size-fair.2v1.txt" using 1:3 title "1 node" with linespoints dt 1 pt 6 ps .5 linecolor rgb 'red'
