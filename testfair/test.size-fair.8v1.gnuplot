set terminal pdf font "Times,18"
set output 'test.size-fair.8v1.pdf'

set yrange [0:29000]

# solo median (stable end part) 21633
# competing 8 node median 16912
# competing 1 node median 2122

set arrow from 1,21633 to 59,21633 dt 2 lw 1 nohead
set label "21633 MB/s" at 20,(21633+1200) left font "Times,14"

set arrow from 5,16912 to 55,16912 dt 2 lw 1 nohead
set label "16912 MB/s" at 47,(16912+1200) left font "Times,14"

set arrow from 5,2122 to 55,2122 dt 2 lw 1 nohead
set label "2122 MB/s" at 47,(2122+1200) left font "Times,14"

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
# set title "Throughput of competing jobs\nSize-fair, 8 nodes vs 1 node"
plot \
     "test.size-fair.8v1.txt" using 1:2 title "8 nodes" with linespoints dt 1 pt 7 ps .5 linecolor rgb '#0060ad', \
     "test.size-fair.8v1.txt" using 1:3 title "1 node" with linespoints dt 1 pt 6 ps .5 linecolor rgb 'red'
