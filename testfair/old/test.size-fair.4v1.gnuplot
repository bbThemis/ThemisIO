# set terminal png size 1200,800 font "Arial,18"
# set output 'test.size-fair.4v1.png'

# set terminal emf size 1200,800 font "Arial,18"
# set output 'test.size-fair.4v1.emf'

# set terminal postscript eps
# set output 'test.size-fair.4v1.eps'

# set terminal pdf monochrome font "Times,18"
set terminal pdf font "Times,18"
set output 'test.size-fair.4v1.pdf'


set yrange [0:29000]

# solo median 21783
# competing 4 node median 17356
# competing 1 node median 4363

set arrow from 1,21783 to 59,21783 dt 2 lw 1 nohead
set label "21783 MB/s" at 20,(21783+1200) left font "Times,14"

set arrow from 5,17356 to 55,17356 dt 2 lw 1 nohead
set label "17356 MB/s" at 6,(17356+1200) left font "Times,14"

set arrow from 5,4363 to 55,4363 dt 2 lw 1 nohead
set label "4363 MB/s" at 6,(4363+1200) left font "Times,14"

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
# set title "Throughput of competing jobs\nSize-fair, 1 node vs 4 nodes"
# set title "Size-fair, 1 node vs 4 nodes"
plot \
     "test.size-fair.4v1.txt" using 1:2 title "4 nodes" with linespoints dt 1 pt 7 ps .5 linecolor rgb '#0060ad',  \
     "test.size-fair.4v1.txt" using 1:3 title "1 node" with linespoints dt 1 pt 6 ps .5 linecolor rgb 'red'
