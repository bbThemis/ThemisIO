set terminal pdf font "Times,18"
set output 'user-size-final.pdf'

set yrange [0:29000]

# solo median 21673
# competing median 10558


# set arrow from 1,21673 to 59,21673 dt 2 lw 1 nohead
# set label "21673 MB/s" at 16,(21673+1200) left font "Times,14"

# set arrow from 5,10558 to 55,10558 dt 2 lw 1 nohead
# set label "10558 MB/s" at 13,(10558+1200) right font "Times,14"

# set arrow from 5,7289 to 55,7289 dt 2 lw 1 nohead
# set label "7289 MB/s" at 6,(7289+1200) left font "Times,14"

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
# set title "Throughput of competing jobs\nJob-fair, 2 nodes vs 1 node"
plot \
     "4_jobs.out" using 1:2 title "Job 1, User 1, 1 nodes" with linespoints dt 1 pt 7 ps .5 linecolor rgb '#0060ad', \
     "4_jobs.out" using 1:3 title "Job 2, User 1, 2 nodes" with linespoints dt 1 pt 6 ps .5 linecolor rgb 'red', \
     "4_jobs.out" using 1:2 title "Job 3, User 2, 4 nodes" with linespoints dt 1 pt 7 ps .5 linecolor rgb '#0060ad', \
     "4_jobs.out" using 1:3 title "Job 4, User 2, 6 nodes" with linespoints dt 1 pt 6 ps .5 linecolor rgb 'red'
