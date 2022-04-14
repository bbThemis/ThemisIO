set terminal png size 1600,2400 font "Arial,18"
set output 'group-user-size-radical2.png'

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
set yrange [0:28000]
set title "Throughput of competing jobs\nGroup-User-Size-fair, 2 groups, 4 users, 8 jobs"
set key outside;
set key right top;
plot \
 "group_8_jobs.txt" using 1:2 title "Group1, User1, Job1, 1 node" with linespoints, \
 "group_8_jobs.txt" using 1:3 title "Group2, User2, Job2, 2 nodes" with linespoints, \
 "group_8_jobs.txt" using 1:4 title "Group2, User2, Job3, 3 nodes" with linespoints, \
 "group_8_jobs.txt" using 1:5 title "Group2, User2, Job4, 2 nodes" with linespoints, \
 "group_8_jobs.txt" using 1:6 title "Group2, User3, Job5, 3 nodes" with linespoints, \
 "group_8_jobs.txt" using 1:7 title "Group2, User3, Job6, 2 nodes" with linespoints, \
 "group_8_jobs.txt" using 1:8 title "Group2, User4, Job7, 1 node" with linespoints, \
 "group_8_jobs.txt" using 1:9 title "Group2, User4, Job8, 2 nodes" with linespoints
