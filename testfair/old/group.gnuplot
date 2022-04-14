set terminal png size 1600,800 font "Arial,24"
set output 'group-user-size8.png'

set xlabel "Time in seconds" font ",24"
set ylabel "Throughput in MB/sec" font ", 24"

set title "Throughput of competing jobs\nGroup-User-Size-fair, 2 groups, 4 users, 8 jobs"
set key outside;
set key right top;
set key font ", 18"

set yrange [0:25000]

set style line 1 lt rgb "red" lw 2 pt 6
set style line 2 lt rgb "orange" lw 2 pt 6
set style line 3 lt rgb "yellow" lw 2 pt 6
set style line 4 lt rgb "green" lw 2 pt 6
set style line 5 lt rgb "blue" lw 2 pt 6
set style line 6 lt rgb "purple" lw 2 pt 6
set style line 7 lt rgb "cyan" lw 2 pt 6
set style line 8 lt rgb "grey" lw 2 pt 6
plot \
 "reduced_group.txt" using 1:2 title "Group1, User1, Job1, 1 node" with linespoints ls 1, \
 "reduced_group.txt" using 1:3 title "Group1, User1, Job2, 2 nodes" with linespoints ls 2, \
 "reduced_group.txt" using 1:4 title "Group1, User2, Job3, 3 nodes" with linespoints ls 3, \
 "reduced_group.txt" using 1:5 title "Group1, User2, Job4, 2 nodes" with linespoints ls 4, \
 "reduced_group.txt" using 1:6 title "Group2, User3, Job5, 3 nodes" with linespoints ls 5, \
 "reduced_group.txt" using 1:7 title "Group2, User3, Job6, 2 nodes" with linespoints ls 6, \
 "reduced_group.txt" using 1:8 title "Group2, User4, Job7, 1 node" with linespoints ls 7, \
 "reduced_group.txt" using 1:9 title "Group2, User4, Job8, 2 nodes" with linespoints ls 8
