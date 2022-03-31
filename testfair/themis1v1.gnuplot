set terminal pngcairo size 1440,864 font "Times,18"
set termoption dashed
set output 'themis.1v1.png'

set yrange [0:29000]

# solo median 21476
# competing median 10866


set arrow from 1,19832 to 59,19832 lt 7 lw 1 nohead
set label "19832 MB/s" at 16,(19832+1200) left font "Times,14"

set arrow from 5,10179 to 55,10179 lt 7 lw 1 nohead
set label "10179 MB/s" at 13,(10179+1200) right font "Times,14"

# set arrow from 5,7289 to 55,7289 dt 2 lw 1 nohead
# set label "7289 MB/s" at 6,(7289+1200) left font "Times,14"

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
# set title "Throughput of competing jobs\nGIFT, 1 node vs 1 node"
plot \
     "themis1v1.out" using 1:2 title "1 node" with linespoints lt 1 pt 7 ps .5 linecolor rgb '#0060ad', \
     "themis1v1.out" using 1:3 title "1 node" with linespoints lt 1 pt 6 ps .5 linecolor rgb 'red'
