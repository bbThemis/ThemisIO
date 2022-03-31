set terminal pngcairo font "Times,18"
set termoption dashe
set output 'gift.1v1.png'

set yrange [0:29000]

# solo median 21476
# competing median 10866


set arrow from 1,17470 to 59,17470 lt 7 lw 1 nohead
set label "17470 MB/s" at 16,(17470+1200) left font "Times,14"

set arrow from 5,9436 to 55,9436 lt 7 lw 1 nohead
set label "9436 MB/s" at 13,(9436+1200) right font "Times,14"

# set arrow from 5,7289 to 55,7289 dt 2 lw 1 nohead
# set label "7289 MB/s" at 6,(7289+1200) left font "Times,14"

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
# set title "Throughput of competing jobs\nGIFT, 1 node vs 1 node"
plot \
     "gift_1.out" using 1:2 title "1 node" with linespoints lt 1 pt 7 ps .5 linecolor rgb '#0060ad', \
     "gift_1.out" using 1:3 title "1 node" with linespoints lt 1 pt 6 ps .5 linecolor rgb 'red'
