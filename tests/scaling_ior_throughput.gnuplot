set terminal pdf font "Times,18"
set output 'scaling_ior_throughput.pdf'

ideal(x) = x * (11766.65 / 1024)

set xlabel "Server node count"
set ylabel "Throughput in GB/sec"
set title "Throughput scaling"
set datafile separator ","
set logscale xy
set xrange[.8:150]
set yrange[5:4000]
set key left top

set xtics ("1" 1, "2" 2, "4" 4, "8" 8, "16" 16, "32" 32, "64" 64, "128" 128)


plot \
     ideal(x) title "Ideal scaling" dt 2, \
     "scaling_ior_throughput.csv" every ::1 using 1:($3/1024) title "FIFO read" with linespoints pt 4 linecolor rgb '#0060ad', \
     "scaling_ior_throughput.csv" every ::1 using 1:($2/1024) title "FIFO write" with linespoints pt 6 linecolor rgb 'red', \
     "scaling_ior_throughput.csv" every ::1 using 1:($5/1024) title "Job-fair read" with linespoints pt 12 linecolor rgb 'green', \
     "scaling_ior_throughput.csv" every ::1 using 1:($4/1024) title "Job-fair write" with linespoints pt 8 linecolor rgb 'black'
