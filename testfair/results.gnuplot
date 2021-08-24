set terminal png size 1200,800 font "Arial,18"
set output 'job-fair.1v1.png'

set xlabel "Time in seconds"
set ylabel "Throughput in MB/sec"
set title "Throughput of competing jobs\nJob-fair, 1 node vs 1 node"
plot "job-fair.1vs1.txt" using 1:2 title "Job1, 1 node" with linespoints, "job-fair.1vs1.txt" using 1:3 title "Job2, 2 nodes" with linespoints


# plot "results.2.tsv" using 1:2 title "Job1" with linespoints, "results.2.tsv" using 1:3 title "Job2" with linespoints

# job-fair.1vs1.txt
# job-fair.4vs1.txt
# size-fair.1vs1.txt
# size-fair.2vs1.txt
# size-fair.4vs1.txt
# user-fair.8+8vs1.txt
