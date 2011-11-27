# Gnuplot script file for plotting data in file "force.dat"
# This file is called   force.p

set   autoscale                        # scale axes automatically
set ytic auto                          # set ytics automatically

set term png
set output "kmeans-times.png"

set grid x y
#set logscale x
set title "K-means: average iteration time VS input size"
set xlabel "Input size (MB)"
set ylabel "Iteration time (s)"
plot "time-kmeans-hadoop-mr" using 1:2:xtic(1) title 'Hadoop-MR' with linespoints,\
     "time-kmeans-stratosphere-mr" using 1:2 title 'Stratosphere-MR' with linespoints
