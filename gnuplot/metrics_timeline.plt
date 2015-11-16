mints = system("cat min_ts.txt.tmp")

joiners = system("cat joiner-tasks.tmp")

set terminal postscript eps size 6,2.62 enhanced color \
	font 'Helvetica,20' linewidth 2
set grid
set xdata time
set timefmt "%s"
set format x "%s"
set xlabel "Time (seconds)"

set output "dispatcher-input-rate.eps"
set ylabel "Input rate (tuples/sec)"
set format y "%1.0e"
#set ytics (0,1000,6000)
#set yrange [0:6000]
plot "dispatcher-input-rate.dat" using ($1 - mints):2 notitle with lines

set output "input-rate.eps"
#set yrange [0:5000]
plot for [joiner in joiners] 'input-rate-'.joiner.'.dat' using ($1 - mints):2 title 'task-'.joiner with lines

unset format y

set output "latency.eps"
set ylabel "Latency (msec)"
#set ytics (0,4,12)
#set yrange [0:11]
plot "latency.dat" using ($1 - mints):3 notitle with lines

set output "interval.eps"
set ylabel "Response time (msec)"
#set yrange [0:3000]
#set ytics (0,100,1000)
plot for [joiner in joiners] 'interval-'.joiner.'.dat' using ($1 - mints):2 title 'task-'.joiner with lines

set output "state.eps"
set ylabel "State (KB)"
#set yrange [0:500]
#set ytics (0,100,500)
plot for [joiner in joiners] 'state-'.joiner.'.dat' using ($1 - mints):($3 / 1000) title 'task-'.joiner with lines
