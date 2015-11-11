mints = 1447210940

set terminal postscript eps size 6,2.62 enhanced color \
	font 'Helvetica,20' linewidth 2
set grid
set xdata time
set timefmt "%s"
set format x "%s"
set xlabel "Time (seconds)"

set output "input-rate-timeline.eps"
set ylabel "Input rate (tuples/sec)"
set format y "%1.0e"
#set ytics (0,1000,6000)
set yrange [0:6000]
plot "dispatcher-input-rate.dat" using ($1 - mints):2 notitle with lines

unset format y

set output "latency.eps"
set ylabel "Latency (msec)"
#set ytics (0,4,12)
set yrange [0:11]
plot "latency.dat" using ($1 - mints):4 notitle with lines

set output "interval.eps"
set ylabel "Response time (msec)"
set yrange [0:3000]
#set ytics (0,100,1000)
plot "interval-5.dat" using ($1 - mints):2 title 'task-5' with lines, \
	"interval-6.dat" using ($1 - mints):2 title 'task-6' with lines

set output "state.eps"
set ylabel "State (KB)"
set yrange [0:500]
#set ytics (0,100,500)
plot "state-5.dat" using ($1 - mints):($2/1000) title 'task-5' with lines, \
	"state-6.dat" using ($1 - mints):($2/1000) title 'task-6' with lines