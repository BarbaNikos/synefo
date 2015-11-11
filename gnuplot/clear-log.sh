#!/bin/csh

echo The $0 script is called with $#argv parameters

# get the state for task 5
grep "5:joiner.*state" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:[0-9]*//' | sed 's/:joiner[ \t]*/\t/' | sed 's/[ \t]*state-size[ \t]*/\t/' | sed 's/[ \t]+/\t/'

# get the state for task 6
grep "6:joiner.*state" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:[0-9]*//' | sed 's/:joiner[ \t]*/\t/' | sed 's/[ \t]*state-size[ \t]*/\t/' | sed 's/[ \t]+/\t/'

# get the control interval for task 5
grep "dispatch[\s]*" metrics.log | grep "control-interval\s*5-[0-9]*" | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:[0-9]*//' | sed 's/[0-9]:dispatch[ \t]*control-interval[ \t]*//' | sed 's/5-//'

# get the control interval for task 6
grep "dispatch[\s]*" metrics.log | grep "control-interval\s*6-[0-9]*" | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:[0-9]*//' | sed 's/[0-9]:dispatch[ \t]*control-interval[ \t]*//' | sed 's/6-//'

# get the complete latency
grep "comp-latency" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:[0-9]*//' | sed 's/:[a-z]*[ \t]*comp-latency[ \t]*/\t/'

# get the dispatcher input-rate
grep "dispatch\s*input-rate" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:[0-9]*[ \t]*[0-9]:dispatch[ \t]*input-rate[ \t]*/\t/'

# get the throughput of the joiners
grep ":joiner\s*throughput" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:6700[ \t]*/\t/' | sed 's/:joiner[ \t]*throughput[ \t]*/\t/'