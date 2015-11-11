#!/bin/bin/perl

use strict;
use Getopt::Std;
use warnings;

my @joiner_array = (5, 6);

system("grep -m 1 \"[0-9]:dispatch\" metrics.log")

#populate the dispatcher-input-rate file
system("rm -f dispatcher-input-rate.dat");
system("grep \"dispatch\\s*input-rate\" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:[0-9]*[ \t]*[0-9]:dispatch[ \t]*input-rate[ \t]*/\t/' > dispatcher-input-rate.dat");

#populate the latency file
system("rm -f latency.dat");
system("grep \"comp-latency\" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:[0-9]*//' | sed 's/:[a-z]*[ \t]*comp-latency[ \t]*/\t/' > latency.dat");

#populate the state size files
foreach my $joiner (@joiner_array)
{
	system("rm -f state-$joiner.dat");
	system("grep \"$joiner:joiner.*state\" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:[0-9]*//' | sed 's/:joiner[ \t]*/\t/' | sed 's/[ \t]*state-size[ \t]*/\t/' | sed 's/[ \t]+/\t/' > state-$joiner.dat");
	system("rm -f input-rate-$joiner.dat");
	system("grep \"$joiner:joiner\\s*input-rate\" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:[0-9]*[ \t]*$joiner:joiner[ \t]*input-rate[ \t]*/\t/' > input-rate-$joiner.dat");
}

#get throughput of the joiners
system("rm -f throughput.dat");
system("grep \":joiner\\s*throughput\" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:6700[ \t]*/\t/' | sed 's/:joiner[ \t]*throughput[ \t]*/\t/' > throughput.dat");