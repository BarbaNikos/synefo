#!/bin/bin/perl

use strict;
use Getopt::Std;
use warnings;
use Cwd;


system("rm -f joiner-tasks.tmp");
system("grep \":joiner\" metrics.log | sed 's/.*astro2.cs.pitt.edu:6700[ \t]*//' | sed 's/:joiner.*//' | sort | uniq | tr '\n' ' ' > joiner-tasks.tmp");

#print "command failed" if (!$?);

my $line = `cat joiner-tasks.tmp`;

#print "Line parsed: $line .\n";

my @joiner_array = split /\s+/, $line;
my @joiners;

for my $joiner (@joiner_array)
{
    if (index($joiner, ":") == -1 && index($joiner, "_") == -1 && index($joiner, "system") == -1 &&
        index($joiner, "ack") == -1 && index($joiner, "{") == -1 && index($joiner, "joiner") == -1)
        {
            push @joiners, $joiner;
        }
}

my $file = "joiner-tasks.tmp";
if (-e $file)
{
    unlink $file or warn "Could not unlink $file: $!";
}

open my $file_handle, ">", "$file" or die "Could not create file $file";
print $file_handle join(" ", @joiners);
close $file_handle;

print "Joiner tasks: ";
print join(" ", @joiners);
print ".\n";

$line = `grep -m 1 "[0-9]:dispatch" metrics.log`;
my @tokens = split /\s+/, $line;
# print "Tokens: \n";
# print join(", ", @tokens);

$file = "min_ts.txt.tmp";
if (-e $file)
{
    unlink $file or warn "Could not unlink $file: $!";
}

open $file_handle, ">", "$file" or die "Could not create file $file";
print $file_handle $tokens[3];
close $file_handle;


# populate the dispatcher-input-rate file
system("rm -f dispatcher-input-rate.dat");
system("grep \"dispatch\\s*input-rate\" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:[0-9]*[ \t]*[0-9]:dispatch[ \t]*input-rate[ \t]*/\t/' > dispatcher-input-rate.dat");
print "resolved dispatcher input-rate.\n";

# populate the latency file
system("rm -f latency.dat");
system("grep \"comp-latency\" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:[0-9]*//' | sed 's/:[a-z]*[ \t]*comp-latency[ \t]*/\t/' > latency.dat");
print "resolved end-to-end latency.\n";

# populate the interval, input-rate, and state-size files
foreach my $joiner (@joiners)
{
    print "preparing statistics for joiner task-$joiner.\n";
    system("rm -f interval-$joiner.dat");
    system("grep \"dispatch\\s*control-interval\" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:[0-9]*[ \t]*[0-9]:dispatch[ \t]*control-interval[ \t]*/\t/' | grep \"$joiner-\" | sed 's/$joiner-//' > interval-$joiner.dat");
	system("rm -f state-$joiner.dat");
	system("grep \"$joiner:joiner.*state-size\" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:[0-9]*//' | sed 's/:joiner[ \t]*/\t/' | sed 's/[ \t]*state-size[ \t]*/\t/' | sed 's/[ \t]+/\t/' > state-$joiner.dat");
	system("rm -f input-rate-$joiner.dat");
	system("grep \"$joiner:joiner\\s*input-rate\" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:[0-9]*[ \t]*$joiner:joiner[ \t]*input-rate[ \t]*/\t/' > input-rate-$joiner.dat");
    print "resolved statistics for joiner task-$joiner.\n";
}

# get throughput of the joiners
system("rm -f throughput.dat");
system("grep \":joiner\\s*throughput\" metrics.log | sed 's/^201[0-9][-][0-9]*[-][0-9]*[ \t]*[0-9]*:[0-9]*:[0-9]*,[0-9]*[ \t]*[0-9]*[ \t]*//' | sed 's/[ \t]*astro2.cs.pitt.edu:6700[ \t]*/\t/' | sed 's/:joiner[ \t]*throughput[ \t]*/\t/' > throughput.dat");