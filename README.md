# Synefo
This repository hosts the codebase for Synefo, which is an elasticity framework that works on top of [Apache Storm](http://storm.apache.org/). Synefo was demonstrated at [ACM SIGMOD 2015](http://www.sigmod2015.org/) Conference and it was part of the research prototype named [CE-Storm: Confidential Elastic Processing of Data Streams](http://db10.cs.pitt.edu/pubserver/web/ViewPublication.php?PublicationUID=a731d278f75dcb03b0926361df4853).

# Overview
Synefo consists of a number of abstraction classes for Apache Storm, along with an external server that manages the scale-in/-out requests. In detail, Synefo's components are: 
  * Storm's Bolt class extensions
  * External Synefo server
  * ZooKeeper API
