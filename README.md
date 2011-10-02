Skeletor
========

Provides a Scala wrapper for Hector https://github.com/rantav/hector, a Java client library for Cassandra http://cassandra.apache.org/
Getting Started
---------------

0) Assumptions

* You have SBT 10 installed. 
* Cassandra is running locally. 

1) Get Skeletor

	git clone git@github.com:joestein/skeletor.git
	cd skeletor

2) Update the schema for Skeletor's Specification Tests

schema/bootstrap.txt contains the schema for Skeletor's Specification Tests

	~/apache-cassandra-0.8.6/bin/cassandra-cli -host localhost -port 9160 -f schema/bootstrap.txt

3) Run Skeletor's test
	
	sbt test

How To Use
----------

The tests are also examples of how to use Skeletor.  Take a look at them.

You can package Skeletor as a jar within your project

	sbt package

Thanx =) Joe Stein

http://linkedin.com/in/charmalloc

