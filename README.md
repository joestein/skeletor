Skeletor
========

Provides a Scala wrapper for Hector https://github.com/rantav/hector, a Java client library for Cassandra http://cassandra.apache.org/
Getting Started
---------------

0) Assumptions

* You have SBT installed - https://github.com/harrah/xsbt/wiki/Getting-Started-Setup
* Cassandra 0.8.6 is running locally - http://wiki.apache.org/cassandra/GettingStarted

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

And in your 
`build.sbt` file add to 
`libraryDependencies ++= Seq` so you get the Hector library

	"me.prettyprint" % "hector-core" % "0.8.0-2"

Thanx =) Joe Stein

http://linkedin.com/in/charmalloc

