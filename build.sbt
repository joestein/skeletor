
name := "Skeletor"

version := "1.0.1.0"

scalaVersion := "2.9.1"

libraryDependencies ++= Seq(
	"org.scala-tools.testing" % "specs_2.8.1" % "1.6.6" % "test",
	"me.prettyprint" % "hector-core" % "1.0-3",
	"org.apache.cassandra" % "cassandra-all" % "1.0.8",	
	"org.slf4j" % "slf4j-log4j12" % "1.6.4"
)