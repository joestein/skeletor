crossScalaVersions := Seq("2.9.0-1", "2.9.1")

organization := "github.joestein"

name := "skeletor"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.9.1"

libraryDependencies ++= Seq(
	"org.scala-tools.testing" % "specs_2.8.1" % "1.6.6" % "test",
	"me.prettyprint" % "hector-core" % "0.8.0-2"
)

publishTo := Some("Nexus" at "http://nexus:8082/nexus/content/repositories/snapshots/")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials") 
