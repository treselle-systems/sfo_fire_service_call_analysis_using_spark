name := "SFFireServiceCallAnalysis"

version := "1.0"

organization := "com.treselle"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
"org.apache.spark" %% "spark-sql" % "2.0.0" % "provided"
)
