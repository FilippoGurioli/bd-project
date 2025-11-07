name := "NYC-Taxi-Tip-Analysis"

version := "1.0"

scalaVersion := "2.12.17"  // compatible with Spark 3.5.x

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided"
)

ThisBuild / organization := "project"

// optional: makes assembly easier later
Compile / mainClass := Some("project.Application")
