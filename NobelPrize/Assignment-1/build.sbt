name := "Assignment-1"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.3",
  "org.apache.spark" %% "spark-sql" % "3.1.3",
  "org.apache.spark" %% "spark-mllib" % "3.1.3",
  "org.apache.spark" %% "spark-streaming" % "3.1.3",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.3"
)