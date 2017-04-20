name := "spark_pipeline"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "ai.h2o" % "sparkling-water-core_2.10" % "1.6.3" % "provided",
  "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.1" % "provided"
)


