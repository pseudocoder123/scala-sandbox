import scala.collection.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "SparkPlayground"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.1",
  "org.apache.spark" %% "spark-sql" % "3.2.1",
  "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.5",
  "org.apache.kafka" % "kafka-clients" % "3.4.0",
  "com.google.code.gson" % "gson" % "2.8.9",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.1",
)
