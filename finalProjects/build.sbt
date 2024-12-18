import scala.collection.Seq
import scala.collection.immutable.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"


scalaVersion := "2.12.10" // Spark 3.2.x supports Scala 2.12.x

// Define Spark version that has better compatibility with newer Java versions
val sparkVersion = "3.5.1"

resolvers ++= Seq(
  "Akka Repository" at "https://repo.akka.io/maven/",
  "Confluent" at "https://packages.confluent.io/maven/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.5",
  "com.google.code.gson" % "gson" % "2.8.9",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  "org.apache.kafka" % "kafka-clients" % "3.4.0",

  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
  "com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.6",
  "org.apache.spark" %%"spark-protobuf"% "3.5.1",

  "com.typesafe.akka" %% "akka-actor" % "2.6.20",
  "com.typesafe.akka" %% "akka-stream" % "2.8.5",
  "com.typesafe.akka" %% "akka-stream-kafka" % "2.1.1",

  "com.typesafe.akka" %% "akka-http" % "10.5.3",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.5.3"
)



