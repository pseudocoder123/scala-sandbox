name := """CorporateEquipmentAllocation"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.13.14"

// Core dependencies
libraryDependencies += guice
libraryDependencies += ws
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.2"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.4.0"  // Add the correct version of Kafka client

// Testing dependencies
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "7.0.0" % Test

// Slick and Flyway dependencies
libraryDependencies ++= Seq(
  "org.playframework" %% "play-slick" % "6.1.0",  // Slick for database interaction
  "org.flywaydb" % "flyway-core" % "9.16.0",       // Flyway for DB migrations
  "mysql" % "mysql-connector-java" % "8.0.26"      // MySQL connector
)

libraryDependencies += "com.auth0" % "java-jwt" % "4.3.0" // Java JWT library
libraryDependencies += filters

// Adds additional packages into Twirl
//TwirlKeys.templateImports += "com.example.controllers._"

// Adds additional packages into conf/routes
// play.sbt.routes.RoutesKeys.routesImport += "com.example.binders._"
