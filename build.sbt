ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "TwitchAnalytics"
  )


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql"  % "2.4.5",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.2",
  "org.mongodb" % "mongo-java-driver" % "3.12.10",
  "org.apache.kafka"    %  "kafka-clients"         % "2.4.0",
  "org.apache.kafka"    %% "kafka"                 % "2.4.0",
  "org.apache.kafka"    %  "kafka-streams"         % "2.4.0"
)

