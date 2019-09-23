
ThisBuild / scalaVersion     := "2.11.0"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.knoldus"
ThisBuild / organizationName := "Knoldus, Inc"

lazy val root = (project in file("."))
  .settings(
    name := "$name$",
    scalaVersion := "2.11.0",
      libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.3",
      "org.apache.spark" %% "spark-streaming" % "2.4.3",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.3",
      "org.apache.spark" %% "spark-sql" % "2.4.3",
      "net.manub" %% "scalatest-embedded-kafka" % "2.0.0" % Test,
      "org.scalatest" %% "scalatest" % "2.2.2" % Test
    ),
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7",
      "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"
    )
  )
