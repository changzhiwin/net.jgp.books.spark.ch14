//
name := "main/scala/ch14"

version := "1.0"
scalaVersion := "2.13.8"

// The “provided” keyword indicates that the dependency is provided by the runtime, so there’s no need to include it in the JAR file.
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided",
  "org.apache.spark" %% "spark-core" % "3.2.1" % "provided",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
)