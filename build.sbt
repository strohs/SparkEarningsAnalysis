name := "SparkEarningsAnalysis"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-mllib" % "2.1.0",
  "com.typesafe.play" %% "play-json" % "2.5.12")
    