name := "SparkLab"

version := "0.1"

scalaVersion := "2.11.12" //Important: It must be spark version installed in your system
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0"
)