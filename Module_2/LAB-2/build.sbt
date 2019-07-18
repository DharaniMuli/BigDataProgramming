name := "SparkLab"

version := "0.1"

scalaVersion := "2.11.12" //Important: It must be spark version installed in your system
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
"org.apache.spark" %% "spark-graphx" % "2.1.0",
"graphframes" % "graphframes" % "0.6.0-spark2.3-s_2.11"
)