name := "SparkTwoExperiments"

version := "0.13.5"

scalaVersion := "2.11.9"

val sparkVersion = "2.1.1"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
  "org.apache.spark" %% "spark-core" % "2.1.1",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
  "org.apache.spark" %% "spark-sql" % "2.1.1",

  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.1.3"
  
) 