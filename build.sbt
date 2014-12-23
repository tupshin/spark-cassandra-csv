name := "learn-spark-examples"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.2.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.2.0"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.1.0" withSources() withJavadoc()

val gitRepo = "git:https://github.com/databricks/spark-csv"

val g = RootProject(uri(gitRepo))

lazy val root = project in file(".") dependsOn g