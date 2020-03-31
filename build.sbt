name := "etl"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.5"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.4.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % "test"
libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.30.1"
