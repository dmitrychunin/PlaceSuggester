name := "PlaceSuggester"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % "test"
