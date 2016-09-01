name := "spark-es-csv"

version := "1.0"

scalaVersion := "2.10.5"

resolvers += Resolver.mavenLocal

javacOptions ++= Seq("-encoding", "UTF-8")


libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"  % "provided"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.5.2"  % "provided"

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.5.2"  % "provided"

libraryDependencies += "org.joda" % "joda-convert" % "1.5"  % "provided"

libraryDependencies += "org.mortbay.jetty" % "servlet-api" % "2.5-20081211" % "provided"

libraryDependencies += "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.3.2"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.2"   % "provided"