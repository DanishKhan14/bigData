name := "sparkStreaming"

version := "0.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter_2.10" % "1.5.0"

libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3" 

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
