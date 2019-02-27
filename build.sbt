name := "akka-persistence-dynamodb"

scalaVersion := "2.12.8"
crossScalaVersions := Seq("2.11.12", "2.12.8")

val akkaVersion = "2.5.19"
val reactiveAwsDynamoDB = "1.0.2"

resolvers ++= Seq(
  Resolver.sonatypeRepo("snapshots"),
  Resolver.sonatypeRepo("releases"),
  "dnvriend" at "http://dl.bintray.com/dnvriend/maven"
)

libraryDependencies ++= Seq(
  "com.github.j5ik2o" %% "reactive-aws-dynamodb-v2-monix" % reactiveAwsDynamoDB,
  "com.github.j5ik2o" %% "reactive-aws-dynamodb-v2-akka" % reactiveAwsDynamoDB,
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.github.j5ik2o" %% "reactive-aws-dynamodb-test" % reactiveAwsDynamoDB % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-persistence-tck" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Test,
  "com.github.dnvriend" %% "akka-persistence-inmemory" % "2.5.15.1" % Test,
  "org.iq80.leveldb" % "leveldb" % "0.9" % Test,
  "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8" % Test
)

parallelExecution in Test := false