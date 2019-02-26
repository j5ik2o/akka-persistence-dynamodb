name := "akka-persistence-dynamodb"

scalaVersion := "2.12.8"
crossScalaVersions := Seq("2.11.12", "2.12.8")

val akkaVersion = "2.5.19"

libraryDependencies ++= Seq(
  "com.github.pureconfig" %% "pureconfig" % "0.10.2",
  "com.github.j5ik2o" %% "reactive-dynamodb-v2-monix" % "1.0.6",
  "com.github.j5ik2o" %% "reactive-dynamodb-v2-akka" % "1.0.6",
  "com.github.j5ik2o" %% "reactive-dynamodb-test" % "1.0.6" % Test,
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-tck" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Test

)

parallelExecution in Test := false