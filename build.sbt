name := "akka-persistence-dynamodb"

scalaVersion := "2.12.8"
crossScalaVersions := Seq("2.11.12", "2.12.8")

val akkaVersion = "2.5.19"

libraryDependencies ++= Seq(
  "com.github.j5ik2o" %% "reactive-dynamodb-v2" % "1.0.6",
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-tck" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test
)

parallelExecution in Test := false