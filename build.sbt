val scala211Version = "2.11.12"
val scala212Version = "2.12.8"
val akkaVersion = "2.5.19"
val reactiveAwsDynamoDB = "1.0.2"

name := "akka-persistence-dynamodb"

scalaVersion := scala211Version

crossScalaVersions := Seq(scala211Version, scala212Version)

def crossScalacOptions(scalaVersion: String) = CrossVersion.partialVersion(scalaVersion) match {
  case Some((2L, scalaMajor)) if scalaMajor == 12 =>
    Seq.empty
  case Some((2L, scalaMajor)) if scalaMajor <= 11 =>
    Seq(
      "-Yinline-warnings"
    )
}

scalacOptions ++= (Seq(
    "-feature",
    "-deprecation",
    "-unchecked",
    "-encoding",
    "UTF-8",
    "-language:_",
    "-Ydelambdafy:method",
    "-target:jvm-1.8"
  ) ++ crossScalacOptions(scalaVersion.value))


resolvers ++= Seq(
  Resolver.sonatypeRepo("snapshots"),
  Resolver.sonatypeRepo("releases"),
  "DynamoDB Local Repository" at "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"
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
  "org.slf4j" % "jul-to-slf4j" % "1.7.26" % Test
)

parallelExecution in Test := false