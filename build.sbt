val scala211Version     = "2.11.12"
val scala212Version     = "2.12.8"
val akkaVersion         = "2.5.20"
val reactiveAwsDynamoDB = "1.1.0"

sonatypeProfileName := "com.github.j5ik2o"

organization := "com.github.j5ik2o"

name := "akka-persistence-dynamodb"

scalaVersion := scala212Version

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
  "com.github.j5ik2o" %% "reactive-aws-dynamodb-monix" % reactiveAwsDynamoDB,
  "com.github.j5ik2o" %% "reactive-aws-dynamodb-akka"  % reactiveAwsDynamoDB,
  "com.typesafe.akka" %% "akka-persistence"               % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query"         % akkaVersion,
  "com.typesafe.akka" %% "akka-stream"                    % akkaVersion,
  "com.github.j5ik2o" %% "reactive-aws-dynamodb-test"     % reactiveAwsDynamoDB % Test,
  "com.typesafe.akka" %% "akka-stream-testkit"            % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-persistence-tck"           % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-testkit"                   % akkaVersion % Test,
  "ch.qos.logback"    % "logback-classic"                 % "1.2.3" % Test,
  "org.slf4j"         % "jul-to-slf4j"                    % "1.7.26" % Test
)

dependencyOverrides ++= Seq(
  "io.netty" % "netty-codec-http" % "4.1.32.Final",
  "io.netty" % "netty-handler" % "4.1.32.Final",
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0"
)

parallelExecution in Test := false

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { _ =>
  false
}

pomExtra := {
  <url>https://github.com/j5ik2o/akka-persistence-dynamodb</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:j5ik2o/akka-persistence-dynamodb.git</url>
      <connection>scm:git:github.com/j5ik2o/akka-persistence-dynamodb</connection>
      <developerConnection>scm:git:git@github.com:j5ik2o/akka-persistence-dynamodb.git</developerConnection>
    </scm>
    <developers>
      <developer>
        <id>j5ik2o</id>
        <name>Junichi Kato</name>
      </developer>
    </developers>
}

publishTo in ThisBuild := sonatypePublishTo.value

credentials := {
  val ivyCredentials = (baseDirectory in LocalRootProject).value / ".credentials"
  Credentials(ivyCredentials) :: Nil
}

scalafmtOnCompile in ThisBuild := true
