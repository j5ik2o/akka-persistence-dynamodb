val scala212Version     = "2.12.10"
val scala213Version     = "2.13.1"
val akkaVersion         = "2.6.4"
val reactiveAwsDynamoDB = "1.2.0"

def crossScalacOptions(scalaVersion: String): Seq[String] = CrossVersion.partialVersion(scalaVersion) match {
  case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
    Seq.empty
}

lazy val deploySettings = Seq(
  sonatypeProfileName := "com.github.j5ik2o",
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ =>
    false
  },
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
  },
  publishTo := sonatypePublishToBundle.value,
//  Global / useGpg := false,
  credentials := {
    val ivyCredentials = (baseDirectory in LocalRootProject).value / ".credentials"
    val gpgCredentials = (baseDirectory in LocalRootProject).value / ".gpgCredentials"
    Credentials(ivyCredentials) :: Credentials(gpgCredentials) :: Nil
  },
)

lazy val baseSettings = Seq(
  organization := "com.github.j5ik2o",
  scalaVersion := scala212Version,
  crossScalaVersions := Seq(scala212Version, scala213Version),
  scalacOptions ++= (Seq(
      "-feature",
      "-deprecation",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-language:_",
      "-Ydelambdafy:method",
      "-target:jvm-1.8"
    ) ++ crossScalacOptions(scalaVersion.value)),
  resolvers ++= Seq(
      Resolver.sonatypeRepo("snapshots"),
      Resolver.sonatypeRepo("releases"),
      "Seasar Repository" at "https://maven.seasar.org/maven2/",
      "DynamoDB Local Repository" at "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"
    ),
  parallelExecution in Test := false,
  scalafmtOnCompile in ThisBuild := true
)

lazy val library = (project in file("library"))
  .settings(baseSettings)
  .settings(deploySettings)
  .settings(
    name := "akka-persistence-dynamodb",
    // crossScalaVersions += scala213Version,
    libraryDependencies ++= Seq(
        "com.github.j5ik2o" %% "reactive-aws-dynamodb-monix" % reactiveAwsDynamoDB,
        "com.github.j5ik2o" %% "reactive-aws-dynamodb-akka"  % reactiveAwsDynamoDB,
        "com.typesafe.akka" %% "akka-persistence"            % akkaVersion,
        "com.typesafe.akka" %% "akka-persistence-query"      % akkaVersion,
        "com.typesafe.akka" %% "akka-stream"                 % akkaVersion,
        "com.typesafe.akka" %% "akka-slf4j"                  % akkaVersion,
        "com.github.j5ik2o" %% "reactive-aws-dynamodb-test"  % reactiveAwsDynamoDB % Test,
        "com.typesafe.akka" %% "akka-stream-testkit"         % akkaVersion % Test,
        "com.typesafe.akka" %% "akka-persistence-tck"        % akkaVersion % Test,
        "com.typesafe.akka" %% "akka-testkit"                % akkaVersion % Test,
        "ch.qos.logback"    % "logback-classic"              % "1.2.3" % Test,
        "org.slf4j"         % "jul-to-slf4j"                 % "1.7.30" % Test
      ),
    dependencyOverrides ++= Seq(
        "io.netty"               % "netty-codec-http"    % "4.1.33.Final",
        "io.netty"               % "netty-transport"     % "4.1.33.Final",
        "io.netty"               % "netty-handler"       % "4.1.33.Final",
        "org.reactivestreams"    % "reactive-streams"    % "1.0.2",
        "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0"
      )
  )

lazy val root = (project in file("."))
  .settings(baseSettings)
  .settings(deploySettings)
  .settings(
    skip in publish := true
  )
  .aggregate(library)
