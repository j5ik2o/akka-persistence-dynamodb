import Dependencies._

val scala211Version = "2.11.12"
val scala212Version = "2.12.10"
val scala213Version = "2.13.1"

val scalaCollectionCompatVersion = "2.1.6"

val akka25Version = "2.5.32"
val akka26Version = "2.6.7"

val logbackVersion      = "1.2.3"
val slf4jVersion        = "1.7.30"
val ficusVersion        = "1.5.0"
val awsSdkV1Version     = "1.11.892"
val awsSdkV1DaxVersion  = "1.0.205917.0"
val awsSdkV2Version     = "2.13.75"
val reactiveAwsDynamoDB = "1.2.6"

val scalaTest31Version      = "3.1.1"
val scalaTest30Version      = "3.0.8"
val scalaJava8CompatVersion = "0.9.1"

val reactiveStreamsVersion = "1.0.2"
val nettyVersion           = "4.1.33.Final"

val kafkaVersion        = "2.4.1.1"
val alpakkaKafkaVersion = "2.0.2"

def crossScalacOptions(scalaVersion: String): Seq[String] = CrossVersion.partialVersion(scalaVersion) match {
  case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
    Seq.empty
  case Some((2L, scalaMajor)) if scalaMajor <= 11 =>
    Seq("-Yinline-warnings")
}

lazy val deploySettings = Seq(
  sonatypeProfileName := "com.github.j5ik2o",
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  pomExtra := {
    <url>https://github.com/j5ik2o/akka-persistence-dynamodb</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
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
  credentials := {
    val ivyCredentials = (baseDirectory in LocalRootProject).value / ".credentials"
    val gpgCredentials = (baseDirectory in LocalRootProject).value / ".gpgCredentials"
    Credentials(ivyCredentials) :: Credentials(gpgCredentials) :: Nil
  }
)

lazy val baseSettings = Seq(
  organization := "com.github.j5ik2o",
  scalaVersion := scala213Version,
  crossScalaVersions := Seq(scala211Version, scala212Version, scala213Version),
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
  scalafmtOnCompile in ThisBuild := true,
  envVars := Map(
      "AWS_REGION" -> "ap-northeast-1"
    )
)

lazy val test = (project in file("test"))
  .settings(baseSettings)
  .settings(deploySettings)
  .settings(
    name := "akka-persistence-dynamodb-test",
    libraryDependencies ++= Seq(
        "com.amazonaws"     % "aws-java-sdk-dynamodb"        % awsSdkV1Version,
        "com.github.j5ik2o" %% "reactive-aws-dynamodb-monix" % reactiveAwsDynamoDB,
        "com.github.j5ik2o" %% "reactive-aws-dynamodb-akka"  % reactiveAwsDynamoDB,
        dimafeng.testcontainerScala
      )
  )

lazy val base = (project in file("base"))
  .settings(baseSettings)
  .settings(deploySettings)
  .settings(
    name := "akka-persistence-dynamodb-base",
    libraryDependencies ++= Seq(
        "org.slf4j"                          % "slf4j-api" % slf4jVersion,
        "com.iheart"                         %% "ficus" % ficusVersion,
        "com.amazonaws"                      % "aws-java-sdk-dynamodb" % awsSdkV1Version,
        "com.amazonaws"                      % "amazon-dax-client" % awsSdkV1DaxVersion,
        "software.amazon.awssdk"             % "dynamodb" % awsSdkV2Version,
        "com.github.j5ik2o"                  %% "reactive-aws-dynamodb-monix" % reactiveAwsDynamoDB,
        "com.github.j5ik2o"                  %% "reactive-aws-dynamodb-akka" % reactiveAwsDynamoDB,
        "ch.qos.logback"                     % "logback-classic" % logbackVersion % Test,
        "org.slf4j"                          % "jul-to-slf4j" % slf4jVersion % Test,
        dimafeng.testcontainerScalaScalaTest % Test
      ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
            "com.typesafe.akka"      %% "akka-slf4j"              % akka26Version,
            "com.typesafe.akka"      %% "akka-stream"             % akka26Version,
            "com.typesafe.akka"      %% "akka-testkit"            % akka26Version % Test,
            "com.typesafe.akka"      %% "akka-stream-testkit"     % akka26Version % Test,
            "org.scalatest"          %% "scalatest"               % scalaTest31Version % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor == 11 =>
          Seq(
            "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
            "com.typesafe.akka"      %% "akka-slf4j"              % akka25Version,
            "com.typesafe.akka"      %% "akka-stream"             % akka25Version,
            "com.typesafe.akka"      %% "akka-testkit"            % akka25Version % Test,
            "com.typesafe.akka"      %% "akka-stream-testkit"     % akka25Version % Test,
            "org.scalatest"          %% "scalatest"               % scalaTest30Version % Test
          )
      }
    },
    dependencyOverrides ++= Seq(
        "io.netty"               % "netty-codec-http"    % nettyVersion,
        "io.netty"               % "netty-transport"     % nettyVersion,
        "io.netty"               % "netty-handler"       % nettyVersion,
        "org.reactivestreams"    % "reactive-streams"    % reactiveStreamsVersion,
        "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatVersion
      )
  ).dependsOn(test % "test->test")

lazy val journal = (project in file("journal"))
  .settings(baseSettings)
  .settings(deploySettings)
  .settings(
    name := "akka-persistence-dynamodb-journal",
    libraryDependencies ++= Seq(
        "com.github.j5ik2o" %% "reactive-aws-dynamodb-test" % reactiveAwsDynamoDB % Test,
        "ch.qos.logback"    % "logback-classic"             % logbackVersion      % Test,
        "org.slf4j"         % "jul-to-slf4j"                % slf4jVersion        % Test
      ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            "com.typesafe.akka" %% "akka-persistence"     % akka26Version,
            "com.typesafe.akka" %% "akka-testkit"         % akka26Version % Test,
            "com.typesafe.akka" %% "akka-stream-testkit"  % akka26Version % Test,
            "com.typesafe.akka" %% "akka-persistence-tck" % akka26Version % Test,
            "org.scalatest"     %% "scalatest"            % scalaTest31Version % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor == 11 =>
          Seq(
            "com.typesafe.akka" %% "akka-persistence"     % akka25Version,
            "com.typesafe.akka" %% "akka-testkit"         % akka25Version % Test,
            "com.typesafe.akka" %% "akka-stream-testkit"  % akka25Version % Test,
            "com.typesafe.akka" %% "akka-persistence-tck" % akka25Version % Test,
            "org.scalatest"     %% "scalatest"            % scalaTest30Version % Test
          )
      }
    },
    dependencyOverrides ++= Seq(
        "io.netty"               % "netty-codec-http"    % nettyVersion,
        "io.netty"               % "netty-transport"     % nettyVersion,
        "io.netty"               % "netty-handler"       % nettyVersion,
        "org.reactivestreams"    % "reactive-streams"    % reactiveStreamsVersion,
        "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatVersion
      )
  ).dependsOn(base % "test->test;compile->compile", snapshot % "test->compile")

lazy val snapshot = (project in file("snapshot"))
  .settings(baseSettings)
  .settings(deploySettings)
  .settings(
    name := "akka-persistence-dynamodb-snapshot",
    libraryDependencies ++= Seq(
        "com.github.j5ik2o" %% "reactive-aws-dynamodb-test" % reactiveAwsDynamoDB % Test,
        "ch.qos.logback"    % "logback-classic"             % logbackVersion      % Test,
        "org.slf4j"         % "jul-to-slf4j"                % slf4jVersion        % Test
      ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            "com.typesafe.akka" %% "akka-persistence"     % akka26Version,
            "com.typesafe.akka" %% "akka-testkit"         % akka26Version % Test,
            "com.typesafe.akka" %% "akka-stream-testkit"  % akka26Version % Test,
            "com.typesafe.akka" %% "akka-persistence-tck" % akka26Version % Test,
            "org.scalatest"     %% "scalatest"            % scalaTest31Version % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor == 11 =>
          Seq(
            "com.typesafe.akka" %% "akka-persistence"     % akka25Version,
            "com.typesafe.akka" %% "akka-testkit"         % akka25Version % Test,
            "com.typesafe.akka" %% "akka-stream-testkit"  % akka25Version % Test,
            "com.typesafe.akka" %% "akka-persistence-tck" % akka25Version % Test,
            "org.scalatest"     %% "scalatest"            % scalaTest30Version % Test
          )
      }
    },
    dependencyOverrides ++= Seq(
        "io.netty"               % "netty-codec-http"    % nettyVersion,
        "io.netty"               % "netty-transport"     % nettyVersion,
        "io.netty"               % "netty-handler"       % nettyVersion,
        "org.reactivestreams"    % "reactive-streams"    % reactiveStreamsVersion,
        "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatVersion
      )
  ).dependsOn(base % "test->test;compile->compile")

lazy val query = (project in file("query"))
  .settings(baseSettings)
  .settings(deploySettings)
  .settings(
    name := "akka-persistence-dynamodb-query",
    libraryDependencies ++= Seq(
        "com.github.j5ik2o" %% "reactive-aws-dynamodb-test" % reactiveAwsDynamoDB % Test,
        "ch.qos.logback"    % "logback-classic"             % logbackVersion      % Test,
        "org.slf4j"         % "jul-to-slf4j"                % slf4jVersion        % Test
      ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            "com.typesafe.akka" %% "akka-persistence-query" % akka26Version,
            "com.typesafe.akka" %% "akka-testkit"           % akka26Version % Test,
            "com.typesafe.akka" %% "akka-stream-testkit"    % akka26Version % Test,
            "com.typesafe.akka" %% "akka-persistence-tck"   % akka26Version % Test,
            "org.scalatest"     %% "scalatest"              % scalaTest31Version % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor == 11 =>
          Seq(
            "com.typesafe.akka" %% "akka-persistence-query" % akka25Version,
            "com.typesafe.akka" %% "akka-testkit"           % akka25Version % Test,
            "com.typesafe.akka" %% "akka-stream-testkit"    % akka25Version % Test,
            "com.typesafe.akka" %% "akka-persistence-tck"   % akka25Version % Test,
            "org.scalatest"     %% "scalatest"              % scalaTest30Version % Test
          )
      }
    },
    dependencyOverrides ++= Seq(
        "io.netty"               % "netty-codec-http"    % nettyVersion,
        "io.netty"               % "netty-transport"     % nettyVersion,
        "io.netty"               % "netty-handler"       % nettyVersion,
        "org.reactivestreams"    % "reactive-streams"    % reactiveStreamsVersion,
        "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatVersion
      )
  ).dependsOn(journal % "test->test;compile->compile", snapshot % "test->compile")

lazy val benchmark = (project in file("benchmark"))
  .settings(baseSettings)
  .settings(deploySettings)
  .settings(
    name := "akka-persistence-dynamodb-benchmark",
    skip in publish := true,
    libraryDependencies ++= Seq(
        "ch.qos.logback" % "logback-classic" % logbackVersion,
        "org.slf4j"      % "slf4j-api"       % slf4jVersion,
        "org.slf4j"      % "jul-to-slf4j"    % slf4jVersion,
        dimafeng.testcontainerScala
      ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            "com.typesafe.akka" %% "akka-slf4j"             % akka26Version,
            "com.typesafe.akka" %% "akka-persistence-typed" % akka26Version
          )
        case Some((2L, scalaMajor)) if scalaMajor == 11 =>
          Seq(
            "com.typesafe.akka" %% "akka-slf4j"             % akka25Version,
            "com.typesafe.akka" %% "akka-persistence-typed" % akka25Version
          )
      }
    }
  )
  .enablePlugins(JmhPlugin)
  .dependsOn(test, journal, snapshot)

lazy val root = (project in file("."))
  .settings(baseSettings)
  .settings(deploySettings)
  .settings(
    name := "akka-persistence-dynamodb-root",
    skip in publish := true
  )
  .aggregate(base, journal, snapshot, query, benchmark)
