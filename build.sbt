import Dependencies._
import Dependencies.Versions._

def crossScalacOptions(scalaVersion: String): Seq[String] = CrossVersion.partialVersion(scalaVersion) match {
  case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
    Seq.empty
  case Some((2L, scalaMajor)) if scalaMajor <= 11 =>
    Seq("-Yinline-warnings")
}

lazy val baseSettings = Seq(
  organization := "com.github.j5ik2o",
  homepage := Some(url("https://github.com/j5ik2o/akka-persistence-dynamodb")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      id = "j5ik2o",
      name = "Junichi Kato",
      email = "j5ik2o@gmail.com",
      url = url("https://blog.j5ik2o.me")
    )
  ),
  scalaVersion := Versions.scala213Version,
  crossScalaVersions := Seq(Versions.scala211Version, Versions.scala212Version, Versions.scala213Version),
  scalacOptions ++= (
    Seq(
      "-feature",
      "-deprecation",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-language:_",
      "-Ydelambdafy:method",
      "-target:jvm-1.8",
      "-Yrangepos",
      "-Ywarn-unused"
    ) ++ crossScalacOptions(scalaVersion.value)
  ),
  resolvers ++= Seq(
    Resolver.sonatypeRepo("snapshots"),
    Resolver.sonatypeRepo("releases"),
    "Seasar Repository" at "https://maven.seasar.org/maven2/",
    "DynamoDB Local Repository" at "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"
  ),
  ThisBuild / scalafixScalaBinaryVersion := CrossVersion.binaryScalaVersion(scalaVersion.value),
  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision,
  Test / publishArtifact := false,
  Test / fork := true,
  Test / parallelExecution := false,
  envVars := Map(
    "AWS_REGION" -> "ap-northeast-1"
  )
)

lazy val test = (project in file("test"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-test",
    libraryDependencies ++= Seq(
      iheart.ficus,
      amazonaws.dynamodb,
      testcontainers.testcontainers,
      dimafeng.testcontainerScala
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor == 13 =>
          Seq(
            akka.stream(akka26Version)
          )
        case Some((2L, scalaMajor)) if scalaMajor == 12 =>
          Seq(
            "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
            akka.stream(akka26Version)
          )
        case Some((2L, scalaMajor)) if scalaMajor == 11 =>
          Seq(
            "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
            akka.stream(akka25Version)
          )
      }
    }
  )

lazy val base = (project in file("base"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-base",
    libraryDependencies ++= Seq(
      slf4j.api,
      iheart.ficus,
      amazonaws.dynamodb,
      amazonaws.dax,
      softwareamazon.dynamodb,
      logback.classic                      % Test,
      slf4j.julToSlf4J                     % Test,
      dimafeng.testcontainerScalaScalaTest % Test
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor == 13 =>
          Seq(
            akka.slf4j(akka26Version),
            akka.stream(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor == 12 =>
          Seq(
            "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
            akka.slf4j(akka26Version),
            akka.stream(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor == 11 =>
          Seq(
            "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
            akka.slf4j(akka25Version),
            akka.stream(akka25Version),
            akka.testkit(akka25Version)             % Test,
            akka.streamTestkit(akka25Version)       % Test,
            scalatest.scalatest(scalaTest30Version) % Test
          )
      }
    },
    dependencyOverrides ++= Seq(
      "io.netty"                % "netty-codec-http"   % nettyVersion,
      "io.netty"                % "netty-transport"    % nettyVersion,
      "io.netty"                % "netty-handler"      % nettyVersion,
      "org.reactivestreams"     % "reactive-streams"   % reactiveStreamsVersion,
      "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatVersion
    )
  ).dependsOn(test % "test->compile")

lazy val journal = (project in file("journal"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-journal",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion % Test,
      "org.slf4j"      % "jul-to-slf4j"    % slf4jVersion   % Test
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor == 11 =>
          Seq(
            akka.persistence(akka25Version),
            akka.testkit(akka25Version)             % Test,
            akka.streamTestkit(akka25Version)       % Test,
            akka.persistenceTck(akka25Version)      % Test,
            scalatest.scalatest(scalaTest30Version) % Test
          )
      }
    },
    dependencyOverrides ++= Seq(
      "io.netty"                % "netty-codec-http"   % nettyVersion,
      "io.netty"                % "netty-transport"    % nettyVersion,
      "io.netty"                % "netty-handler"      % nettyVersion,
      "org.reactivestreams"     % "reactive-streams"   % reactiveStreamsVersion,
      "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatVersion
    )
  ).dependsOn(test % "test->compile", base % "test->test;compile->compile", snapshot % "test->compile")

lazy val snapshot = (project in file("snapshot"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-snapshot",
    libraryDependencies ++= Seq(
      logback.classic  % Test,
      slf4j.julToSlf4J % Test
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor == 11 =>
          Seq(
            akka.persistence(akka25Version),
            akka.testkit(akka25Version)             % Test,
            akka.streamTestkit(akka25Version)       % Test,
            akka.persistenceTck(akka25Version)      % Test,
            scalatest.scalatest(scalaTest30Version) % Test
          )
      }
    },
    dependencyOverrides ++= Seq(
      "io.netty"                % "netty-codec-http"   % nettyVersion,
      "io.netty"                % "netty-transport"    % nettyVersion,
      "io.netty"                % "netty-handler"      % nettyVersion,
      "org.reactivestreams"     % "reactive-streams"   % reactiveStreamsVersion,
      "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatVersion
    )
  ).dependsOn(test % "test->compile", base % "test->test;compile->compile")

lazy val query = (project in file("query"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-query",
    libraryDependencies ++= Seq(
      logback.classic  % Test,
      slf4j.julToSlf4J % Test
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            akka.persistenceQuery(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor == 11 =>
          Seq(
            akka.persistenceQuery(akka25Version),
            akka.testkit(akka25Version)             % Test,
            akka.streamTestkit(akka25Version)       % Test,
            akka.persistenceTck(akka25Version)      % Test,
            scalatest.scalatest(scalaTest30Version) % Test
          )
      }
    },
    dependencyOverrides ++= Seq(
      "io.netty"                % "netty-codec-http"   % nettyVersion,
      "io.netty"                % "netty-transport"    % nettyVersion,
      "io.netty"                % "netty-handler"      % nettyVersion,
      "org.reactivestreams"     % "reactive-streams"   % reactiveStreamsVersion,
      "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatVersion
    )
  ).dependsOn(test % "test->compile", journal % "test->test;compile->compile", snapshot % "test->compile")

lazy val benchmark = (project in file("benchmark"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-benchmark",
    publish / skip := true,
    libraryDependencies ++= Seq(
      logback.classic,
      slf4j.api,
      slf4j.julToSlf4J,
      dimafeng.testcontainerScala
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            akka.slf4j(akka26Version),
            akka.persistenceTyped(akka26Version)
          )
        case _ =>
          Seq(
            akka.slf4j(akka25Version),
            akka.persistence(akka25Version)
          )
      }
    }
  )
  .enablePlugins(JmhPlugin)
  .dependsOn(test, journal, snapshot)

lazy val root = (project in file("."))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-root",
    publish / skip := true
  )
  .aggregate(test, base, journal, snapshot, query, benchmark)

// --- Custom commands
addCommandAlias("lint", ";scalafmtCheck;test:scalafmtCheck;scalafmtSbtCheck;scalafixAll --check")
addCommandAlias("fmt", ";scalafmtAll;scalafmtSbt")
