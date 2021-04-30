import Dependencies._
import Dependencies.Versions._

ThisBuild / scalafixScalaBinaryVersion := CrossVersion.binaryScalaVersion(scalaVersion.value)

def crossScalacOptions(scalaVersion: String): Seq[String] =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((3L, _)) =>
      Seq(
        "-source:3.0-migration",
        "-Xignore-scala2-macros"
      )
    case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
      Seq(
        "-Ydelambdafy:method",
        "-target:jvm-1.8",
        "-Yrangepos",
        "-Ywarn-unused"
      )
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
  crossScalaVersions := Seq(Versions.scala212Version, Versions.scala213Version, Versions.scala3Version),
  scalacOptions ++= (
    Seq(
      "-unchecked",
      "-feature",
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-language:implicitConversions",
      "language:postfixOps"
    ) ++ crossScalacOptions(scalaVersion.value)
  ),
  resolvers ++= Seq(
    Resolver.sonatypeRepo("snapshots"),
    Resolver.sonatypeRepo("releases"),
    "Seasar Repository" at "https://maven.seasar.org/maven2/",
    "DynamoDB Local Repository" at "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"
  ),
  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision,
  Test / publishArtifact := false,
  Test / fork := true,
  Test / parallelExecution := false,
  envVars := Map(
    "AWS_REGION" -> "ap-northeast-1"
  ),
  Compile / doc / sources := {
    val old = (Compile / doc / sources).value
    if (scalaVersion.value == scala3Version) {
      Nil
    } else {
      old
    }
  }
)

lazy val test = (project in file("test"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-test",
    libraryDependencies ++= Seq(
      amazonaws.dynamodb,
      scalatest.scalatest,
      "org.scalactic" %% "scalactic" % "3.2.9"
    ),
    libraryDependencies ++= Seq(
      iheart.ficus,
      j5ik2o.dockerControllerScalaScalatest excludeAll (
        ExclusionRule(organization = "org.scalatest")
      ),
      j5ik2o.dockerControllerScalaDynamoDBLocal,
      akka.stream
    ).map(_.cross(CrossVersion.for3Use2_13)),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3L, _)) =>
          Seq.empty
        case Some((2L, scalaMajor)) if scalaMajor == 13 =>
          Seq.empty
        case Some((2L, scalaMajor)) if scalaMajor == 12 =>
          Seq(
            "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion
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
      amazonaws.dynamodb,
      amazonaws.dax,
      softwareamazon.dynamodb,
      logback.classic     % Test,
      slf4j.julToSlf4J    % Test,
      scalatest.scalatest % Test,
      "org.scalactic"    %% "scalactic" % "3.2.9"
    ),
    libraryDependencies ++= Seq(
      iheart.ficus,
      akka.slf4j,
      akka.stream,
      akka.testkit % Test excludeAll (
        ExclusionRule(organization = "org.scalatest"),
        ExclusionRule(organization = "org.scalatic")
      ),
      akka.streamTestkit % Test excludeAll (
        ExclusionRule(organization = "org.scalatest"),
        ExclusionRule(organization = "org.scalatic")
      )
    ).map(_.cross(CrossVersion.for3Use2_13)),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3L, _)) =>
          Seq.empty
        case Some((2L, scalaMajor)) if scalaMajor == 13 =>
          Seq.empty
        case Some((2L, scalaMajor)) if scalaMajor == 12 =>
          Seq(
            "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion
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
      "ch.qos.logback"    % "logback-classic" % logbackVersion % Test,
      "org.slf4j"         % "jul-to-slf4j"    % slf4jVersion   % Test,
      scalatest.scalatest % Test
    ),
    libraryDependencies ++= Seq(
      akka.persistence,
      akka.testkit % Test excludeAll (
        ExclusionRule(organization = "org.scalatest"),
        ExclusionRule(organization = "org.scalatic")
      ),
      akka.streamTestkit % Test excludeAll (
        ExclusionRule(organization = "org.scalatest"),
        ExclusionRule(organization = "org.scalatic")
      ),
      akka.persistenceTck % Test excludeAll (
        ExclusionRule(organization = "org.scalatest"),
        ExclusionRule(organization = "org.scalatic")
      )
    ).map(_.cross(CrossVersion.for3Use2_13)),
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
      logback.classic     % Test,
      slf4j.julToSlf4J    % Test,
      scalatest.scalatest % Test
    ),
    libraryDependencies ++= Seq(
      akka.persistence,
      akka.testkit % Test excludeAll (
        ExclusionRule(organization = "org.scalatest"),
        ExclusionRule(organization = "org.scalatic")
      ),
      akka.streamTestkit % Test excludeAll (
        ExclusionRule(organization = "org.scalatest"),
        ExclusionRule(organization = "org.scalatic")
      ),
      akka.persistenceTck % Test excludeAll (
        ExclusionRule(organization = "org.scalatest"),
        ExclusionRule(organization = "org.scalatic")
      )
    ).map(_.cross(CrossVersion.for3Use2_13)),
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
      logback.classic     % Test,
      slf4j.julToSlf4J    % Test,
      scalatest.scalatest % Test
    ),
    libraryDependencies ++= Seq(
      akka.persistenceQuery,
      akka.testkit % Test excludeAll (
        ExclusionRule(organization = "org.scalatest"),
        ExclusionRule(organization = "org.scalatic")
      ),
      akka.streamTestkit % Test excludeAll (
        ExclusionRule(organization = "org.scalatest"),
        ExclusionRule(organization = "org.scalatic")
      ),
      akka.persistenceTck % Test excludeAll (
        ExclusionRule(organization = "org.scalatest"),
        ExclusionRule(organization = "org.scalatic")
      )
    ).map(_.cross(CrossVersion.for3Use2_13)),
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
      slf4j.julToSlf4J
    ),
    libraryDependencies ++= Seq(
      akka.slf4j,
      akka.persistenceTyped,
      dimafeng.testcontainerScala
    ).map(_.cross(CrossVersion.for3Use2_13))
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
