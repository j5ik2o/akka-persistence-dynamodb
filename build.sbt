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
  crossScalaVersions := Seq(
    Versions.scala212Version,
    Versions.scala213Version
  ),
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
  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision,
  Test / publishArtifact := false,
  Test / fork := true,
  Test / parallelExecution := false,
  Compile / doc / sources := {
    val old = (Compile / doc / sources).value
    if (scalaVersion.value == scala3Version) {
      Nil
    } else {
      old
    }
  },
  envVars := Map(
    "AWS_REGION" -> "ap-northeast-1"
  )
)

lazy val test = (project in file("test"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-test",
    libraryDependencies ++= Seq(
      amazonaws.dynamodb,
      testcontainers.testcontainers,
      dimafeng.testcontainerScala,
      "net.java.dev.jna" % "jna" % "5.11.0"
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3L, scalaMajor)) =>
          Seq(
            akka.stream(akka26Version)
          )
        case Some((2L, scalaMajor)) if scalaMajor == 13 =>
          Seq(
            akka.stream(akka26Version)
          )
        case Some((2L, scalaMajor)) if scalaMajor == 12 =>
          Seq(
            "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
            akka.stream(akka26Version)
          )
      }
    }
  )

lazy val `base` = (project in file("base"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-base",
    libraryDependencies ++= Seq(
      slf4j.api,
      logback.classic                      % Test,
      slf4j.julToSlf4J                     % Test,
      dimafeng.testcontainerScalaScalaTest % Test
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3L, _)) =>
          Seq(
            akka.slf4j(akka26Version),
            akka.stream(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
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

lazy val `base-v1` = (project in file("base-v1"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-base-v1",
    libraryDependencies ++= Seq(
      slf4j.api,
      amazonaws.dynamodb,
      amazonaws.dax,
      logback.classic                      % Test,
      slf4j.julToSlf4J                     % Test,
      dimafeng.testcontainerScalaScalaTest % Test
    )
  ).dependsOn(base, test % "test->compile")

lazy val `base-v2` = (project in file("base-v2"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-base-v2",
    libraryDependencies ++= Seq(
      slf4j.api,
      softwareamazon.dynamodb,
      logback.classic                      % Test,
      slf4j.julToSlf4J                     % Test,
      dimafeng.testcontainerScalaScalaTest % Test
    )
  ).dependsOn(base, test % "test->compile")

lazy val `journal-base` = (project in file("journal-base"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-journal-base",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion % Test,
      "org.slf4j"      % "jul-to-slf4j"    % slf4jVersion   % Test
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3L, _)) =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
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
  ).dependsOn(base)

lazy val `journal-v1` = (project in file("journal-v1"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-journal-v1",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion % Test,
      "org.slf4j"      % "jul-to-slf4j"    % slf4jVersion   % Test
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3L, _)) =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
      }
    }
  ).dependsOn(`journal-base`, base % "test->test", `base-v1`, `snapshot-base` % "test->compile")

lazy val `journal-v2` = (project in file("journal-v2"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-journal-v2",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion % Test,
      "org.slf4j"      % "jul-to-slf4j"    % slf4jVersion   % Test
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3L, _)) =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
      }
    }
  ).dependsOn(`journal-base`, base % "test->test", `base-v2`, `snapshot-base` % "test->compile")

lazy val `snapshot-base` = (project in file("snapshot-base"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-snapshot-base",
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3L, _)) =>
          Seq(
            akka.persistence(akka26Version)
          )
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            akka.persistence(akka26Version)
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
  ).dependsOn(base)

lazy val `snapshot-v1` = (project in file("snapshot-v1"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-snapshot-v1",
    libraryDependencies ++= Seq(
      logback.classic  % Test,
      slf4j.julToSlf4J % Test
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3L, _)) =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
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
  ).dependsOn(`snapshot-base`, base % "test->test", `base-v1`)

lazy val `snapshot-v2` = (project in file("snapshot-v2"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-snapshot-v2",
    libraryDependencies ++= Seq(
      logback.classic  % Test,
      slf4j.julToSlf4J % Test
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3L, _)) =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
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
  ).dependsOn(`snapshot-base`, base % "test->test", `base-v2`)

lazy val `state-base` = (project in file("state-base"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-state-base",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion % Test,
      "org.slf4j"      % "jul-to-slf4j"    % slf4jVersion   % Test
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3L, _)) =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
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
  ).dependsOn(base)

lazy val `state-v1` = (project in file("state-v1"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-state-v1",
    libraryDependencies ++= Seq(
      "ch.qos.logback"                     % "logback-classic" % logbackVersion % Test,
      "org.slf4j"                          % "jul-to-slf4j"    % slf4jVersion   % Test,
      dimafeng.testcontainerScalaScalaTest % Test
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3L, _)) =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
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
  ).dependsOn(
    base         % "test->test",
    `state-base` % "test->test;compile->compile",
    `base-v1`,
    `snapshot-base` % "test->compile"
  )

lazy val `state-v2` = (project in file("state-v2"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-state-v2",
    libraryDependencies ++= Seq(
      "ch.qos.logback"                     % "logback-classic" % logbackVersion % Test,
      "org.slf4j"                          % "jul-to-slf4j"    % slf4jVersion   % Test,
      dimafeng.testcontainerScalaScalaTest % Test
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3L, _)) =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
          )
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            akka.persistence(akka26Version),
            akka.testkit(akka26Version)             % Test,
            akka.streamTestkit(akka26Version)       % Test,
            akka.persistenceTck(akka26Version)      % Test,
            scalatest.scalatest(scalaTest32Version) % Test
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
  ).dependsOn(
    base         % "test->test",
    `state-base` % "test->test;compile->compile",
    `base-v2`,
    `snapshot-base` % "test->compile"
  )

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
        case Some((3L, _)) =>
          Seq(
            akka.slf4j(akka26Version),
            akka.persistenceTyped(akka26Version)
          )
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            akka.slf4j(akka26Version),
            akka.persistenceTyped(akka26Version)
          )
      }
    }
  )
  .enablePlugins(JmhPlugin)
  .dependsOn(test, `journal-v1`, `journal-v2`, `snapshot-v1`, `snapshot-v2`)

lazy val example = (project in file("example"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-example",
    publish / skip := true,
    libraryDependencies ++= Seq(
      logback.classic,
      slf4j.api,
      slf4j.julToSlf4J,
      dimafeng.testcontainerScala,
      fasterxml.jacksonModuleScala
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3L, _)) =>
          Seq(
            akka.slf4j(akka26Version),
            akka.persistenceTyped(akka26Version),
            akka.serializationJackson(akka26Version)
          )
        case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
          Seq(
            akka.slf4j(akka26Version),
            akka.persistenceTyped(akka26Version),
            akka.serializationJackson(akka26Version)
          )
      }
    }
  )
  .dependsOn(test, `journal-v1`, `journal-v2`, `snapshot-v1`, `snapshot-v2`, `state-v1`, `state-v2`)

lazy val root = (project in file("."))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-root",
    publish / skip := true
  )
  .aggregate(
    test,
    base,
    `base-v1`,
    `base-v2`,
    `journal-base`,
    `journal-v1`,
    `journal-v2`,
    `snapshot-base`,
    `snapshot-v1`,
    `snapshot-v2`,
    `state-base`,
    `state-v1`,
    `state-v2`,
    benchmark,
    example
  )

// --- Custom commands
addCommandAlias("lint", ";scalafmtCheck;test:scalafmtCheck;scalafmtSbtCheck;scalafixAll --check")
addCommandAlias("fmt", ";scalafmtAll;scalafmtSbt;scalafix RemoveUnused")
