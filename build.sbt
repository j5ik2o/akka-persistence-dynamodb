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
  licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      id = "j5ik2o",
      name = "Junichi Kato",
      email = "j5ik2o@gmail.com",
      url = url("https://blog.j5ik2o.me")
    )
  ),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/j5ik2o/akka-persistence-dynamodb"),
      "scm:git@github.com:j5ik2o/akka-persistence-dynamodb.git"
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
      akka.stream(akka26Version),
      amazonaws.dynamodb,
      testcontainers.testcontainers,
      dimafeng.testcontainerScala,
      javaDevJnv.jna
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor == 12 =>
          Seq(
            scalaLangModules.scalaCollectionCompat
          )
        case _ =>
          Seq.empty
      }
    }
  )

lazy val `base` = (project in file("base/base"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-base",
    libraryDependencies ++= Seq(
      slf4j.api,
      iheart.ficus,
      akka.slf4j(akka26Version),
      akka.stream(akka26Version),
      akka.testkit(akka26Version)             % Test,
      logback.classic                         % Test,
      slf4j.julToSlf4J                        % Test,
      dimafeng.testcontainerScalaScalaTest    % Test,
      akka.streamTestkit(akka26Version)       % Test,
      scalatest.scalatest(scalaTest32Version) % Test
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor == 12 =>
          Seq(
            scalaLangModules.scalaCollectionCompat
          )
        case _ =>
          Seq.empty
      }
    }
  ).dependsOn(test % "test->compile")

lazy val `base-v1` = (project in file("base/base-v1"))
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

lazy val `base-v2` = (project in file("base/base-v2"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-base-v2",
    libraryDependencies ++= Seq(
      slf4j.api,
      softwareamazon.dynamodb,
      "software.amazon.dax"                % "amazon-dax-client" % "2.0.1",
      logback.classic                      % Test,
      slf4j.julToSlf4J                     % Test,
      dimafeng.testcontainerScalaScalaTest % Test
    )
  ).dependsOn(base, test % "test->compile")

lazy val `journal-base` = (project in file("journal/journal-base"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-journal-base",
    libraryDependencies ++= Seq(
      akka.persistence(akka26Version),
      akka.testkit(akka26Version)             % Test,
      akka.streamTestkit(akka26Version)       % Test,
      akka.persistenceTck(akka26Version)      % Test,
      scalatest.scalatest(scalaTest32Version) % Test,
      "ch.qos.logback"                        % "logback-classic" % logbackVersion % Test,
      "org.slf4j"                             % "jul-to-slf4j"    % slf4jVersion   % Test
    )
  ).dependsOn(base)

lazy val `journal-v1` = (project in file("journal/journal-v1"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-journal-v1",
    libraryDependencies ++= Seq(
      akka.persistence(akka26Version),
      akka.testkit(akka26Version)             % Test,
      akka.streamTestkit(akka26Version)       % Test,
      akka.persistenceTck(akka26Version)      % Test,
      scalatest.scalatest(scalaTest32Version) % Test,
      "ch.qos.logback"                        % "logback-classic" % logbackVersion % Test,
      "org.slf4j"                             % "jul-to-slf4j"    % slf4jVersion   % Test
    )
  ).dependsOn(`journal-base`, base % "test->test", `base-v1`, `snapshot-base` % "test->compile")

lazy val `journal-v2` = (project in file("journal/journal-v2"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-journal-v2",
    libraryDependencies ++= Seq(
      akka.persistence(akka26Version),
      akka.testkit(akka26Version)             % Test,
      akka.streamTestkit(akka26Version)       % Test,
      akka.persistenceTck(akka26Version)      % Test,
      scalatest.scalatest(scalaTest32Version) % Test,
      "ch.qos.logback"                        % "logback-classic" % logbackVersion % Test,
      "org.slf4j"                             % "jul-to-slf4j"    % slf4jVersion   % Test
    )
  ).dependsOn(`journal-base`, base % "test->test", `base-v2`, `snapshot-base` % "test->compile")

lazy val `snapshot-base` = (project in file("snapshot/snapshot-base"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-snapshot-base",
    libraryDependencies ++= Seq(
      akka.persistence(akka26Version)
    )
  ).dependsOn(base)

lazy val `snapshot-v1` = (project in file("snapshot/snapshot-v1"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-snapshot-v1",
    libraryDependencies ++= Seq(
      akka.persistence(akka26Version),
      akka.testkit(akka26Version)             % Test,
      akka.streamTestkit(akka26Version)       % Test,
      akka.persistenceTck(akka26Version)      % Test,
      scalatest.scalatest(scalaTest32Version) % Test,
      logback.classic                         % Test,
      slf4j.julToSlf4J                        % Test
    )
  ).dependsOn(`snapshot-base`, base % "test->test", `base-v1`)

lazy val `snapshot-v2` = (project in file("snapshot/snapshot-v2"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-snapshot-v2",
    libraryDependencies ++= Seq(
      logback.classic  % Test,
      slf4j.julToSlf4J % Test,
      akka.persistence(akka26Version),
      akka.testkit(akka26Version)             % Test,
      akka.streamTestkit(akka26Version)       % Test,
      akka.persistenceTck(akka26Version)      % Test,
      scalatest.scalatest(scalaTest32Version) % Test
    )
  ).dependsOn(`snapshot-base`, base % "test->test", `base-v2`)

lazy val `state-base` = (project in file("state/state-base"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-state-base",
    libraryDependencies ++= Seq(
      akka.persistence(akka26Version),
      akka.testkit(akka26Version)             % Test,
      akka.streamTestkit(akka26Version)       % Test,
      akka.persistenceTck(akka26Version)      % Test,
      scalatest.scalatest(scalaTest32Version) % Test,
      "ch.qos.logback"                        % "logback-classic" % logbackVersion % Test,
      "org.slf4j"                             % "jul-to-slf4j"    % slf4jVersion   % Test
    )
  ).dependsOn(base)

lazy val `state-v1` = (project in file("state/state-v1"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-state-v1",
    libraryDependencies ++= Seq(
      akka.persistence(akka26Version),
      akka.testkit(akka26Version)             % Test,
      akka.streamTestkit(akka26Version)       % Test,
      akka.persistenceTck(akka26Version)      % Test,
      scalatest.scalatest(scalaTest32Version) % Test,
      "ch.qos.logback"                        % "logback-classic" % logbackVersion % Test,
      "org.slf4j"                             % "jul-to-slf4j"    % slf4jVersion   % Test,
      dimafeng.testcontainerScalaScalaTest    % Test
    )
  ).dependsOn(
    base         % "test->test",
    `state-base` % "test->test;compile->compile",
    `base-v1`,
    `snapshot-base` % "test->compile"
  )

lazy val `state-v2` = (project in file("state/state-v2"))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-state-v2",
    libraryDependencies ++= Seq(
      akka.persistence(akka26Version),
      akka.testkit(akka26Version)             % Test,
      akka.streamTestkit(akka26Version)       % Test,
      akka.persistenceTck(akka26Version)      % Test,
      scalatest.scalatest(scalaTest32Version) % Test,
      "ch.qos.logback"                        % "logback-classic" % logbackVersion % Test,
      "org.slf4j"                             % "jul-to-slf4j"    % slf4jVersion   % Test,
      dimafeng.testcontainerScalaScalaTest    % Test
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
      dimafeng.testcontainerScala,
      akka.slf4j(akka26Version),
      akka.persistenceTyped(akka26Version)
    )
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
      fasterxml.jacksonModuleScala,
      akka.slf4j(akka26Version),
      akka.persistenceTyped(akka26Version),
      akka.serializationJackson(akka26Version),
      scalatest.scalatest(scalaTest32Version) % Test,
      akka.persistenceTestkit(akka26Version)  % Test,
      "com.github.j5ik2o"                    %% "docker-controller-scala-scalatest"      % "1.14.52",
      "com.github.j5ik2o"                    %% "docker-controller-scala-dynamodb-local" % "1.14.52"
    )
  )
  .dependsOn(
    test,
    base % "test->test",
    `journal-v1`,
    `journal-v2`,
    `snapshot-v1`,
    `snapshot-v2`,
    `state-v1`,
    `state-v2`
  )

lazy val docs = (project in file("docs"))
  .enablePlugins(SphinxPlugin, SiteScaladocPlugin, GhpagesPlugin, PreprocessPlugin)
  .settings(baseSettings)
  .settings(
    name := "documents",
    git.remoteRepo := "git@github.com:j5ik2o/akka-persistence-dynamodb.git",
    ghpagesNoJekyll := true
  )

lazy val root = (project in file("."))
  .settings(baseSettings)
  .settings(
    name := "akka-persistence-dynamodb-root",
    publish / skip := true
  )
  .aggregate(
    docs,
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
