import sbt._

object Dependencies {

  object Versions {
    val scala213Version = "2.13.14"
    val scala3Version   = "3.3.3"

    val scalaCollectionCompatVersion = "2.12.0"

    val akkaVersion = "2.9.5"

    val logbackVersion      = "1.5.12"
    val slf4jVersion        = "1.7.36"
    val ficusVersion        = "1.5.2"
    val awsSdkV1Version     = "1.12.779"
    val awsSdkV1DaxVersion  = "1.0.230341.0"
    val awsSdkV2Version     = "2.29.26"
    val reactiveAwsDynamoDB = "1.2.6"

    val scalaTest32Version      = "3.2.16"
    val scalaJava8CompatVersion = "1.0.2"

    val reactiveStreamsVersion = "1.0.3"
    val nettyVersion           = "4.1.33.Final"

  }

  import Versions._

  object iheart {
    val ficus = "com.iheart" %% "ficus" % ficusVersion
  }

  object slf4j {
    val api        = "org.slf4j" % "slf4j-api"    % slf4jVersion
    val julToSlf4J = "org.slf4j" % "jul-to-slf4j" % slf4jVersion
  }

  object fasterxml {

    val jacksonModuleScala = "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.18.2"

  }

  object softwareamazon {
    val dynamodb = "software.amazon.awssdk" % "dynamodb" % awsSdkV2Version
  }

  object logback {
    val classic = "ch.qos.logback" % "logback-classic" % logbackVersion
  }

  object amazonaws {
    val dynamodb = "com.amazonaws" % "aws-java-sdk-dynamodb" % awsSdkV1Version
    val dax      = "com.amazonaws" % "amazon-dax-client"     % awsSdkV1DaxVersion
  }

  object akka {
    def slf4j(version: String): ModuleID                = "com.typesafe.akka" %% "akka-slf4j"                 % version
    def stream(version: String): ModuleID               = "com.typesafe.akka" %% "akka-stream"                % version
    def testkit(version: String): ModuleID              = "com.typesafe.akka" %% "akka-testkit"               % version
    def streamTestkit(version: String): ModuleID        = "com.typesafe.akka" %% "akka-stream-testkit"        % version
    def persistence(version: String): ModuleID          = "com.typesafe.akka" %% "akka-persistence"           % version
    def persistenceTestkit(version: String): ModuleID   = "com.typesafe.akka" %% "akka-persistence-testkit"   % version
    def persistenceQuery(version: String): ModuleID     = "com.typesafe.akka" %% "akka-persistence-query"     % version
    def persistenceTyped(version: String): ModuleID     = "com.typesafe.akka" %% "akka-persistence-typed"     % version
    def persistenceTck(version: String): ModuleID       = "com.typesafe.akka" %% "akka-persistence-tck"       % version
    def serializationJackson(version: String): ModuleID = "com.typesafe.akka" %% "akka-serialization-jackson" % version
  }

  object scalatest {
    def scalatest(version: String) = "org.scalatest" %% "scalatest" % version
  }

  object testcontainers {
    val testcontainersVersion    = "1.20.4"
    val testcontainers           = "org.testcontainers" % "testcontainers" % testcontainersVersion
    val testcontainersLocalStack = "org.testcontainers" % "localstack"     % testcontainersVersion
    val testcontainersKafka      = "org.testcontainers" % "kafka"          % testcontainersVersion
  }

  object dimafeng {
    val testcontainersScalaVersion  = "0.41.4"
    val testcontainerScala          = "com.dimafeng" %% "testcontainers-scala"           % testcontainersScalaVersion
    val testcontainerScalaScalaTest = "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion
    //    val testcontainerScalaMsql       = "com.dimafeng" %% "testcontainers-scala-mysql" % testcontainersScalaVersion
    val testcontainerScalaKafka      = "com.dimafeng" %% "testcontainers-scala-kafka"      % testcontainersScalaVersion
    val testcontainerScalaLocalstack = "com.dimafeng" %% "testcontainers-scala-localstack" % testcontainersScalaVersion
  }

  object javaDevJnv {
    val jna = "net.java.dev.jna" % "jna" % "5.15.0"
  }

  object scalaLangModules {
    val scalaCollectionCompat = "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
  }

}
