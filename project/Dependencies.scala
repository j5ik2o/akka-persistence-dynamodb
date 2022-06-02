import sbt._

object Dependencies {

  object Versions {
    val scala212Version = "2.12.10"
    val scala213Version = "2.13.6"
    val scala3Version   = "3.0.1"

    val scalaCollectionCompatVersion = "2.7.0"

    val akka26Version = "2.6.19"

    val logbackVersion      = "1.2.11"
    val slf4jVersion        = "1.7.36"
    val ficusVersion        = "1.5.2"
    val awsSdkV1Version     = "1.12.233"
    val awsSdkV1DaxVersion  = "1.0.221844.0"
    val awsSdkV2Version     = "2.17.204"
    val reactiveAwsDynamoDB = "1.2.6"

    val scalaTest32Version      = "3.2.4"
    val scalaTest30Version      = "3.0.8"
    val scalaJava8CompatVersion = "0.9.1"

    val reactiveStreamsVersion = "1.0.2"
    val nettyVersion           = "4.1.33.Final"

  }

  import Versions._

  object slf4j {
    val api        = "org.slf4j" % "slf4j-api"    % slf4jVersion
    val julToSlf4J = "org.slf4j" % "jul-to-slf4j" % slf4jVersion
  }

  object iheart {
    val ficus = "com.iheart" %% "ficus" % ficusVersion
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
    def slf4j(version: String)            = "com.typesafe.akka" %% "akka-slf4j"             % version
    def stream(version: String)           = "com.typesafe.akka" %% "akka-stream"            % version
    def testkit(version: String)          = "com.typesafe.akka" %% "akka-testkit"           % version
    def streamTestkit(version: String)    = "com.typesafe.akka" %% "akka-stream-testkit"    % version
    def persistence(version: String)      = "com.typesafe.akka" %% "akka-persistence"       % version
    def persistenceQuery(version: String) = "com.typesafe.akka" %% "akka-persistence-query" % version
    def persistenceTyped(version: String) = "com.typesafe.akka" %% "akka-persistence-typed" % version
    def persistenceTck(version: String)   = "com.typesafe.akka" %% "akka-persistence-tck"   % version
  }

  object scalatest {
    def scalatest(version: String) = "org.scalatest" %% "scalatest" % version
  }

  object testcontainers {
    val testcontainersVersion    = "1.17.2"
    val testcontainers           = "org.testcontainers" % "testcontainers" % testcontainersVersion
    val testcontainersLocalStack = "org.testcontainers" % "localstack"     % testcontainersVersion
    val testcontainersKafka      = "org.testcontainers" % "kafka"          % testcontainersVersion
  }

  object dimafeng {
    val testcontainersScalaVersion  = "0.40.8"
    val testcontainerScala          = "com.dimafeng" %% "testcontainers-scala"           % testcontainersScalaVersion
    val testcontainerScalaScalaTest = "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion
    //    val testcontainerScalaMsql       = "com.dimafeng" %% "testcontainers-scala-mysql" % testcontainersScalaVersion
    val testcontainerScalaKafka      = "com.dimafeng" %% "testcontainers-scala-kafka"      % testcontainersScalaVersion
    val testcontainerScalaLocalstack = "com.dimafeng" %% "testcontainers-scala-localstack" % testcontainersScalaVersion
  }

}
