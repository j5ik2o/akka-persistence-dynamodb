import sbt._

object Dependencies {

  object Versions {
    val scala212Version = "2.12.10"
    val scala213Version = "2.13.6"
    val scala3Version   = "3.0.0"

    val scalaCollectionCompatVersion = "2.4.4"

    val akkaVersion = "2.6.10"

    val logbackVersion      = "1.2.0"
    val slf4jVersion        = "1.7.30"
    val ficusVersion        = "1.5.0"
    val awsSdkV1Version     = "1.11.1027"
    val awsSdkV1DaxVersion  = "1.0.221844.0"
    val awsSdkV2Version     = "2.16.71"
    val reactiveAwsDynamoDB = "1.2.6"

    val scalaTestVersion        = "3.2.9"
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
    def slf4j            = "com.typesafe.akka" %% "akka-slf4j"             % akkaVersion
    def stream           = "com.typesafe.akka" %% "akka-stream"            % akkaVersion
    def testkit          = "com.typesafe.akka" %% "akka-testkit"           % akkaVersion
    def streamTestkit    = "com.typesafe.akka" %% "akka-stream-testkit"    % akkaVersion
    def persistence      = "com.typesafe.akka" %% "akka-persistence"       % akkaVersion
    def persistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion
    def persistenceTyped = "com.typesafe.akka" %% "akka-persistence-typed" % akkaVersion
    def persistenceTck   = "com.typesafe.akka" %% "akka-persistence-tck"   % akkaVersion
  }

  object scalatest {
    def scalatest = "org.scalatest" %% "scalatest" % scalaTestVersion
  }

  object j5ik2o {
    val version = "1.1.1"

    val dockerControllerScalaScalatest =
      "com.github.j5ik2o" %% "docker-controller-scala-scalatest" % version

    val dockerControllerScalaDynamoDBLocal =
      "com.github.j5ik2o" %% "docker-controller-scala-dynamodb-local" % version

  }

  object testcontainers {
    val testcontainersVersion    = "1.15.3"
    val testcontainers           = "org.testcontainers" % "testcontainers" % testcontainersVersion
    val testcontainersLocalStack = "org.testcontainers" % "localstack"     % testcontainersVersion
    val testcontainersKafka      = "org.testcontainers" % "kafka"          % testcontainersVersion
  }

  object dimafeng {
    val testcontainersScalaVersion  = "0.39.5"
    val testcontainerScala          = "com.dimafeng" %% "testcontainers-scala"           % testcontainersScalaVersion
    val testcontainerScalaScalaTest = "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion
    //    val testcontainerScalaMsql       = "com.dimafeng" %% "testcontainers-scala-mysql" % testcontainersScalaVersion
    val testcontainerScalaKafka      = "com.dimafeng" %% "testcontainers-scala-kafka"      % testcontainersScalaVersion
    val testcontainerScalaLocalstack = "com.dimafeng" %% "testcontainers-scala-localstack" % testcontainersScalaVersion
  }

}
