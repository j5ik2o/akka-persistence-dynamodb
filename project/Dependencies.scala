import sbt._

object Dependencies {

  object testcontainers {
    val testcontainersVersion    = "1.15.0-rc2"
    val testcontainers           = "org.testcontainers" % "testcontainers" % testcontainersVersion
    val testcontainersLocalStack = "org.testcontainers" % "localstack" % testcontainersVersion
    val testcontainersKafka      = "org.testcontainers" % "kafka" % testcontainersVersion
  }

  object dimafeng {
    val testcontainersScalaVersion  = "0.38.4"
    val testcontainerScalaScalaTest = "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion
    //    val testcontainerScalaMsql       = "com.dimafeng" %% "testcontainers-scala-mysql" % testcontainersScalaVersion
    val testcontainerScalaKafka      = "com.dimafeng" %% "testcontainers-scala-kafka"      % testcontainersScalaVersion
    val testcontainerScalaLocalstack = "com.dimafeng" %% "testcontainers-scala-localstack" % testcontainersScalaVersion
  }

}
