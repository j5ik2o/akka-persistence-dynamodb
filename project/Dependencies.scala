import sbt._

object Scala {
  val java8Compat = "org.scala-lang.modules" %% "scala-java8-compat" % "0.8.0"
}

object Akka {
  private val version     = "2.5.19"
  val actor: ModuleID     = "com.typesafe.akka" %% "akka-actor" % version
  val stream: ModuleID    = "com.typesafe.akka" %% "akka-stream" % version
  val testkit: ModuleID   = "com.typesafe.akka" %% "akka-testkit" % version
  val slf4j: ModuleID     = "com.typesafe.akka" %% "akka-slf4j" % version
  val persistence = "com.typesafe.akka" %% "akka-persistence" % version
  val cluster = "com.typesafe.akka" %% "akka-cluster" % version
  val clusterSharding = "com.typesafe.akka" %% "akka-cluster-sharding" % version

  private val httpVersion = "10.1.7"
  val http                = "com.typesafe.akka" %% "akka-http" % httpVersion
  val httpTestKit         = "com.typesafe.akka" %% "akka-http-testkit" % httpVersion

  private val akkaManagementVersion = "0.18.0"
  val discoveryK8sAPI = "com.lightbend.akka.discovery" %% "akka-discovery-kubernetes-api" % akkaManagementVersion
  val discoveryDns = "com.lightbend.akka.discovery" %% "akka-discovery-dns" % akkaManagementVersion
  val management = "com.lightbend.akka.management" %% "akka-management" % akkaManagementVersion
  val managementClusterHttp = "com.lightbend.akka.management" %% "akka-management-cluster-http" % akkaManagementVersion
  val managementClusterBootstrap = "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % akkaManagementVersion
  val clusterCustomDowning = "com.github.TanUkkii007" %% "akka-cluster-custom-downing" % "0.0.12"
  val managementAll = Seq(discoveryK8sAPI, discoveryDns, management, managementClusterHttp, managementClusterBootstrap, clusterCustomDowning)
}


object Redis {
  val rediscala: ModuleID = "com.github.etaty" %% "rediscala"     % "1.8.0"
  val embRedis: ModuleID  = "com.chatwork"     % "embedded-redis" % "0.7"
}

object ScalaMock {
  val version = "org.scalamock" %% "scalamock" % "4.1.0"
}

object Circe {
  private val version   = "0.11.1"
  val core: ModuleID    = "io.circe" %% "circe-core" % version
  val parser: ModuleID  = "io.circe" %% "circe-parser" % version
  val generic: ModuleID = "io.circe" %% "circe-generic" % version
  val extras: ModuleID  = "io.circe" %% "circe-generic-extras" % version

}

object Slick {
  val version            = "3.2.3"
  val slick: ModuleID    = "com.typesafe.slick" %% "slick" % version
  val hikaricp: ModuleID = "com.typesafe.slick" %% "slick-hikaricp" % version
}

object Monix {
  val monixVersion = "3.0.0-RC2"
  val version      = "io.monix" %% "monix" % monixVersion
}

object Logback {
  private val version   = "1.2.3"
  val classic: ModuleID = "ch.qos.logback" % "logback-classic" % version
}

object LogstashLogbackEncoder {
  private val version = "4.11"
  val encoder = "net.logstash.logback" % "logstash-logback-encoder" % version excludeAll (
    ExclusionRule(organization = "com.fasterxml.jackson.core", name = "jackson-core"),
    ExclusionRule(organization = "com.fasterxml.jackson.core", name = "jackson-databind")
  )
}

object Janino {
  val version: ModuleID = "org.codehaus.janino" % "janino" % "2.7.8"
}

object ScalaLogging {
  val version      = "3.5.0"
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % version
}

object ChatWork {

  object AkkaGuard {
    val version = "1.4.0"
    val http = "com.chatwork" %% "akka-guard-http" % version excludeAll (
      ExclusionRule(organization = "io.circe"),
      ExclusionRule(organization = "org.typelevel"),
      ExclusionRule(organization = "com.typesafe.akka")
    )
  }

}

object Commons {
  private val version = "1.10"
  val codec           = "commons-codec" % "commons-codec" % version
}

object ScalaTest {
  val version = "org.scalatest" %% "scalatest" % "3.0.5"
}

object MySQLConnectorJava {
  val version = "mysql" % "mysql-connector-java" % "5.1.42"
}

object Cats {
  val version = "org.typelevel" %% "cats-core" % "1.5.0"
}

object ScalaTestPlusDB {
  val version = "com.github.j5ik2o" %% "scalatestplus-db" % "1.0.7"
}

object ScalaDddBase {
  val version  = "com.github.j5ik2o" %% "scala-ddd-base-core"     % "1.0.22"
  val slick    = "com.github.j5ik2o" %% "scala-ddd-base-slick"    % "1.0.22"
  val dynamodb = "com.github.j5ik2o" %% "scala-ddd-base-dynamodb" % "1.0.22"
}

object ReactiveDynamoDB {
  val test = "com.github.j5ik2o" %% "reactive-dynamodb-test" % "1.0.6"
}

object Airframe {
  private val version = "0.80"
  val di              = "org.wvlet.airframe" %% "airframe" % version
}

object TypesafeConfig {
  val version = "com.typesafe" % "config" % "1.3.1"
}

object Shapeless {
  val version = "com.chuusai" %% "shapeless" % "2.3.3"
}

object SeasarUtil {
  val version = "org.seasar.util" % "s2util" % "0.0.1"
}

object Enumeratum {
  val version = "com.beachape" %% "enumeratum" % "1.5.12"
}

object Passay {
  val latest = "org.passay" % "passay" % "1.3.0"
}

//object AspectJ {
//  val version = "org.aspectj" % "aspectjweaver" % "1.8.13"
//}

object Refined {
  val refined    = "eu.timepit" %% "refined" % "0.9.4"
  val cats       = "eu.timepit" %% "refined-cats" % "0.9.4"
  val eval       = "eu.timepit" %% "refined-eval" % "0.9.4"
  val jsonpath   = "eu.timepit" %% "refined-jsonpath" % "0.9.4"
  val pureconfig = "eu.timepit" %% "refined-pureconfig" % "0.9.4"
  val scalacheck = "eu.timepit" %% "refined-scalacheck" % "0.9.4"
  val all        = Seq(refined, cats, eval, jsonpath, pureconfig)
}

object Kamon {

  val core = "io.kamon" %% "kamon-core" % "1.1.3" excludeAll (
    ExclusionRule(organization = "log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "org.scala-lang.modules")
  )

  val systemMetrics = "io.kamon" %% "kamon-system-metrics" % "1.0.1" excludeAll (
    ExclusionRule(organization = "log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "org.scala-lang.modules")
  )

  val akka = "io.kamon" %% "kamon-akka-2.5" % "1.0.0" excludeAll (
    ExclusionRule(organization = "log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "org.scala-lang.modules")
  )

  val akkaHttp = "io.kamon" %% "kamon-akka-http-2.5" % "1.0.0" excludeAll (
    ExclusionRule(organization = "log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "org.scala-lang.modules"),
    ExclusionRule(organization = "com.typesafe.akka")
  )

  val jmx = "io.kamon" %% "kamon-jmx-collector" % "0.1.8" excludeAll (
    ExclusionRule(organization = "log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "org.scala-lang.modules")
  )

  val datadog = "io.kamon" %% "kamon-datadog" % "1.0.0" excludeAll (
    ExclusionRule(organization = "log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "org.scala-lang.modules")
  )

  val logback = "io.kamon" %% "kamon-logback" % "1.0.2" excludeAll (
    ExclusionRule(organization = "log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "org.scala-lang.modules")
  )

  val jdbc = "io.kamon" %% "kamon-jdbc" % "1.0.2" excludeAll (
    ExclusionRule(organization = "log4j"),
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "org.scala-lang.modules")
  )

  val all = Seq(core, systemMetrics, akka, akkaHttp, jmx, datadog, logback)
}

object GatlingDeps {
  val version                 = "2.2.3"
  val highcharts: ModuleID    = "io.gatling.highcharts" % "gatling-charts-highcharts" % version
  val testFramework: ModuleID = "io.gatling" % "gatling-test-framework" % version
  val app: ModuleID           = "io.gatling" % "gatling-app" % version
}

object AWSSDK {
  val version        = "1.11.169"
  val core: ModuleID = "com.amazonaws" % "aws-java-sdk-core" % version
  val s3: ModuleID   = "com.amazonaws" % "aws-java-sdk-s3" % version
  val sqs: ModuleID  = "com.amazonaws" % "aws-java-sdk-sqs" % version
  val kms: ModuleID  = "com.amazonaws" % "aws-java-sdk-kms" % version
}

object Aws {
  val encryptionSdkJava = "com.amazonaws" % "aws-encryption-sdk-java" % "1.3.1"
}

object SQS {
  val elasticmqRestSqs: ModuleID = "org.elasticmq" %% "elasticmq-rest-sqs" % "0.13.8"
}

object Alpakka {
  val sqs = "com.lightbend.akka" %% "akka-stream-alpakka-sqs" % "0.11"
}

object Apache {
  val commonsLang: ModuleID = "org.apache.commons" % "commons-lang3" % "3.1"
}

object Everpeace {
  val healthCheck = "com.github.everpeace" %% "healthchecks-k8s-probes" % "0.3.0"
}

object PureConfig {
  val pureConfig = "com.github.pureconfig" %% "pureconfig" % "0.9.1"
}

object T3hnar {
  val bCrypt = "com.github.t3hnar" %% "scala-bcrypt" % "3.1"
}

object ScalaCheck {
  val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.14.0"
}

object Hayasshi {
  val router = "com.github.hayasshi" %% "akka-http-router" % "0.4.0"
}

object CORS {
  val `akka-http` = "ch.megard" %% "akka-http-cors" % "0.3.4"
}

object Heikoseeberger {
  val `akka-http-crice` = "de.heikoseeberger" %% "akka-http-circe" % "1.24.3"
}

object Swagger {
  val akkaHttp = "com.github.swagger-akka-http" %% "swagger-akka-http" % "2.0.1"
  val RSAPI    = "javax.ws.rs" % "javax.ws.rs-api" % "2.0.1"
  val all      = Seq(akkaHttp, RSAPI)
}
