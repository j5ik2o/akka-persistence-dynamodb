package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalPerfSpec
import com.github.dockerjava.core.DockerClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.github.j5ik2o.dockerController.DockerClientConfigUtil
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

object DynamoDBJournalPerfSpec {
  val dockerClientConfig: DockerClientConfig = DockerClientConfigUtil.buildConfigAwareOfDockerMachine()
  val dynamoDBHost: String                   = DockerClientConfigUtil.dockerHost(dockerClientConfig)
  val dynamoDBPort: Int                      = RandomPortUtil.temporaryServerPort()
}

class DynamoDBJournalPerfSpec
    extends JournalPerfSpec(
      ConfigFactory
        .parseString(
          s"""
        |j5ik2o.dynamo-db-journal {
        |  shard-count = 64
        |  queue-buffer-size = 1024
        |  queue-parallelism = 64
        |  write-parallelism = 64
        |  query-batch-size = 1024
        |  dynamo-db-client {
        |    endpoint = "http://${DynamoDBJournalPerfSpec.dynamoDBHost}:${DynamoDBJournalPerfSpec.dynamoDBPort}/"
        |  }
        |}
        |
        |j5ik2o.dynamo-db-snapshot.dynamo-db-client {
        |  endpoint = "http://${DynamoDBJournalPerfSpec.dynamoDBHost}:${DynamoDBJournalPerfSpec.dynamoDBPort}/"
        |}
        |
        """.stripMargin
        ).withFallback(ConfigFactory.load("journal-reference"))
    )
    with BeforeAndAfterAll
    with ScalaFutures
    with DynamoDBSpecSupport {
  private val testTimeFactor: Double = sys.env.getOrElse("TEST_TIME_FACTOR", "1").toDouble

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = false

  /** Override in order to customize timeouts used for expectMsg, in order to tune the awaits to your journal's perf */
  override def awaitDurationMillis: Long = (60 * testTimeFactor).seconds.toMillis

  /** Number of messages sent to the PersistentActor under test for each test iteration */
  override def eventsCount: Int = 100

  /** Number of measurement iterations each test will be run. */
  override def measurementIterations: Int = 5

  override protected lazy val dynamoDBHost: String = DynamoDBJournalPerfSpec.dynamoDBHost
  override protected lazy val dynamoDBPort: Int    = DynamoDBJournalPerfSpec.dynamoDBPort

  override protected def afterStartContainers(): Unit = {
    createTable()
  }

  override protected def beforeStopContainers(): Unit = {
    deleteTable()
  }
}
