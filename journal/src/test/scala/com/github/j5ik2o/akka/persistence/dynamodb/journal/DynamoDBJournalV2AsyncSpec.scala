package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.github.dockerjava.core.DockerClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ ClientType, ClientVersion }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ConfigHelper, DynamoDBSpecSupport, RandomPortUtil }
import com.github.j5ik2o.dockerController.DockerClientConfigUtil
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

object DynamoDBJournalV2AsyncSpec {
  val dockerClientConfig: DockerClientConfig = DockerClientConfigUtil.buildConfigAwareOfDockerMachine()
  val dynamoDBHost: String                   = DockerClientConfigUtil.dockerHost(dockerClientConfig)
  val dynamoDBPort: Int                      = RandomPortUtil.temporaryServerPort()
  val legacyJournalMode: Boolean             = false
}

class DynamoDBJournalV2AsyncSpec
    extends JournalSpec(
      ConfigHelper.config(
        Some("journal-reference"),
        legacyConfigFormat = false,
        legacyJournalMode = DynamoDBJournalV2AsyncSpec.legacyJournalMode,
        dynamoDBHost = DynamoDBJournalV2AsyncSpec.dynamoDBHost,
        dynamoDBPort = DynamoDBJournalV2AsyncSpec.dynamoDBPort,
        clientVersion = ClientVersion.V2.toString,
        clientType = ClientType.Async.toString
      )
    )
    with ScalaFutures
    with DynamoDBSpecSupport {

  private val testTimeFactor: Double = sys.env.getOrElse("TEST_TIME_FACTOR", "1").toDouble

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled((30 * testTimeFactor).seconds), interval = scaled((1 * testTimeFactor).seconds))

  override protected lazy val dynamoDBHost: String = DynamoDBJournalV2AsyncSpec.dynamoDBHost
  override protected lazy val dynamoDBPort: Int    = DynamoDBJournalV2AsyncSpec.dynamoDBPort

  override val legacyJournalTable: Boolean = DynamoDBJournalV2AsyncSpec.legacyJournalMode

  override protected def afterStartContainers(): Unit = {
    createTable()
  }

  override protected def beforeStopContainers(): Unit = {
    deleteTable()
  }

}
