package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ ClientType, ClientVersion }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ConfigHelper, DynamoDBSpecSupport, RandomPortUtil }
import org.scalatest.concurrent.ScalaFutures
import org.testcontainers.DockerClientFactory

import scala.concurrent.duration._

object DynamoDBJournalV1SyncSpec {
  val dynamoDBHost: String       = DockerClientFactory.instance().dockerHostIpAddress()
  val dynamoDBPort: Int          = RandomPortUtil.temporaryServerPort()
  val legacyJournalMode: Boolean = false

}

class DynamoDBJournalV1SyncSpec
    extends JournalSpec(
      ConfigHelper
        .config(
          Some("journal-reference"),
          legacyConfigFormat = false,
          legacyJournalMode = DynamoDBJournalV1AsyncSpec.legacyJournalMode,
          dynamoDBHost = DynamoDBJournalV1SyncSpec.dynamoDBHost,
          dynamoDBPort = DynamoDBJournalV1AsyncSpec.dynamoDBPort,
          clientVersion = ClientVersion.V1.toString,
          clientType = ClientType.Sync.toString
        )
    )
    with ScalaFutures
    with DynamoDBSpecSupport {
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

  implicit val pc: PatienceConfig = PatienceConfig(30 seconds, 1 seconds)

  override protected lazy val dynamoDBPort: Int = DynamoDBJournalV1AsyncSpec.dynamoDBPort

  override val legacyJournalTable: Boolean = DynamoDBJournalV1AsyncSpec.legacyJournalMode

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTable()
  }

  override def afterAll(): Unit = {
    deleteTable()
    super.afterAll()
  }

}
