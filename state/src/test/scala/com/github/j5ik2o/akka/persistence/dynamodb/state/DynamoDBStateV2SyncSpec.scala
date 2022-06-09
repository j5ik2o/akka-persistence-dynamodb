package com.github.j5ik2o.akka.persistence.dynamodb.state

import akka.persistence.state.DurableStateStoreRegistry
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ ClientType, ClientVersion }
import com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl.{ DynamoDBDurableStateStoreV2, StateSpecBase }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ConfigHelper, DynamoDBSpecSupport, RandomPortUtil }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.testcontainers.DockerClientFactory

import java.util.UUID

object DynamoDBStateV2SyncSpec {
  val dynamoDBHost: String = DockerClientFactory.instance().dockerHostIpAddress()
  val dynamoDBPort: Int    = RandomPortUtil.temporaryServerPort()
}

class DynamoDBStateV2SyncSpec
    extends StateSpecBase(
      ConfigHelper
        .config(
          Some("state-reference"),
          legacyConfigFormat = false,
          legacyJournalMode = false,
          dynamoDBHost = DynamoDBStateV2SyncSpec.dynamoDBHost,
          dynamoDBPort = DynamoDBStateV2SyncSpec.dynamoDBPort,
          clientVersion = ClientVersion.V2.toString,
          clientType = ClientType.Sync.toString
        )
    )
    with ScalaFutures
    with DynamoDBSpecSupport {

  implicit val pc: PatienceConfig = PatienceConfig(30.seconds, 1.seconds)

  override protected lazy val dynamoDBPort: Int = DynamoDBStateV2SyncSpec.dynamoDBPort

  "A durable state store plugin" - {
    "instantiate a JdbcDurableDataStore successfully" in {
      val store = DurableStateStoreRegistry
        .get(system)
        .durableStateStoreFor[DynamoDBDurableStateStoreV2[String]](DynamoDBDurableStateStoreProvider.Identifier)

      {
        val id   = UUID.randomUUID()
        val data = "ABC"
        val tag  = ""
        store.upsertObject(id.toString, 1, data, tag).futureValue()
        val result = store.getObject(id.toString).futureValue()
        result.value shouldBe Some(data)
      }

      store shouldBe a[DynamoDBDurableStateStoreV2[_]]
      store.system.settings.config shouldBe system.settings.config
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTable()
  }

  override def afterAll(): Unit = {
    deleteTable()
    super.afterAll()
  }
}
