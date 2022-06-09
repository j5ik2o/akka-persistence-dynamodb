package com.github.j5ik2o.akka.persistence.dynamodb.state

import akka.persistence.state.DurableStateStoreRegistry
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ClientType, ClientVersion}
import com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl.{DynamoDBDurableStateStoreV1, StateSpecBase}
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ConfigHelper, DynamoDBSpecSupport, RandomPortUtil}
import org.scalatest.concurrent.ScalaFutures
import org.testcontainers.DockerClientFactory

import java.util.UUID
import scala.concurrent.duration.DurationInt

object DynamoDBStateV1AsyncSpec {
  val dynamoDBHost: String = DockerClientFactory.instance().dockerHostIpAddress()
  val dynamoDBPort: Int    = RandomPortUtil.temporaryServerPort()
}

class DynamoDBStateV1AsyncSpec
    extends StateSpecBase(
      ConfigHelper
        .config(
          Some("state-reference"),
          legacyConfigFormat = false,
          legacyJournalMode = false,
          dynamoDBHost = DynamoDBStateV1AsyncSpec.dynamoDBHost,
          dynamoDBPort = DynamoDBStateV1AsyncSpec.dynamoDBPort,
          clientVersion = ClientVersion.V1.toString,
          clientType = ClientType.Async.toString
        )
    )
    with ScalaFutures
    with DynamoDBSpecSupport {

  implicit val pc: PatienceConfig = PatienceConfig(30.seconds, 1.seconds)

  override protected lazy val dynamoDBPort: Int = DynamoDBStateV1AsyncSpec.dynamoDBPort

  "A durable state store plugin" - {
    "instantiate a JdbcDurableDataStore successfully" in {
      val store = DurableStateStoreRegistry
        .get(system)
        .durableStateStoreFor[DynamoDBDurableStateStoreV1[String]](DynamoDBDurableStateStoreProvider.Identifier)

      {
        val id = UUID.randomUUID()
        val data = "ABC"
        val tag = ""
        store.upsertObject(id.toString, 1, data, tag).futureValue()
        val result = store.getObject(id.toString).futureValue()
        result.value shouldBe Some(data)
      }

      store shouldBe a[DynamoDBDurableStateStoreV1[_]]
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
