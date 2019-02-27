package com.github.j5ik2o.akka.persistence.dynamodb.journal
import java.net.URI

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.github.j5ik2o.reactive.dynamodb.model._
import com.github.j5ik2o.reactive.dynamodb.{ DynamoDBAsyncClientV2, DynamoDBEmbeddedSpecSupport }
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.duration._

class DynamoDBJournalSpec
    extends JournalSpec(ConfigFactory.load("default.conf"))
    with ScalaFutures
    with DynamoDBEmbeddedSpecSupport {
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

  implicit val pc: PatienceConfig = PatienceConfig(20 seconds, 1 seconds)

  override protected lazy val dynamoDBPort: Int = 8000

  val underlying: DynamoDbAsyncClient = DynamoDbAsyncClient
    .builder()
    .httpClient(NettyNioAsyncHttpClient.builder().maxConcurrency(1).build())
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  import scala.concurrent.ExecutionContext.Implicits.global

  val client: DynamoDBAsyncClientV2 = DynamoDBAsyncClientV2(underlying)
  override def beforeAll(): Unit = {
    super.beforeAll()
    val tableName = "Journal"
    val createRequest = CreateTableRequest()
      .withAttributeDefinitions(
        Some(
          Seq(
            AttributeDefinition()
              .withAttributeName(Some("persistence-id"))
              .withAttributeType(Some(AttributeType.S)),
            AttributeDefinition()
              .withAttributeName(Some("sequence-nr"))
              .withAttributeType(Some(AttributeType.N))
          )
        )
      )
      .withKeySchema(
        Some(
          Seq(
            KeySchemaElement()
              .withAttributeName(Some("persistence-id"))
              .withKeyType(Some(KeyType.HASH)),
            KeySchemaElement()
              .withAttributeName(Some("sequence-nr"))
              .withKeyType(Some(KeyType.RANGE))
          )
        )
      )
      .withProvisionedThroughput(
        Some(
          ProvisionedThroughput()
            .withReadCapacityUnits(Some(10L))
            .withWriteCapacityUnits(Some(10L))
        )
      )
      .withTableName(Some(tableName))
    val createResponse = client
      .createTable(createRequest).futureValue
    createResponse.isSuccessful shouldBe true
  }
}
