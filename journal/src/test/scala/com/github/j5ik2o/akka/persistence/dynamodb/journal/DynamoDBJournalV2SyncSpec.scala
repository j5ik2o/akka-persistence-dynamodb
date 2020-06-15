package com.github.j5ik2o.akka.persistence.dynamodb.journal

import java.net.URI

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ ClientType, ClientVersion }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

import scala.concurrent.duration._

object DynamoDBJournalV2SyncSpec {
  val dynamoDBPort: Int          = RandomPortUtil.temporaryServerPort()
  val legacyJournalMode: Boolean = false
}

class DynamoDBJournalV2SyncSpec
    extends JournalSpec(
      ConfigHelper.config(
        "journal-reference",
        legacyConfigFormat = false,
        legacyJournalMode = DynamoDBJournalV2AsyncSpec.legacyJournalMode,
        dynamoDBPort = DynamoDBJournalV2AsyncSpec.dynamoDBPort,
        clientVersion = ClientVersion.V2,
        clientType = ClientType.Sync,
        None,
        None
      )
    )
    with ScalaFutures
    with DynamoDBSpecSupport {
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

  implicit val pc: PatienceConfig = PatienceConfig(30 seconds, 1 seconds)

  override protected lazy val dynamoDBPort: Int = DynamoDBJournalV2AsyncSpec.dynamoDBPort

  override val legacyJournalTable: Boolean = DynamoDBJournalV2AsyncSpec.legacyJournalMode

  val underlying: JavaDynamoDbAsyncClient = JavaDynamoDbAsyncClient
    .builder()
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  override def dynamoDbAsyncClient: DynamoDbAsyncClient = DynamoDbAsyncClient(underlying)

  before { createTable() }

  after { deleteTable() }

}
