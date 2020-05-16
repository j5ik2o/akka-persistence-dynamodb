package com.github.j5ik2o.akka.persistence.dynamodb.journal

import java.net.URI

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

import scala.concurrent.duration._

object DynamoDBJournalSpec {
  val dynamoDBPort: Int          = RandomPortUtil.temporaryServerPort()
  val legacyJournalMode: Boolean = true
}

class DynamoDBJournalSpec
    extends JournalSpec(
      ConfigFactory
        .parseString(
          s"""
           |blocking-dispatcher {
           |  type = Dispatcher
           |  executor = "thread-pool-executor"
           |  thread-pool-executor {
           |    fixed-pool-size = 32
           |  }
           |  throughput = 1
           |}
           |
           |j5ik2o.dynamo-db-journal {
           |  legacy-config-layout = true
           |  query-batch-size = 1
           |  dynamo-db-client {
           |    region = "ap-northeast-1"
           |    endpoint = "http://127.0.0.1:${DynamoDBJournalSpec.dynamoDBPort}/"
           |    client-version = "v2"
           |    v1.dispatcher-name = "blocking-dispatcher"
           |  }
           |  columns-def {
           |    sort-key-column-name = ${if (DynamoDBJournalSpec.legacyJournalMode) "sequence-nr" else "skey"}
           |  }
           |}
           |
           |j5ik2o.dynamo-db-snapshot {
           |  dynamo-db-client {
           |    region = "ap-northeast-1"
           |    endpoint = "http://127.0.0.1:${DynamoDBJournalSpec.dynamoDBPort}/"
           |  }
           |}
           |
           |j5ik2o.dynamo-db-read-journal {
           |  query-batch-size = 1
           |  dynamo-db-client {
           |    region = "ap-northeast-1"
           |    endpoint = "http://127.0.0.1:${DynamoDBJournalSpec.dynamoDBPort}/"
           |  }
           |  columns-def {
           |    sort-key-column-name = ${if (DynamoDBJournalSpec.legacyJournalMode) "sequence-nr" else "skey"}
           |  }
           |}
           """.stripMargin
        ).withFallback(ConfigFactory.load())
    )
    with ScalaFutures
    with DynamoDBSpecSupport {
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

  implicit val pc: PatienceConfig = PatienceConfig(30 seconds, 1 seconds)

  override protected lazy val dynamoDBPort: Int = DynamoDBJournalSpec.dynamoDBPort

  override val legacyJournalTable: Boolean = DynamoDBJournalSpec.legacyJournalMode

  val underlying: JavaDynamoDbAsyncClient = JavaDynamoDbAsyncClient
    .builder()
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  override def dynamoDbAsyncClient: DynamoDbAsyncClient = DynamoDbAsyncClient(underlying)

  before { createTable }

  after { deleteTable }

}
