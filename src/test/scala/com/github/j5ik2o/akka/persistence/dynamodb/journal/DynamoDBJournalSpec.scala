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
  val dynamoDBPort = RandomPortUtil.temporaryServerPort()
}

class DynamoDBJournalSpec
    extends JournalSpec(
      ConfigFactory
        .parseString(
          s"""
           |akka.persistence.journal.plugin = "dynamo-db-journal"
           |akka.persistence.snapshot-store.plugin = "dynamo-db-snapshot"
           |
           |dynamo-db-journal {
           |  refresh-interval = 10ms
           |  
           |  dynamodb-client {
           |    access-key-id = "x"
           |    secret-access-key = "x"
           |    endpoint = "http://127.0.0.1:${DynamoDBJournalSpec.dynamoDBPort}/"
           |  }
           |}
           |
           |dynamo-db-snapshot {
           |  refresh-interval = 10ms
           |  
           |  dynamodb-client {
           |    access-key-id = "x"
           |    secret-access-key = "x"
           |    endpoint = "http://127.0.0.1:${DynamoDBJournalSpec.dynamoDBPort}/"
           |  }
           |}
           |
           |dynamo-db-read-journal {
           |  write-plugin = "dynamo-db-journal"
           |  refresh-interval = 10ms
           |
           |  dynamodb-client {
           |    access-key-id = "x"
           |    secret-access-key = "x"
           |    endpoint = "http://127.0.0.1:${DynamoDBJournalSpec.dynamoDBPort}/"
           |  }
           |
        |}
      """.stripMargin
        ).withFallback(ConfigFactory.load())
    )
    with ScalaFutures
    with DynamoDBSpecSupport {
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

  implicit val pc: PatienceConfig = PatienceConfig(20 seconds, 1 seconds)

  override protected lazy val dynamoDBPort: Int = DynamoDBJournalSpec.dynamoDBPort

  val underlying: JavaDynamoDbAsyncClient = JavaDynamoDbAsyncClient
    .builder()
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    )
    .endpointOverride(URI.create(dynamoDBEndpoint))
    .build()

  override def asyncClient: DynamoDbAsyncClient = DynamoDbAsyncClient(underlying)

  before { createTable }

  after { deleteTable }

}
