package com.github.j5ik2o.akka.persistence.dynamodb.journal

import java.net.URI

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalPerfSpec
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ DynamoDBSpecSupport, RandomPortUtil }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

object DynamoDBJournalPerfSpec {
  val dynamoDBPort = RandomPortUtil.temporaryServerPort()
}

class DynamoDBJournalPerfSpec
    extends JournalPerfSpec(
      ConfigFactory
        .parseString(
          s"""
        |j5ik2o.dynamo-db-journal {
        |  shard-count = 2
        |  queue-buffer-size = 1024
        |  queue-parallelism = 1
        |  write-parallelism = 1
        |  query-batch-size = 1024
        |  dynamo-db-client {
        |    endpoint = "http://127.0.0.1:${DynamoDBJournalPerfSpec.dynamoDBPort}/"
        |  }
        |}
        |
        |j5ik2o.dynamo-db-snapshot.dynamo-db-client {
        |  endpoint = "http://127.0.0.1:${DynamoDBJournalPerfSpec.dynamoDBPort}/"
        |}
        |
        """.stripMargin
        ).withFallback(ConfigFactory.load())
    )
    with ScalaFutures
    with DynamoDBSpecSupport {
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = false

  override def eventsCount: Int = 100

  override protected lazy val dynamoDBPort: Int = DynamoDBJournalPerfSpec.dynamoDBPort

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
