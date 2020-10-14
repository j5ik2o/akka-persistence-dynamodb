package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.github.j5ik2o.reactive.aws.dynamodb.implicits._
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.{ BeforeAndAfter, Matchers, Suite }
import org.slf4j.bridge.SLF4JBridgeHandler
import org.slf4j.{ Logger, LoggerFactory }
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

trait DynamoDBSpecSupport
    extends Matchers
    with Eventually
    with BeforeAndAfter
    with ScalaFutures
    with DynamoDBContainerSpecSupport {
  this: Suite =>
  private implicit val pc: PatienceConfig = PatienceConfig(30 seconds, 1 seconds)

  SLF4JBridgeHandler.removeHandlersForRootLogger()
  SLF4JBridgeHandler.install()

  val logger: Logger = LoggerFactory.getLogger(getClass)

  val journalTableName  = "Journal"
  val snapshotTableName = "Snapshot"

  def dynamoDbAsyncClient: DynamoDbAsyncClient

  def deleteTable(): Unit = synchronized {
    Thread.sleep(500)
    deleteJournalTable()
    deleteSnapshotTable()
    Thread.sleep(500)
  }

  private def deleteJournalTable(): Unit = {
    val listTablesResult = dynamoDbAsyncClient.listTables(2).futureValue
    if (listTablesResult.tableNamesAsScala.exists(_.contains(journalTableName)))
      dynamoDbAsyncClient.deleteTable(journalTableName).futureValue
    eventually {
      val result = dynamoDbAsyncClient.listTables(2).futureValue
      result.tableNamesAsScala.exists(_.contains(journalTableName)) shouldBe false
    }
  }

  private def deleteSnapshotTable(): Unit = {
    val listTablesResult = dynamoDbAsyncClient.listTables(2).futureValue
    if (listTablesResult.tableNamesAsScala.exists(_.contains(snapshotTableName)))
      dynamoDbAsyncClient.deleteTable(snapshotTableName).futureValue
    eventually {
      val result = dynamoDbAsyncClient.listTables(2).futureValue
      result.tableNamesAsScala.exists(_.contains(snapshotTableName)) shouldBe false
    }
  }

  val legacyJournalTable = false

  def createTable(): Unit = synchronized {
    Thread.sleep(500)
    if (legacyJournalTable)
      createLegacyJournalTable()
    else
      createJournalTable()
    createSnapshotTable()
    waitDynamoDBLocal(Seq(journalTableName, snapshotTableName))
  }

  def createSnapshotTable(): Unit = {
    val createRequest = CreateTableRequest
      .builder()
      .tableName(snapshotTableName)
      .attributeDefinitionsAsScala(
        Seq(
          AttributeDefinition
            .builder()
            .attributeName("persistence-id")
            .attributeType(ScalarAttributeType.S).build(),
          AttributeDefinition
            .builder()
            .attributeName("sequence-nr")
            .attributeType(ScalarAttributeType.N).build()
        )
      )
      .keySchemaAsScala(
        Seq(
          KeySchemaElement
            .builder()
            .attributeName("persistence-id")
            .keyType(KeyType.HASH).build(),
          KeySchemaElement
            .builder()
            .attributeName("sequence-nr")
            .keyType(KeyType.RANGE).build()
        )
      )
      .provisionedThroughput(
        ProvisionedThroughput
          .builder()
          .readCapacityUnits(10L)
          .writeCapacityUnits(10L).build()
      )
      .build()
    val listTablesResult = dynamoDbAsyncClient.listTables(2).futureValue
    // println("listTablesResult = " + listTablesResult)
    if (!listTablesResult.tableNames.asScala.contains(snapshotTableName)) {
      val createResponse = dynamoDbAsyncClient.createTable(createRequest).futureValue
      createResponse.sdkHttpResponse().isSuccessful shouldBe true
    }
  }

  private def createLegacyJournalTable(): Unit = {
    val createRequest = CreateTableRequest
      .builder()
      .tableName(journalTableName)
      .attributeDefinitionsAsScala(
        Seq(
          AttributeDefinition
            .builder()
            .attributeName("pkey")
            .attributeType(ScalarAttributeType.S).build(),
          AttributeDefinition
            .builder()
            .attributeName("persistence-id")
            .attributeType(ScalarAttributeType.S).build(),
          AttributeDefinition
            .builder()
            .attributeName("sequence-nr")
            .attributeType(ScalarAttributeType.N).build(),
          AttributeDefinition
            .builder()
            .attributeName("tags")
            .attributeType(ScalarAttributeType.S).build()
        )
      )
      .keySchemaAsScala(
        Seq(
          KeySchemaElement
            .builder()
            .attributeName("pkey")
            .keyType(KeyType.HASH).build(),
          KeySchemaElement
            .builder()
            .attributeName("sequence-nr")
            .keyType(KeyType.RANGE).build()
        )
      )
      .provisionedThroughput(
        ProvisionedThroughput
          .builder()
          .readCapacityUnits(10L)
          .writeCapacityUnits(10L).build()
      )
      .globalSecondaryIndexesAsScala(
        Seq(
          GlobalSecondaryIndex
            .builder()
            .indexName("TagsIndex")
            .keySchemaAsScala(
              Seq(
                KeySchemaElement.builder().keyType(KeyType.HASH).attributeName("tags").build()
              )
            ).projection(Projection.builder().projectionType(ProjectionType.ALL).build())
            .provisionedThroughput(
              ProvisionedThroughput
                .builder()
                .readCapacityUnits(10L)
                .writeCapacityUnits(10L).build()
            ).build(),
          GlobalSecondaryIndex
            .builder()
            .indexName("GetJournalRowsIndex").keySchemaAsScala(
              Seq(
                KeySchemaElement.builder().keyType(KeyType.HASH).attributeName("persistence-id").build(),
                KeySchemaElement.builder().keyType(KeyType.RANGE).attributeName("sequence-nr").build()
              )
            ).projection(Projection.builder().projectionType(ProjectionType.ALL).build())
            .provisionedThroughput(
              ProvisionedThroughput
                .builder()
                .readCapacityUnits(10L)
                .writeCapacityUnits(10L).build()
            ).build()
        )
      )
      .streamSpecification(
        StreamSpecification.builder().streamEnabled(true).streamViewType(StreamViewType.NEW_IMAGE).build()
      )
      .build()
    val listTablesResult = dynamoDbAsyncClient.listTables(2).futureValue
    //  println("listTablesResult = " + listTablesResult)
    if (!listTablesResult.tableNames.asScala.contains(journalTableName)) {
      val createResponse = dynamoDbAsyncClient
        .createTable(createRequest).futureValue
      createResponse.sdkHttpResponse().isSuccessful shouldBe true
    }
  }

  private def createJournalTable(): Unit = {
    val createRequest = CreateTableRequest
      .builder()
      .tableName(journalTableName)
      .attributeDefinitionsAsScala(
        Seq(
          AttributeDefinition
            .builder()
            .attributeName("pkey")
            .attributeType(ScalarAttributeType.S).build(),
          AttributeDefinition
            .builder()
            .attributeName("skey")
            .attributeType(ScalarAttributeType.S).build(),
          AttributeDefinition
            .builder()
            .attributeName("persistence-id")
            .attributeType(ScalarAttributeType.S).build(),
          AttributeDefinition
            .builder()
            .attributeName("sequence-nr")
            .attributeType(ScalarAttributeType.N).build(),
          AttributeDefinition
            .builder()
            .attributeName("tags")
            .attributeType(ScalarAttributeType.S).build()
        )
      )
      .keySchemaAsScala(
        Seq(
          KeySchemaElement
            .builder()
            .attributeName("pkey")
            .keyType(KeyType.HASH).build(),
          KeySchemaElement
            .builder()
            .attributeName("skey")
            .keyType(KeyType.RANGE).build()
        )
      )
      .provisionedThroughput(
        ProvisionedThroughput
          .builder()
          .readCapacityUnits(10L)
          .writeCapacityUnits(10L).build()
      )
      .globalSecondaryIndexesAsScala(
        Seq(
          GlobalSecondaryIndex
            .builder()
            .indexName("TagsIndex")
            .keySchemaAsScala(
              Seq(
                KeySchemaElement.builder().keyType(KeyType.HASH).attributeName("tags").build()
              )
            ).projection(Projection.builder().projectionType(ProjectionType.ALL).build())
            .provisionedThroughput(
              ProvisionedThroughput
                .builder()
                .readCapacityUnits(10L)
                .writeCapacityUnits(10L).build()
            ).build(),
          GlobalSecondaryIndex
            .builder()
            .indexName("GetJournalRowsIndex").keySchemaAsScala(
              Seq(
                KeySchemaElement.builder().keyType(KeyType.HASH).attributeName("persistence-id").build(),
                KeySchemaElement.builder().keyType(KeyType.RANGE).attributeName("sequence-nr").build()
              )
            ).projection(Projection.builder().projectionType(ProjectionType.ALL).build())
            .provisionedThroughput(
              ProvisionedThroughput
                .builder()
                .readCapacityUnits(10L)
                .writeCapacityUnits(10L).build()
            ).build()
        )
      )
      .streamSpecification(
        StreamSpecification.builder().streamEnabled(true).streamViewType(StreamViewType.NEW_IMAGE).build()
      )
      .build()
    val listTablesResult = dynamoDbAsyncClient.listTables(2).futureValue
    // println("listTablesResult = " + listTablesResult)
    if (!listTablesResult.tableNames.asScala.contains(journalTableName)) {
      val createResponse = dynamoDbAsyncClient.createTable(createRequest).futureValue
      createResponse.sdkHttpResponse().isSuccessful shouldBe true
    }
  }
}
