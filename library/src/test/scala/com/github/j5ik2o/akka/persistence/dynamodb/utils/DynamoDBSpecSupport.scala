package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.github.j5ik2o.reactive.aws.dynamodb.implicits._
import com.github.j5ik2o.reactive.aws.dynamodb.{ DynamoDBEmbeddedSpecSupport, DynamoDbAsyncClient }
import com.sun.xml.internal.bind.v2.schemagen.xmlschema.AttributeType
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.{ BeforeAndAfter, Matchers, Suite }
import org.slf4j.bridge.SLF4JBridgeHandler
import org.slf4j.{ Logger, LoggerFactory }
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration._

trait DynamoDBSpecSupport
    extends Matchers
    with Eventually
    with BeforeAndAfter
    with ScalaFutures
    with DynamoDBEmbeddedSpecSupport {
  this: Suite =>
  private implicit val pc: PatienceConfig = PatienceConfig(30 seconds, 1 seconds)

  SLF4JBridgeHandler.removeHandlersForRootLogger()
  SLF4JBridgeHandler.install()

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def asyncClient: DynamoDbAsyncClient

  def deleteTable: Unit = {
    deleteJournalTable
    deleteSnapshotTable
  }

  private def deleteJournalTable: Unit = {
    logger.debug("deleteJournalTable: start")
    val tableName = "Journal"
    asyncClient.deleteTable(tableName)
    eventually {
      val result = asyncClient.listTables(ListTablesRequest.builder().limit(1).build()).futureValue
      result.tableNamesAsScala.fold(true)(v => !v.contains(tableName)) shouldBe true
    }
    logger.debug("deleteJournalTable: finish")
  }

  private def deleteSnapshotTable: Unit = {
    logger.debug("deleteSnapshotTable: start")
    val tableName = "Snapshot"
    asyncClient.deleteTable(tableName)
    eventually {
      val result = asyncClient.listTables(ListTablesRequest.builder().limit(1).build()).futureValue
      result.tableNamesAsScala.fold(true)(v => !v.contains(tableName)) shouldBe true
    }
    logger.debug("deleteSnapshotTable: finish")
  }

  def createTable: Unit = {
    createJournalTable
    createSnapshotTable
  }

  def createSnapshotTable: Unit = {
    logger.debug("createSnapshotTable: start")
    val tableName = "Snapshot"
    val createRequest = CreateTableRequest
      .builder()
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
      .tableName(tableName).build()
    val createResponse = asyncClient
      .createTable(createRequest).futureValue
    logger.debug("createSnapshotTable: finish")
    createResponse.sdkHttpResponse().isSuccessful shouldBe true
  }

  private def createJournalTable: Unit = {
    logger.debug("createJournalTable: start")
    val tableName = "Journal"
    val createRequest = CreateTableRequest
      .builder()
      .tableName(tableName)
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
      ).build()

    val createResponse = asyncClient
      .createTable(createRequest).futureValue
    eventually {
      val result = asyncClient.listTables(ListTablesRequest.builder().limit(1).build()).futureValue
      result.tableNames.fold(false)(_.contains(tableName)) shouldBe true
    }
    logger.debug("createJournalTable: finish")
    createResponse.sdkHttpResponse().isSuccessful shouldBe true

  }
}
