package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.github.j5ik2o.reactive.aws.dynamodb.model._
import com.github.j5ik2o.reactive.aws.dynamodb.{ DynamoDBAsyncClientV2, DynamoDBEmbeddedSpecSupport }
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.{ BeforeAndAfter, Matchers, Suite }
import org.slf4j.bridge.SLF4JBridgeHandler
import org.slf4j.{ Logger, LoggerFactory }

import scala.concurrent.duration._

trait DynamoDBSpecSupport
    extends Matchers
    with Eventually
    with BeforeAndAfter
    with ScalaFutures
    with DynamoDBEmbeddedSpecSupport {
  this: Suite =>
  private implicit val pc: PatienceConfig = PatienceConfig(20 seconds, 1 seconds)

  SLF4JBridgeHandler.removeHandlersForRootLogger()
  SLF4JBridgeHandler.install()

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def asyncClient: DynamoDBAsyncClientV2

  def deleteTable: Unit = {
    deleteJournalTable
    deleteSnapshotTable
  }

  private def deleteJournalTable: Unit = {
    logger.debug("deleteJournalTable: start")
    val tableName = "Journal"
    asyncClient.deleteTable(tableName)
    eventually {
      val result = asyncClient.listTables(ListTablesRequest().withLimit(Some(1))).futureValue
      result.tableNames.fold(true)(v => !v.contains(tableName)) shouldBe true
    }
    logger.debug("deleteJournalTable: finish")
  }

  private def deleteSnapshotTable: Unit = {
    logger.debug("deleteSnapshotTable: start")
    val tableName = "Snapshot"
    asyncClient.deleteTable(tableName)
    eventually {
      val result = asyncClient.listTables(ListTablesRequest().withLimit(Some(1))).futureValue
      result.tableNames.fold(true)(v => !v.contains(tableName)) shouldBe true
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
    val createResponse = asyncClient
      .createTable(createRequest).futureValue
    logger.debug("createSnapshotTable: finish")
    createResponse.isSuccessful shouldBe true
  }

  private def createJournalTable: Unit = {
    logger.debug("createJournalTable: start")
    val tableName = "Journal"
    val createRequest = CreateTableRequest()
      .withTableName(Some(tableName))
      .withAttributeDefinitions(
        Some(
          Seq(
            AttributeDefinition()
              .withAttributeName(Some("pkey"))
              .withAttributeType(Some(AttributeType.S)),
            AttributeDefinition()
              .withAttributeName(Some("persistence-id"))
              .withAttributeType(Some(AttributeType.S)),
            AttributeDefinition()
              .withAttributeName(Some("sequence-nr"))
              .withAttributeType(Some(AttributeType.N)),
            AttributeDefinition()
              .withAttributeName(Some("tags"))
              .withAttributeType(Some(AttributeType.S))
          )
        )
      )
      .withKeySchema(
        Some(
          Seq(
            KeySchemaElement()
              .withAttributeName(Some("pkey"))
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
      .withGlobalSecondaryIndexes(
        Some(
          Seq(
            GlobalSecondaryIndex()
              .withIndexName(Some("TagsIndex")).withKeySchema(
                Some(
                  Seq(
                    KeySchemaElement().withKeyType(Some(KeyType.HASH)).withAttributeName(Some("tags"))
                  )
                )
              ).withProjection(Some(Projection().withProjectionType(Some(ProjectionType.ALL))))
              .withProvisionedThroughput(
                Some(
                  ProvisionedThroughput()
                    .withReadCapacityUnits(Some(10L))
                    .withWriteCapacityUnits(Some(10L))
                )
              ),
            GlobalSecondaryIndex()
              .withIndexName(Some("GetJournalRows")).withKeySchema(
                Some(
                  Seq(
                    KeySchemaElement().withKeyType(Some(KeyType.HASH)).withAttributeName(Some("persistence-id")),
                    KeySchemaElement().withKeyType(Some(KeyType.RANGE)).withAttributeName(Some("sequence-nr"))
                  )
                )
              ).withProjection(Some(Projection().withProjectionType(Some(ProjectionType.ALL))))
              .withProvisionedThroughput(
                Some(
                  ProvisionedThroughput()
                    .withReadCapacityUnits(Some(10L))
                    .withWriteCapacityUnits(Some(10L))
                )
              )
          )
        )
      )

    val createResponse = asyncClient
      .createTable(createRequest).futureValue
    eventually {
      val result = asyncClient.listTables(ListTablesRequest().withLimit(Some(1))).futureValue
      result.tableNames.fold(false)(_.contains(tableName)) shouldBe true
    }
    logger.debug("createJournalTable: finish")
    createResponse.isSuccessful shouldBe true

  }
}
