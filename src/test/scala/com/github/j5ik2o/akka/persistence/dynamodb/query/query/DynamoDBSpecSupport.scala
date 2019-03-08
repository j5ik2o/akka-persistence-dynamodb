package com.github.j5ik2o.akka.persistence.dynamodb.query.query

import com.github.j5ik2o.reactive.aws.dynamodb.model._
import com.github.j5ik2o.reactive.aws.dynamodb.{ DynamoDBAsyncClientV2, DynamoDBEmbeddedSpecSupport }
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.{ BeforeAndAfter, Matchers, Suite }
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

trait DynamoDBSpecSupport
    extends Matchers
    with Eventually
    with BeforeAndAfter
    with ScalaFutures
    with DynamoDBEmbeddedSpecSupport {
  this: Suite =>
  private implicit val pc: PatienceConfig = PatienceConfig(20 seconds, 1 seconds)

  val logger = LoggerFactory.getLogger(getClass)

  def asyncClient: DynamoDBAsyncClientV2

  val tableName = "Journal"

  def deleteTable: Unit = {
    asyncClient.deleteTable(tableName)
    eventually {
      val result = asyncClient.listTables(ListTablesRequest().withLimit(Some(1))).futureValue
      result.tableNames.fold(true)(v => !v.contains(tableName)) shouldBe true
    }
    logger.info("delete table")
  }

  def createTable: Unit = {
    val createRequest = CreateTableRequest()
      .withTableName(Some(tableName))
      .withAttributeDefinitions(
        Some(
          Seq(
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
    logger.info("create table")
    createResponse.isSuccessful shouldBe true

  }
}
