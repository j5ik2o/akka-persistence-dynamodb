/*
 * Copyright 2020 Junichi Kato
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBClientBuilder }
import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import org.slf4j.LoggerFactory
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.wait.strategy.Wait

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

trait DynamoDBContainerHelper {
  LoggerFactory.getLogger(getClass)

  protected lazy val region: Regions = Regions.AP_NORTHEAST_1

  protected lazy val accessKeyId: String = "x"

  protected lazy val secretAccessKey: String = "x"

  protected lazy val dynamoDBHost: String = DockerClientFactory.instance().dockerHostIpAddress()

  protected lazy val dynamoDBPort: Int = RandomPortUtil.temporaryServerPort()

  protected lazy val dynamoDBEndpoint: String = s"http://$dynamoDBHost:$dynamoDBPort"

  protected lazy val dynamoDBImageVersion: String = "1.13.4"

  protected lazy val dynamoDBImageName: String = s"amazon/dynamodb-local:$dynamoDBImageVersion"

  protected lazy val dynamoDbLocalContainer: FixedHostPortGenericContainer = FixedHostPortGenericContainer(
    dynamoDBImageName,
    exposedHostPort = dynamoDBPort,
    exposedContainerPort = 8000,
    command = Seq("-Xmx256m", "-jar", "DynamoDBLocal.jar", "-dbPath", ".", "-sharedDb"),
    waitStrategy = Wait.forListeningPort()
  )

  protected lazy val dynamoDBClient: AmazonDynamoDB = {
    AmazonDynamoDBClientBuilder
      .standard().withCredentials(
        new AWSStaticCredentialsProvider(
          new BasicAWSCredentials(accessKeyId, secretAccessKey)
        )
      )
      .withEndpointConfiguration(
        new EndpointConfiguration(dynamoDBEndpoint, region.getName)
      ).build()
  }

  val journalTableName  = "Journal"
  val snapshotTableName = "Snapshot"
  val stateTableName    = "State"

  protected val waitIntervalForDynamoDBLocal: FiniteDuration = 500.milliseconds

  protected val MaxCount = 10

  protected def waitDynamoDBLocal(tableNames: Seq[String]): Unit = {
    var isWaken: Boolean = false
    var counter          = 0
    while (counter < MaxCount && !isWaken) {
      try {
        val listTablesResult = dynamoDBClient.listTables(2)
        if (tableNames.forall(s => listTablesResult.getTableNames.asScala.contains(s))) {
          isWaken = true
        } else {
          Thread.sleep(1000)
        }
      } catch {
        case _: ResourceNotFoundException =>
          counter += 1
          Thread.sleep(waitIntervalForDynamoDBLocal.toMillis)
      }
    }
  }

  def deleteTable(): Unit = synchronized {
    Thread.sleep(500)
    deleteJournalTable()
    deleteSnapshotTable()
    deleteStateTable()
    Thread.sleep(500)
  }

  val legacyJournalTable = false

  def createTable(): Unit = synchronized {
    Thread.sleep(500)
    if (legacyJournalTable)
      createLegacyJournalTable()
    else
      createJournalTable()
    createSnapshotTable()
    createStateTable()
    waitDynamoDBLocal(Seq(journalTableName, snapshotTableName))
  }

  private def deleteJournalTable(): Unit = {
    val listTablesResult = dynamoDBClient.listTables(2)
    if (listTablesResult.getTableNames.asScala.exists(_.contains(journalTableName)))
      dynamoDBClient.deleteTable(journalTableName)
    val result = dynamoDBClient.listTables(2)
    require(!result.getTableNames.asScala.exists(_.contains(journalTableName)))
  }

  private def deleteSnapshotTable(): Unit = {
    val listTablesResult = dynamoDBClient.listTables(2)
    if (listTablesResult.getTableNames.asScala.exists(_.contains(snapshotTableName)))
      dynamoDBClient.deleteTable(snapshotTableName)
    val result = dynamoDBClient.listTables(2)
    require(!result.getTableNames.asScala.exists(_.contains(snapshotTableName)))
  }

  private def deleteStateTable(): Unit = {
    val listTablesResult = dynamoDBClient.listTables(2)
    if (listTablesResult.getTableNames.asScala.exists(_.contains(stateTableName)))
      dynamoDBClient.deleteTable(stateTableName)
    val result = dynamoDBClient.listTables(2)
    require(!result.getTableNames.asScala.exists(_.contains(stateTableName)))
  }

  def createSnapshotTable(): Unit = {
    val listTablesResult = dynamoDBClient.listTables(2)
    if (!listTablesResult.getTableNames.asScala.exists(_.contains(snapshotTableName))) {
      val createRequest = new CreateTableRequest()
        .withTableName(snapshotTableName).withAttributeDefinitions(
          Seq(
            new AttributeDefinition().withAttributeName("persistence-id").withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition().withAttributeName("sequence-nr").withAttributeType(ScalarAttributeType.N)
          ).asJava
        ).withKeySchema(
          Seq(
            new KeySchemaElement().withAttributeName("persistence-id").withKeyType(KeyType.HASH),
            new KeySchemaElement().withAttributeName("sequence-nr").withKeyType(KeyType.RANGE)
          ).asJava
        ).withProvisionedThroughput(
          new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L)
        )
      val createResponse = dynamoDBClient.createTable(createRequest)
      require(createResponse.getSdkHttpMetadata.getHttpStatusCode == 200)
    }
  }

  private def createLegacyJournalTable(): Unit = {
    val listTablesResult = dynamoDBClient.listTables(2)
    if (!listTablesResult.getTableNames.asScala.exists(_.contains(journalTableName))) {
      val createRequest = new CreateTableRequest()
        .withTableName(journalTableName)
        .withAttributeDefinitions(
          Seq(
            new AttributeDefinition().withAttributeName("pkey").withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition().withAttributeName("persistence-id").withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition().withAttributeName("sequence-nr").withAttributeType(ScalarAttributeType.N),
            new AttributeDefinition().withAttributeName("tags").withAttributeType(ScalarAttributeType.S)
          ).asJava
        ).withKeySchema(
          Seq(
            new KeySchemaElement().withAttributeName("pkey").withKeyType(KeyType.HASH),
            new KeySchemaElement().withAttributeName("sequence-nr").withKeyType(KeyType.RANGE)
          ).asJava
        ).withProvisionedThroughput(
          new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L)
        ).withGlobalSecondaryIndexes(
          Seq(
            new GlobalSecondaryIndex()
              .withIndexName("TagsIndex").withKeySchema(
                Seq(
                  new KeySchemaElement().withAttributeName("tags").withKeyType(KeyType.HASH)
                ).asJava
              ).withProjection(new Projection().withProjectionType(ProjectionType.ALL))
              .withProvisionedThroughput(
                new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L)
              ),
            new GlobalSecondaryIndex()
              .withIndexName("GetJournalRowsIndex").withKeySchema(
                Seq(
                  new KeySchemaElement().withAttributeName("persistence-id").withKeyType(KeyType.HASH),
                  new KeySchemaElement().withAttributeName("sequence-nr").withKeyType(KeyType.RANGE)
                ).asJava
              ).withProjection(new Projection().withProjectionType(ProjectionType.ALL))
              .withProvisionedThroughput(
                new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L)
              )
          ).asJava
        ).withStreamSpecification(
          new StreamSpecification().withStreamEnabled(true).withStreamViewType(StreamViewType.NEW_IMAGE)
        )
      val createResponse = dynamoDBClient.createTable(createRequest)
      require(createResponse.getSdkHttpMetadata.getHttpStatusCode == 200)
    }
  }

  protected def createJournalTable(): Unit = {
    val listTablesResult = dynamoDBClient.listTables(2)
    if (!listTablesResult.getTableNames.asScala.exists(_.contains(journalTableName))) {
      val createRequest = new CreateTableRequest()
        .withTableName(journalTableName)
        .withAttributeDefinitions(
          Seq(
            new AttributeDefinition().withAttributeName("pkey").withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition().withAttributeName("skey").withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition().withAttributeName("persistence-id").withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition().withAttributeName("sequence-nr").withAttributeType(ScalarAttributeType.N),
            new AttributeDefinition().withAttributeName("tags").withAttributeType(ScalarAttributeType.S)
          ).asJava
        ).withKeySchema(
          Seq(
            new KeySchemaElement().withAttributeName("pkey").withKeyType(KeyType.HASH),
            new KeySchemaElement().withAttributeName("skey").withKeyType(KeyType.RANGE)
          ).asJava
        ).withProvisionedThroughput(
          new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L)
        ).withGlobalSecondaryIndexes(
          Seq(
            new GlobalSecondaryIndex()
              .withIndexName("TagsIndex").withKeySchema(
                Seq(
                  new KeySchemaElement().withAttributeName("tags").withKeyType(KeyType.HASH)
                ).asJava
              ).withProjection(new Projection().withProjectionType(ProjectionType.ALL))
              .withProvisionedThroughput(
                new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L)
              ),
            new GlobalSecondaryIndex()
              .withIndexName("GetJournalRowsIndex").withKeySchema(
                Seq(
                  new KeySchemaElement().withAttributeName("persistence-id").withKeyType(KeyType.HASH),
                  new KeySchemaElement().withAttributeName("sequence-nr").withKeyType(KeyType.RANGE)
                ).asJava
              ).withProjection(new Projection().withProjectionType(ProjectionType.ALL))
              .withProvisionedThroughput(
                new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L)
              )
          ).asJava
        ).withStreamSpecification(
          new StreamSpecification().withStreamEnabled(true).withStreamViewType(StreamViewType.NEW_IMAGE)
        )
      val createResponse = dynamoDBClient.createTable(createRequest)
      require(createResponse.getSdkHttpMetadata.getHttpStatusCode == 200)
    }
  }

  def createStateTable(): Unit = {
    val listTablesResult = dynamoDBClient.listTables(2)
    if (!listTablesResult.getTableNames.asScala.exists(_.contains(stateTableName))) {
      val createRequest = new CreateTableRequest()
        .withTableName(stateTableName).withAttributeDefinitions(
          Seq(
            new AttributeDefinition().withAttributeName("pkey").withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition().withAttributeName("persistence-id").withAttributeType(ScalarAttributeType.S)
          ).asJava
        ).withKeySchema(
          Seq(
            new KeySchemaElement().withAttributeName("pkey").withKeyType(KeyType.HASH)
          ).asJava
        ).withGlobalSecondaryIndexes(
          Seq(
            new GlobalSecondaryIndex()
              .withIndexName("PersistenceIdIndex").withKeySchema(
                Seq(
                  new KeySchemaElement().withAttributeName("persistence-id").withKeyType(KeyType.HASH)
                ).asJava
              ).withProjection(new Projection().withProjectionType(ProjectionType.ALL))
              .withProvisionedThroughput(
                new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L)
              )
          ).asJava
        )
        .withProvisionedThroughput(
          new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L)
        )
      val createResponse = dynamoDBClient.createTable(createRequest)
      require(createResponse.getSdkHttpMetadata.getHttpStatusCode == 200)
    }
  }

}
