package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBClientBuilder }
import com.github.j5ik2o.dockerController.dynamodbLocal.DynamoDBLocalController
import com.github.j5ik2o.dockerController.{ CmdConfigures, DockerController, DockerControllerHelper, WaitPredicates }
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

trait DynamoDBContainerHelper extends DockerControllerHelper {

  protected lazy val region: Regions = Regions.AP_NORTHEAST_1

  protected lazy val accessKeyId: String = "x"

  protected lazy val secretAccessKey: String = "x"

  protected lazy val dynamoDBHost: String = dockerHost

  protected lazy val dynamoDBPort: Int = RandomPortUtil.temporaryServerPort()

  protected lazy val dynamoDBEndpoint: String = s"http://$dynamoDBHost:$dynamoDBPort"

  val dynamoDBLocalController: DynamoDBLocalController = DynamoDBLocalController(dockerClient)(dynamoDBPort)
    .configureCmds {
      CmdConfigures(
        removeContainerCmdConfigure = {
          _.withForce(true)
        }
      )
    }.asInstanceOf[DynamoDBLocalController]

  override protected val dockerControllers: Vector[DockerController] = Vector(dynamoDBLocalController)

  override protected val waitPredicatesSettings: Map[DockerController, WaitPredicateSetting] =
    Map(
      dynamoDBLocalController -> WaitPredicateSetting(
        Duration.Inf,
        WaitPredicates.forLogMessageByRegex(DynamoDBLocalController.RegexOfWaitPredicate, Some(10.seconds))
      )
    )

  protected def startDockerContainers(): Unit = {
    for (dockerController <- dockerControllers) {
      createDockerContainer(dockerController, None)
    }
    for (dockerController <- dockerControllers) {
      startDockerContainer(dockerController, None)
    }
  }

  protected def stopDockerContainers(): Unit = {
    for (dockerController <- dockerControllers) {
      stopDockerContainer(dockerController, None)
    }
    for (dockerController <- dockerControllers) {
      removeDockerContainer(dockerController, None)
    }
  }

//  protected lazy val dynamoDbLocalContainer: FixedHostPortGenericContainer = FixedHostPortGenericContainer(
//    dynamoDBImageName,
//    exposedHostPort = dynamoDBPort,
//    exposedContainerPort = 8000,
//    command = Seq("-Xmx256m", "-jar", "DynamoDBLocal.jar", "-dbPath", ".", "-sharedDb"),
//    waitStrategy = Wait.forListeningPort()
//  )

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
}
