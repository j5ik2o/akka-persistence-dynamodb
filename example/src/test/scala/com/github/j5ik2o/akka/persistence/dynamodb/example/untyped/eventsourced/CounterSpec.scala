package com.github.j5ik2o.akka.persistence.dynamodb.example.untyped.eventsourced

import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBClientBuilder }
import com.github.dockerjava.core.DockerClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.example.CborSerializable
import com.github.j5ik2o.akka.persistence.dynamodb.example.untyped.eventsourced.CounterProtocol.GetValueReply
import com.github.j5ik2o.dockerController.WaitPredicates.WaitPredicate
import com.github.j5ik2o.dockerController.dynamodbLocal.DynamoDBLocalController
import com.github.j5ik2o.dockerController.{
  DockerClientConfigUtil,
  DockerController,
  DockerControllerSpecSupport,
  RandomPortUtil,
  WaitPredicates
}
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{ ScalaFutures, Waiters }
import org.scalatest.diagrams.Diagrams
import org.scalatest.freespec.AnyFreeSpecLike

import java.util.UUID
import scala.concurrent.duration.{ Duration, DurationInt }
import scala.jdk.CollectionConverters._

object CounterSpec {
  val dockerClientConfig: DockerClientConfig = DockerClientConfigUtil.buildConfigAwareOfDockerMachine()
  val dockerHost: String                     = DockerClientConfigUtil.dockerHost(dockerClientConfig)

  val dynamoDBHost: String            = dockerHost
  val dynamoDBPort: Int               = RandomPortUtil.temporaryServerPort()
  val dynamoDBAccessKeyId: String     = "x"
  val dynamoDBSecretAccessKey: String = "x"

  val config: Config = ConfigFactory.parseString(
    s"""
        akka {
          loglevel = DEBUG
          actor.provider = local
          persistence {
            journal {
              plugin = "j5ik2o.dynamo-db-journal"
            }
            snapshot-store {
              plugin = "j5ik2o.dynamo-db-snapshot"
            }
          }
        }
        j5ik2o.dynamo-db-journal {
          partition-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver$$PersistenceIdBased"
          sort-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.journal.SortKeyResolver$$Default"
          dynamo-db-client {
            access-key-id = "$dynamoDBAccessKeyId"
            secret-access-key = "$dynamoDBSecretAccessKey"
            endpoint = "http://$dynamoDBHost:$dynamoDBPort"
            region = "ap-northeast-1"
          }
        }
        j5ik2o.dynamo-db-snapshot {
          dynamo-db-client {
            access-key-id = "$dynamoDBAccessKeyId"
            secret-access-key = "$dynamoDBSecretAccessKey"
            endpoint = "http://$dynamoDBHost:$dynamoDBPort"
            region = "ap-northeast-1"
          }
        }
        akka.actor.allow-java-serialization = off
        akka.actor.serialization-bindings {
          "${classOf[CborSerializable].getName}" = jackson-cbor
        }
        """
  )
}

class CounterSpec
    extends TestKit(ActorSystem("CounterSpec", CounterSpec.config))
    with AnyFreeSpecLike
    with BeforeAndAfterAll
    with DockerControllerSpecSupport
    with ImplicitSender
    with ScalaFutures
    with Diagrams
    with Waiters {
  val testTimeFactor: Int = sys.env.getOrElse("TEST_TIME_FACTOR", "1").toInt
  logger.debug(s"testTimeFactor = $testTimeFactor")

  protected lazy val dynamoDBAccessKeyId: String = "x"

  protected lazy val dynamoDBSecretAccessKey: String = "x"

  protected lazy val dynamoDBHost: String = CounterSpec.dynamoDBHost

  protected lazy val dynamoDBPort: Int = CounterSpec.dynamoDBPort

  protected lazy val dynamoDBEndpoint: String = s"http://$dynamoDBHost:$dynamoDBPort"
  protected lazy val dynamoDBRegion: Regions  = Regions.AP_NORTHEAST_1

  logger.info(s"dynamoDBAccessKeyId = $dynamoDBAccessKeyId")
  logger.info(s"dynamoDBSecretAccessKey = $dynamoDBSecretAccessKey")
  logger.info(s"dynamoDBEndpoint = $dynamoDBEndpoint")

  lazy val dynamoDbLocalController: DynamoDBLocalController =
    new DynamoDBLocalController(dockerClient, imageTag = None)(dynamoDBPort)

  private val waitPredicate: WaitPredicate = WaitPredicates.forLogMessageByRegex(
    DynamoDBLocalController.RegexOfWaitPredicate,
    Some((1 * testTimeFactor).seconds)
  )

  protected val waitPredicateSettingForDynamoDbLocal: WaitPredicateSetting =
    WaitPredicateSetting(Duration.Inf, waitPredicate)

  protected def journalTableName: String  = "Journal"
  protected def snapshotTableName: String = "Snapshot"

  protected lazy val dynamoDBClient: AmazonDynamoDB = {
    AmazonDynamoDBClientBuilder
      .standard()
      .withEndpointConfiguration(new EndpointConfiguration(dynamoDBEndpoint, dynamoDBRegion.getName))
      .withCredentials(
        new AWSStaticCredentialsProvider(new BasicAWSCredentials(dynamoDBAccessKeyId, dynamoDBSecretAccessKey))
      )
      .build()
  }

  override protected val dockerControllers: Vector[DockerController] = Vector(dynamoDbLocalController)

  override protected val waitPredicatesSettings: Map[DockerController, WaitPredicateSetting] = Map(
    dynamoDbLocalController -> waitPredicateSettingForDynamoDbLocal
  )

  override protected def afterStartContainers(): Unit = {
    createJournalTable()
    createSnapshotTable()
  }

  private def isExistsTable(tableName: String): Boolean =
    dynamoDBClient.listTables(10).getTableNames.asScala.exists(_.contains(tableName))

  private def createTableIfNotExists(
      tableName: String,
      buildRequest: CreateTableRequest => CreateTableRequest,
      readCapacityUnit: Long = 10,
      writeCapacityUnit: Long = 10
  ): Unit = {
    if (!isExistsTable(tableName)) {
      val startedAt = System.currentTimeMillis()
      print(s"Creating DynamoDB table $tableName...")
      val request = buildRequest(
        new CreateTableRequest()
          .withTableName(tableName)
          .withProvisionedThroughput(
            new ProvisionedThroughput()
              .withReadCapacityUnits(readCapacityUnit)
              .withWriteCapacityUnits(writeCapacityUnit)
          )
      )
      val createResponse = dynamoDBClient.createTable(request)
      require(createResponse.getSdkHttpMetadata.getHttpStatusCode == 200)
      println(s"Success. ${System.currentTimeMillis() - startedAt}ms.")
    } else {
      println(s"DynamoDB table $tableName already exists.")
    }
  }

  protected def createJournalTable(): Unit =
    createTableIfNotExists(
      journalTableName,
      req =>
        req
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
    )

  protected def createSnapshotTable(): Unit =
    createTableIfNotExists(
      snapshotTableName,
      req =>
        req
          .withAttributeDefinitions(
            Seq(
              new AttributeDefinition().withAttributeName("pkey").withAttributeType(ScalarAttributeType.S),
              new AttributeDefinition().withAttributeName("skey").withAttributeType(ScalarAttributeType.S),
              new AttributeDefinition().withAttributeName("persistence-id").withAttributeType(ScalarAttributeType.S),
              new AttributeDefinition().withAttributeName("sequence-nr").withAttributeType(ScalarAttributeType.N)
            ).asJava
          ).withKeySchema(
            Seq(
              new KeySchemaElement().withAttributeName("pkey").withKeyType(KeyType.HASH)
            ).asJava
          ).withGlobalSecondaryIndexes(
            Seq(
              new GlobalSecondaryIndex()
                .withIndexName("GetSnapshotRowsIndex").withKeySchema(
                  Seq(
                    new KeySchemaElement().withAttributeName("persistence-id").withKeyType(KeyType.HASH),
                    new KeySchemaElement().withAttributeName("sequence-nr").withKeyType(KeyType.RANGE)
                  ).asJava
                ).withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                .withProvisionedThroughput(
                  new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L)
                )
            ).asJava
          )
    )

  def killActors(actors: ActorRef*): Unit = {
    actors.foreach { actorRef => system.stop(actorRef) }
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "CounterSpec" - {
    "increment & getValue" in {
      val testProbe = TestProbe()
      val actorId   = UUID.randomUUID()

      {
        val counterRef = system.actorOf(Counter.props(actorId))
        counterRef ! CounterProtocol.Increment // seqNr = 1, 1,
        counterRef ! CounterProtocol.Increment // seqNr = 2, 2, saveSnapshot(2)
        counterRef ! CounterProtocol.Increment // seqNr = 3, 3,
        counterRef ! CounterProtocol.Increment // seqNr = 4, 4, saveSnapshot(4)

        counterRef ! CounterProtocol.GetValue(testProbe.ref)
        val reply = testProbe.expectMsgType[GetValueReply](3.seconds)
        assert(reply.n == 4)
        counterRef ! CounterProtocol.DeleteSnapshot(reply.seqNr)
        killActors(counterRef)
      }

      {
        val counterRef = system.actorOf(Counter.props(actorId))
        counterRef ! CounterProtocol.GetValue(testProbe.ref)
        val reply = testProbe.expectMsgType[GetValueReply](3.seconds)
        assert(reply.n == 4)
      }
    }
  }
}
