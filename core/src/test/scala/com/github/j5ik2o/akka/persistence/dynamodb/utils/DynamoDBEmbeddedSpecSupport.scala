package com.github.j5ik2o.akka.persistence.dynamodb.utils

import java.io.File
import java.util.logging.{ Level, Logger }

import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.local.server.{
  DynamoDBProxyServer,
  LocalDynamoDBRequestHandler,
  LocalDynamoDBServerHandler
}
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBClientBuilder }
import com.github.j5ik2o.reactive.aws.test.RandomPortSupport
import org.scalatest.{ BeforeAndAfterAll, Suite }
import org.seasar.util.io.ResourceUtil
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException

import scala.concurrent.duration._

trait DynamoDBEmbeddedSpecSupport extends BeforeAndAfterAll with RandomPortSupport { this: Suite =>

  protected val waitIntervalForDynamoDBLocal: FiniteDuration = 500 milliseconds

  protected def sqlite4javaLibraryPath: File = new File(ResourceUtil.getBuildDir(getClass), "/../../../native-libs")

  protected val region: Regions = Regions.AP_NORTHEAST_1

  protected lazy val accessKeyId: String = "x"

  protected lazy val secretAccessKey: String = "x"

  protected lazy val dynamoDBPort: Int = temporaryServerPort()

  protected lazy val dynamoDBEndpoint: String = s"http://127.0.0.1:$dynamoDBPort"

  protected lazy val dynamoDBProxyServer: DynamoDBProxyServer = {
    System.setProperty("sqlite4java.library.path", sqlite4javaLibraryPath.toString)
    val inMemory = true
    // scalastyle:off
    val dbPath     = null
    val sharedDb   = true
    val corsParams = null
    // scalastyle:on
    new DynamoDBProxyServer(
      dynamoDBPort,
      new LocalDynamoDBServerHandler(
        new LocalDynamoDBRequestHandler(0, inMemory, dbPath, sharedDb, false),
        corsParams
      )
    )
  }

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

  val MaxCount = 10

  protected def waitDynamoDBLocal(): Unit = {
    var isWaken: Boolean = false
    var counter          = 0
    while (counter < MaxCount && !isWaken) {
      try {
        dynamoDBClient.listTables()
        isWaken = true
      } catch {
        case _: ResourceNotFoundException =>
          counter += 1
          Thread.sleep(waitIntervalForDynamoDBLocal.toMillis)
      }
    }
  }

  protected def startDynamoDBLocal(): Unit = {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.OFF)
    dynamoDBProxyServer.start()
  }

  protected def shutdownDynamoDBLocal(): Unit = {
    dynamoDBProxyServer.stop()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    startDynamoDBLocal()
    waitDynamoDBLocal()
  }

  override protected def afterAll(): Unit = {
    shutdownDynamoDBLocal()
    super.afterAll()
  }

}
