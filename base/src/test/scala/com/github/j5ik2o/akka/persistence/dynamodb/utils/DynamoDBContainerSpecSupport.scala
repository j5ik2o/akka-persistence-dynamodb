package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBClientBuilder }
import com.dimafeng.testcontainers.{ FixedHostPortGenericContainer, ForAllTestContainer }
import org.scalatest.{ BeforeAndAfterAll, Suite }
import org.testcontainers.containers.wait.strategy.Wait
import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

trait DynamoDBContainerSpecSupport extends ForAllTestContainer with BeforeAndAfterAll {
  this: Suite =>

  protected lazy val region: Regions = Regions.AP_NORTHEAST_1

  protected lazy val accessKeyId: String = "x"

  protected lazy val secretAccessKey: String = "x"

  protected lazy val dynamoDBPort: Int = RandomPortUtil.temporaryServerPort()

  protected lazy val dynamoDBEndpoint: String = s"http://127.0.0.1:$dynamoDBPort"

  protected lazy val dynamoDBImageVersion: String = "1.13.4"

  protected lazy val dynamoDBImageName: String = s"amazon/dynamodb-local:$dynamoDBImageVersion"

  protected lazy val dynamoDbLocalContainer: FixedHostPortGenericContainer = FixedHostPortGenericContainer(
    dynamoDBImageName,
    exposedHostPort = dynamoDBPort,
    exposedContainerPort = 8000,
    command = Seq("-Xmx256m", "-jar", "DynamoDBLocal.jar", "-dbPath", ".", "-sharedDb"),
    waitStrategy = Wait.forListeningPort()
  )

  override val container: FixedHostPortGenericContainer = dynamoDbLocalContainer

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

  protected val waitIntervalForDynamoDBLocal: FiniteDuration = 500 milliseconds

  protected val MaxCount = 10

  protected def waitDynamoDBLocal(tableNames: Seq[String]): Unit = {
    var isWaken: Boolean = false
    var counter          = 0
    while (counter < MaxCount && !isWaken) {
      try {
        val listTablesResult = dynamoDBClient.listTables(2)
        if (tableNames.forall(s => listTablesResult.getTableNames.asScala.contains(s))) {
          isWaken = true
        }
      } catch {
        case _: ResourceNotFoundException =>
          counter += 1
          Thread.sleep(waitIntervalForDynamoDBLocal.toMillis)
      }
    }
  }
}
