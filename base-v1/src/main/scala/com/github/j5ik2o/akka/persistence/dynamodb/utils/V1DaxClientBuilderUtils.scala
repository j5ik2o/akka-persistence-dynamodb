package com.github.j5ik2o.akka.persistence.dynamodb.utils

import com.amazon.dax.client.dynamodbv2.{ AmazonDaxAsyncClientBuilder, AmazonDaxClientBuilder }
import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig

private[utils] object V1DaxClientBuilderUtils {

  def setupSync(dynamoDBClientConfig: DynamoDBClientConfig): AmazonDaxClientBuilder = {
    val cc      = V1DaxClientConfigUtils.setup(dynamoDBClientConfig)
    val builder = AmazonDaxClientBuilder.standard().withClientConfiguration(cc)
    (dynamoDBClientConfig.accessKeyId, dynamoDBClientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        builder.setCredentials(
          new AWSStaticCredentialsProvider(new BasicAWSCredentials(a, s))
        )
      case _ =>
    }
    dynamoDBClientConfig.region.foreach(builder.setRegion)
    dynamoDBClientConfig.endpoint.foreach { v => builder.setEndpointConfiguration(v.split(","): _*) }
    builder
  }

  def setupAsync(dynamoDBClientConfig: DynamoDBClientConfig): AmazonDaxAsyncClientBuilder = {
    val cc      = V1DaxClientConfigUtils.setup(dynamoDBClientConfig)
    val builder = AmazonDaxAsyncClientBuilder.standard().withClientConfiguration(cc)
    (dynamoDBClientConfig.accessKeyId, dynamoDBClientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        builder.setCredentials(
          new AWSStaticCredentialsProvider(new BasicAWSCredentials(a, s))
        )
      case _ =>
    }
    dynamoDBClientConfig.region.foreach(builder.setRegion)
    dynamoDBClientConfig.endpoint.foreach { v => builder.setEndpointConfiguration(v.split(","): _*) }
    builder
  }

}
