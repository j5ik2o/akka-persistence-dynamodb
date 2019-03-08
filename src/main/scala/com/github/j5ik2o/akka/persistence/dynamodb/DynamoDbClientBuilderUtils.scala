package com.github.j5ik2o.akka.persistence.dynamodb
import java.net.URI

import com.github.j5ik2o.akka.persistence.dynamodb.config.PersistencePluginConfig
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.http.SdkHttpClient
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient,
  DynamoDbAsyncClientBuilder,
  DynamoDbClient,
  DynamoDbClientBuilder
}

object DynamoDbClientBuilderUtils {

  def syncBuilder(persistencePluginConfig: PersistencePluginConfig,
                  httpClientBuilder: SdkHttpClient): DynamoDbClientBuilder = {
    var dynamoDbClientBuilder = DynamoDbClient.builder().httpClient(httpClientBuilder)
    val clientConfig          = persistencePluginConfig.clientConfig
    (clientConfig.accessKeyId, clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        dynamoDbClientBuilder = dynamoDbClientBuilder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(a, s))
        )
      case _ =>
    }
    clientConfig.endpoint.foreach { ep =>
      dynamoDbClientBuilder = dynamoDbClientBuilder.endpointOverride(URI.create(ep))
    }
    dynamoDbClientBuilder
  }

  def asyncBuilder(persistencePluginConfig: PersistencePluginConfig,
                   httpClientBuilder: SdkAsyncHttpClient): DynamoDbAsyncClientBuilder = {
    var dynamoDbAsyncClientBuilder = DynamoDbAsyncClient.builder().httpClient(httpClientBuilder)
    val clientConfig               = persistencePluginConfig.clientConfig
    (clientConfig.accessKeyId, clientConfig.secretAccessKey) match {
      case (Some(a), Some(s)) =>
        dynamoDbAsyncClientBuilder = dynamoDbAsyncClientBuilder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(a, s))
        )
      case _ =>
    }
    clientConfig.endpoint.foreach { ep =>
      dynamoDbAsyncClientBuilder = dynamoDbAsyncClientBuilder.endpointOverride(URI.create(ep))
    }
    dynamoDbAsyncClientBuilder
  }

}
