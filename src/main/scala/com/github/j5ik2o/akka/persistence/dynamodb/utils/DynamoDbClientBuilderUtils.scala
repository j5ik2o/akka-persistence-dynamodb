package com.github.j5ik2o.akka.persistence.dynamodb.utils

import java.net.URI

import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbAsyncClientBuilder }

object DynamoDbClientBuilderUtils {

  def setup(persistencePluginConfig: PluginConfig,
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
