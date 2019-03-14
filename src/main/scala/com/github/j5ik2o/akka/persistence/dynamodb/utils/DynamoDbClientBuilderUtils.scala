package com.github.j5ik2o.akka.persistence.dynamodb.utils

import java.net.URI

import com.github.j5ik2o.akka.persistence.dynamodb.config.{ DynamoDBClientConfig, PluginConfig }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbAsyncClientBuilder }

object DynamoDbClientBuilderUtils {

  def setup(clientConfig: DynamoDBClientConfig, httpClientBuilder: SdkAsyncHttpClient): DynamoDbAsyncClientBuilder = {
    var dynamoDbAsyncClientBuilder = DynamoDbAsyncClient.builder().httpClient(httpClientBuilder)
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
