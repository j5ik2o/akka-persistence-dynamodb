package com.github.j5ik2o.akka.persistence.dynamodb.client.v2

import java.util.concurrent.CompletableFuture

import akka.NotUsed
import akka.actor.ActorSystem
import akka.japi.function
import akka.stream.javadsl.{ Flow => JavaFlow }
import akka.stream.scaladsl.{ Flow, RestartFlow }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ BackoffConfig, PluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils._
import software.amazon.awssdk.services.dynamodb.model.{ QueryRequest, QueryResponse, ScanRequest, ScanResponse }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbClient }

class StreamReadClient(
    val system: ActorSystem,
    val asyncClient: Option[DynamoDbAsyncClient],
    val syncClient: Option[DynamoDbClient],
    val pluginConfig: PluginConfig,
    val readBackoffConfig: BackoffConfig
) {

  def queryFlow: Flow[QueryRequest, QueryResponse, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          JavaFlow
            .create[QueryRequest]().mapAsync(
              1,
              new function.Function[QueryRequest, CompletableFuture[QueryResponse]] {
                override def apply(request: QueryRequest): CompletableFuture[QueryResponse] = c.query(request)
              }
            ).asScala
        case (None, Some(c)) =>
          Flow[QueryRequest].map { request => c.query(request) }.withV2Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("queryFlow")
    if (readBackoffConfig.enabled)
      RestartFlow
        .withBackoff(
          minBackoff = readBackoffConfig.minBackoff,
          maxBackoff = readBackoffConfig.maxBackoff,
          randomFactor = readBackoffConfig.randomFactor,
          maxRestarts = readBackoffConfig.maxRestarts
        ) { () => flow }
    else flow
  }

  def scanFlow: Flow[ScanRequest, ScanResponse, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          JavaFlow
            .create[ScanRequest]().mapAsync(
              1,
              new akka.japi.function.Function[ScanRequest, CompletableFuture[ScanResponse]] {
                override def apply(request: ScanRequest): CompletableFuture[ScanResponse] =
                  c.scan(request)
              }
            ).asScala
        case (None, Some(c)) =>
          Flow[ScanRequest].map { request => c.scan(request) }.withV2Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("scanFlow")
    if (readBackoffConfig.enabled)
      RestartFlow
        .withBackoff(
          minBackoff = readBackoffConfig.minBackoff,
          maxBackoff = readBackoffConfig.maxBackoff,
          randomFactor = readBackoffConfig.randomFactor,
          maxRestarts = readBackoffConfig.maxRestarts
        ) { () => flow }
    else flow
  }

}
