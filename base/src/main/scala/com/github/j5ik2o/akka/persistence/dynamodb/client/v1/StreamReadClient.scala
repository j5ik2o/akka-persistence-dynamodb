package com.github.j5ik2o.akka.persistence.dynamodb.client.v1

import java.util.concurrent.CompletableFuture

import akka.NotUsed
import akka.actor.ActorSystem
import akka.japi.function
import akka.stream.javadsl.{ Flow => JavaFlow }
import akka.stream.scaladsl.{ Flow, RestartFlow }
import com.amazonaws.services.dynamodbv2.model.{ QueryRequest, QueryResult, ScanRequest, ScanResult }
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ BackoffConfig, PluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.CompletableFutureUtils._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils._

class StreamReadClient(
    val system: ActorSystem,
    val asyncClient: Option[AmazonDynamoDBAsync],
    val syncClient: Option[AmazonDynamoDB],
    val pluginConfig: PluginConfig,
    val readBackoffConfig: BackoffConfig
) {

  def queryFlow: Flow[QueryRequest, QueryResult, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          implicit val executor = DispatcherUtils.newV1Executor(pluginConfig, system)
          JavaFlow
            .create[QueryRequest]().mapAsync(
              1,
              new function.Function[QueryRequest, CompletableFuture[QueryResult]] {
                override def apply(request: QueryRequest): CompletableFuture[QueryResult] =
                  c.queryAsync(request).toCompletableFuture
              }
            ).asScala
        case (None, Some(c)) =>
          Flow[QueryRequest].map { request => c.query(request) }.withV1Dispatcher(pluginConfig)
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

  def scanFlow: Flow[ScanRequest, ScanResult, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          implicit val executor = DispatcherUtils.newV1Executor(pluginConfig, system)
          JavaFlow
            .create[ScanRequest]().mapAsync(
              1,
              new function.Function[ScanRequest, CompletableFuture[ScanResult]] {
                override def apply(request: ScanRequest): CompletableFuture[ScanResult] =
                  c.scanAsync(request).toCompletableFuture
              }
            ).asScala
        case (None, Some(c)) =>
          Flow[ScanRequest].map { request => c.scan(request) }.withV1Dispatcher(pluginConfig)
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
