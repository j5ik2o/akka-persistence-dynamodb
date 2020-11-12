package com.github.j5ik2o.akka.persistence.dynamodb.client.v1

import java.util.concurrent.CompletableFuture

import akka.NotUsed
import akka.actor.ActorSystem
import akka.japi.function
import akka.stream.javadsl.{ Flow => JavaFlow }
import akka.stream.scaladsl.{ Flow, RestartFlow }
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ BackoffConfig, PluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.CompletableFutureUtils._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils._

class StreamWriteClient(
    val system: ActorSystem,
    val asyncClient: Option[AmazonDynamoDBAsync],
    val syncClient: Option[AmazonDynamoDB],
    val pluginConfig: PluginConfig,
    val writeBackoffConfig: BackoffConfig
) {

  def putItemFlow: Flow[PutItemRequest, PutItemResult, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          implicit val executor = DispatcherUtils.newV1Executor(pluginConfig, system)
          JavaFlow
            .create[PutItemRequest]().mapAsync(
              1,
              new function.Function[PutItemRequest, CompletableFuture[PutItemResult]] {
                override def apply(request: PutItemRequest): CompletableFuture[PutItemResult] =
                  c.putItemAsync(request).toCompletableFuture
              }
            ).asScala
        case (None, Some(c)) =>
          Flow[PutItemRequest].map { request => c.putItem(request) }.withV1Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("putItemFlow")
    if (writeBackoffConfig.enabled)
      RestartFlow
        .withBackoff(
          minBackoff = writeBackoffConfig.minBackoff,
          maxBackoff = writeBackoffConfig.maxBackoff,
          randomFactor = writeBackoffConfig.randomFactor,
          maxRestarts = writeBackoffConfig.maxRestarts
        ) { () => flow }
    else flow
  }

  def updateItemFlow: Flow[UpdateItemRequest, UpdateItemResult, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          implicit val executor = DispatcherUtils.newV1Executor(pluginConfig, system)
          JavaFlow
            .create[UpdateItemRequest]().mapAsync(
              1,
              new function.Function[UpdateItemRequest, CompletableFuture[UpdateItemResult]] {
                override def apply(request: UpdateItemRequest): CompletableFuture[UpdateItemResult] =
                  c.updateItemAsync(request).toCompletableFuture
              }
            ).asScala
        case (None, Some(c)) =>
          Flow[UpdateItemRequest].map { request => c.updateItem(request) }.withV1Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("updateItemFlow")
    if (writeBackoffConfig.enabled)
      RestartFlow
        .withBackoff(
          minBackoff = writeBackoffConfig.minBackoff,
          maxBackoff = writeBackoffConfig.maxBackoff,
          randomFactor = writeBackoffConfig.randomFactor,
          maxRestarts = writeBackoffConfig.maxRestarts
        ) { () => flow }
    else flow
  }

  def batchWriteItemFlow: Flow[BatchWriteItemRequest, BatchWriteItemResult, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          implicit val executor = DispatcherUtils.newV1Executor(pluginConfig, system)
          JavaFlow
            .create[BatchWriteItemRequest]().mapAsync(
              1,
              new function.Function[BatchWriteItemRequest, CompletableFuture[BatchWriteItemResult]] {
                override def apply(request: BatchWriteItemRequest): CompletableFuture[BatchWriteItemResult] =
                  c.batchWriteItemAsync(request).toCompletableFuture
              }
            ).asScala
        case (None, Some(c)) =>
          Flow[BatchWriteItemRequest].map { request => c.batchWriteItem(request) }.withV1Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("batchWriteItemFlow")
    if (writeBackoffConfig.enabled)
      RestartFlow
        .withBackoff(
          minBackoff = writeBackoffConfig.minBackoff,
          maxBackoff = writeBackoffConfig.maxBackoff,
          randomFactor = writeBackoffConfig.randomFactor,
          maxRestarts = writeBackoffConfig.maxRestarts
        ) { () => flow }
    else flow
  }

  def deleteItemFlow: Flow[DeleteItemRequest, DeleteItemResult, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          implicit val executor = DispatcherUtils.newV1Executor(pluginConfig, system)
          JavaFlow
            .create[DeleteItemRequest]().mapAsync(
              1,
              new function.Function[DeleteItemRequest, CompletableFuture[DeleteItemResult]] {
                override def apply(request: DeleteItemRequest): CompletableFuture[DeleteItemResult] =
                  c.deleteItemAsync(request).toCompletableFuture
              }
            ).asScala
        case (None, Some(c)) =>
          Flow[DeleteItemRequest].map { request => c.deleteItem(request) }.withV1Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("deleteItemFlow")
    if (writeBackoffConfig.enabled)
      RestartFlow
        .withBackoff(
          minBackoff = writeBackoffConfig.minBackoff,
          maxBackoff = writeBackoffConfig.maxBackoff,
          randomFactor = writeBackoffConfig.randomFactor,
          maxRestarts = writeBackoffConfig.maxRestarts
        ) { () => flow }
    else flow
  }
}
