package com.github.j5ik2o.akka.persistence.dynamodb.client.v2

import java.io.IOException
import java.util.concurrent.CompletableFuture
import akka.NotUsed
import akka.actor.ActorSystem
import akka.japi.function
import akka.stream.RestartSettings
import akka.stream.javadsl.{ Flow => JavaFlow }
import akka.stream.scaladsl.{ Concat, Flow, RestartFlow, Source }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ BackoffConfig, PluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils._
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbClient }

import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

class StreamWriteClient(
    val system: ActorSystem,
    val asyncClient: Option[DynamoDbAsyncClient],
    val syncClient: Option[DynamoDbClient],
    val pluginConfig: PluginConfig,
    val writeBackoffConfig: BackoffConfig
) {

  def deleteItemFlow: Flow[DeleteItemRequest, DeleteItemResponse, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          JavaFlow
            .create[DeleteItemRequest]().mapAsync(
              1,
              new function.Function[DeleteItemRequest, CompletableFuture[DeleteItemResponse]] {
                override def apply(request: DeleteItemRequest): CompletableFuture[DeleteItemResponse] =
                  c.deleteItem(request)
              }
            ).asScala
        case (None, Some(c)) =>
          Flow[DeleteItemRequest].map { request => c.deleteItem(request) }.withV2Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("deleteItemFlow")
    if (writeBackoffConfig.enabled)
      RestartFlow
        .withBackoff(
          RestartSettings(
            minBackoff = writeBackoffConfig.minBackoff,
            maxBackoff = writeBackoffConfig.maxBackoff,
            randomFactor = writeBackoffConfig.randomFactor
          ).withMaxRestarts(writeBackoffConfig.maxRestarts, writeBackoffConfig.minBackoff)
        ) { () => flow }
    else flow
  }

  def batchWriteItemFlow: Flow[BatchWriteItemRequest, BatchWriteItemResponse, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          JavaFlow
            .create[BatchWriteItemRequest]().mapAsync(
              1,
              new function.Function[BatchWriteItemRequest, CompletableFuture[BatchWriteItemResponse]] {
                override def apply(request: BatchWriteItemRequest): CompletableFuture[BatchWriteItemResponse] =
                  c.batchWriteItem(request)
              }
            ).asScala
        case (None, Some(c)) =>
          Flow[BatchWriteItemRequest].map { request => c.batchWriteItem(request) }.withV2Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("batchWriteItemFlow")
    if (writeBackoffConfig.enabled)
      RestartFlow
        .withBackoff(
          RestartSettings(
            minBackoff = writeBackoffConfig.minBackoff,
            maxBackoff = writeBackoffConfig.maxBackoff,
            randomFactor = writeBackoffConfig.randomFactor
          ).withMaxRestarts(writeBackoffConfig.maxRestarts, writeBackoffConfig.minBackoff)
        ) { () => flow }
    else flow
  }

  def recursiveBatchWriteItemFlow: Flow[BatchWriteItemRequest, BatchWriteItemResponse, NotUsed] = {
    def loop(
        acc: Source[BatchWriteItemResponse, NotUsed]
    ): Flow[BatchWriteItemRequest, BatchWriteItemResponse, NotUsed] =
      Flow[BatchWriteItemRequest].flatMapConcat { request =>
        Source.single(request).via(batchWriteItemFlow).flatMapConcat { response =>
          if (response.sdkHttpResponse().isSuccessful) {
            val unprocessedItems = Option(response.unprocessedItems)
              .map(_.asScala.toMap).map { _.map { case (k, v) => (k, v.asScala.toVector) } }.flatMap(
                _.get(pluginConfig.tableName)
              ).getOrElse(Vector.empty)
            if (unprocessedItems.nonEmpty) {
              val nextRequest =
                request.toBuilder
                  .requestItems(
                    Map(pluginConfig.tableName -> unprocessedItems.asJava).asJava
                  ).build()
              Source.single(nextRequest).via(loop(Source.combine(acc, Source.single(response))(Concat(_))))
            } else {
              Source.combine(acc, Source.single(response))(Concat(_))
            }
          } else {
            val statusCode = response.sdkHttpResponse().statusCode()
            val statusText = response.sdkHttpResponse().statusText()
            Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
          }
        }
      }
    loop(Source.empty)
  }

  def putItemFlow: Flow[PutItemRequest, PutItemResponse, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          JavaFlow
            .create[PutItemRequest]().mapAsync(
              1,
              new function.Function[PutItemRequest, CompletableFuture[PutItemResponse]] {
                override def apply(request: PutItemRequest): CompletableFuture[PutItemResponse] = c.putItem(request)
              }
            ).asScala
        case (None, Some(c)) =>
          Flow[PutItemRequest].map { request => c.putItem(request) }.withV2Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("putItemFlow")
    if (writeBackoffConfig.enabled)
      RestartFlow
        .withBackoff(
          RestartSettings(
            minBackoff = writeBackoffConfig.minBackoff,
            maxBackoff = writeBackoffConfig.maxBackoff,
            randomFactor = writeBackoffConfig.randomFactor
          ).withMaxRestarts(writeBackoffConfig.maxRestarts, writeBackoffConfig.minBackoff)
        ) { () => flow }
    else flow
  }

  def updateItemFlow: Flow[UpdateItemRequest, UpdateItemResponse, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          JavaFlow
            .create[UpdateItemRequest]().mapAsync(
              1,
              new function.Function[UpdateItemRequest, CompletableFuture[UpdateItemResponse]] {
                override def apply(request: UpdateItemRequest): CompletableFuture[UpdateItemResponse] =
                  c.updateItem(request)
              }
            ).asScala
        case (None, Some(c)) =>
          Flow[UpdateItemRequest].map { request => c.updateItem(request) }.withV2Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("updateItemFlow")
    if (writeBackoffConfig.enabled)
      RestartFlow
        .withBackoff(
          RestartSettings(
            minBackoff = writeBackoffConfig.minBackoff,
            maxBackoff = writeBackoffConfig.maxBackoff,
            randomFactor = writeBackoffConfig.randomFactor
          ).withMaxRestarts(writeBackoffConfig.maxRestarts, writeBackoffConfig.minBackoff)
        ) { () => flow }
    else flow
  }

}
