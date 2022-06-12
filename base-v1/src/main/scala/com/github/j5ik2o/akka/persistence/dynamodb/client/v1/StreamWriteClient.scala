package com.github.j5ik2o.akka.persistence.dynamodb.client.v1

import akka.NotUsed
import akka.actor.ActorSystem
import akka.japi.function
import akka.stream.javadsl.{ Flow => JavaFlow }
import akka.stream.scaladsl.{ Concat, Flow, Source }
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.client.StreamSupport
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ BackoffConfig, PluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.CompletableFutureUtils._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils._

import java.io.IOException
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

final class StreamWriteClient(
    val system: ActorSystem,
    val asyncClient: Option[AmazonDynamoDBAsync],
    val syncClient: Option[AmazonDynamoDB],
    val pluginConfig: PluginConfig,
    val writeBackoffConfig: BackoffConfig
) extends StreamSupport {

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
    flowWithBackoffSettings(writeBackoffConfig, flow)
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
    flowWithBackoffSettings(writeBackoffConfig, flow)
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
    flowWithBackoffSettings(writeBackoffConfig, flow)
  }

  def recursiveBatchWriteItemFlow: Flow[BatchWriteItemRequest, BatchWriteItemResult, NotUsed] = {
    def loop(
        acc: Source[BatchWriteItemResult, NotUsed]
    ): Flow[BatchWriteItemRequest, BatchWriteItemResult, NotUsed] =
      Flow[BatchWriteItemRequest].flatMapConcat { request =>
        Source.single(request).via(batchWriteItemFlow).flatMapConcat { response =>
          if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
            val unprocessedItems = Option(response.getUnprocessedItems)
              .map(_.asScala.toMap).map(_.map { case (k, v) => (k, v.asScala.toVector) }).flatMap(
                _.get(pluginConfig.tableName)
              ).getOrElse(Vector.empty)
            if (unprocessedItems.nonEmpty) {
              val nextRequest =
                request.withRequestItems(
                  Map(pluginConfig.tableName -> unprocessedItems.asJava).asJava
                )
              Source.single(nextRequest).via(loop(Source.combine(acc, Source.single(response))(Concat(_))))
            } else
              Source.combine(acc, Source.single(response))(Concat(_))
          } else {
            val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
            Source.failed(new IOException(s"statusCode: $statusCode"))
          }
        }
      }

    loop(Source.empty)
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
    flowWithBackoffSettings(writeBackoffConfig, flow)
  }
}
