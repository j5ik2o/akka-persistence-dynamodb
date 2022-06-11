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

class StreamReadClient(
    val system: ActorSystem,
    val asyncClient: Option[AmazonDynamoDBAsync],
    val syncClient: Option[AmazonDynamoDB],
    val pluginConfig: PluginConfig,
    val readBackoffConfig: BackoffConfig
) extends StreamSupport {

  private val log = system.log

  def getFlow: Flow[GetItemRequest, GetItemResult, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          implicit val executor = DispatcherUtils.newV1Executor(pluginConfig, system)
          JavaFlow
            .create[GetItemRequest]().mapAsync(
              1,
              new function.Function[GetItemRequest, CompletableFuture[GetItemResult]] {
                override def apply(request: GetItemRequest): CompletableFuture[GetItemResult] =
                  c.getItemAsync(request).toCompletableFuture
              }
            ).asScala
        case (None, Some(c)) =>
          Flow[GetItemRequest].map { request => c.getItem(request) }.withV1Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("getFlow")
    flowWithBackoffSettings(readBackoffConfig, flow)
  }

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
    flowWithBackoffSettings(readBackoffConfig, flow)
  }

  def recursiveQuerySource(queryRequest: QueryRequest, maxOpt: Option[Long]): Source[QueryResult, NotUsed] = {
    def loop(
        queryRequest: QueryRequest,
        maxOpt: Option[Long],
        lastEvaluatedKey: Option[Map[String, AttributeValue]] = None,
        acc: Source[QueryResult, NotUsed] = Source.empty,
        count: Long = 0,
        index: Int = 1
    ): Source[QueryResult, NotUsed] = {
      val newQueryRequest = lastEvaluatedKey match {
        case None => queryRequest
        case Some(_) =>
          queryRequest.withExclusiveStartKey(lastEvaluatedKey.map(_.asJava).orNull)
      }
      Source
        .single(newQueryRequest).via(queryFlow).flatMapConcat { response =>
          if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
            val lastEvaluatedKey = Option(response.getLastEvaluatedKey).map(_.asScala.toMap)
            val combinedSource   = Source.combine(acc, Source.single(response))(Concat(_))
            if (lastEvaluatedKey.nonEmpty && maxOpt.fold(true) { max => (count + response.getCount) < max }) {
              log.debug("next loop: count = {}, response.count = {}", count, response.getCount)
              loop(
                queryRequest,
                maxOpt,
                lastEvaluatedKey,
                combinedSource,
                count + response.getCount,
                index + 1
              )
            } else
              combinedSource
          } else {
            val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
            Source.failed(new IOException(s"statusCode: $statusCode"))
          }
        }
    }
    loop(queryRequest, maxOpt)
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
    flowWithBackoffSettings(readBackoffConfig, flow)
  }

  def recursiveScanSource(scanRequest: ScanRequest, maxOpt: Option[Long]): Source[ScanResult, NotUsed] = {
    def loop(
        scanRequest: ScanRequest,
        maxOpt: Option[Long],
        lastEvaluatedKey: Option[Map[String, AttributeValue]] = None,
        acc: Source[ScanResult, NotUsed] = Source.empty,
        count: Long = 0,
        index: Int = 1
    ): Source[ScanResult, NotUsed] = {
      val newScanRequest = lastEvaluatedKey match {
        case None => scanRequest
        case Some(_) =>
          scanRequest.withExclusiveStartKey(lastEvaluatedKey.map(_.asJava).orNull)
      }
      Source
        .single(newScanRequest).via(scanFlow).flatMapConcat { response =>
          if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
            val lastEvaluatedKey = Option(response.getLastEvaluatedKey).map(_.asScala.toMap)
            val combinedSource   = Source.combine(acc, Source.single(response))(Concat(_))
            if (lastEvaluatedKey.nonEmpty && maxOpt.fold(true) { max => (count + response.getCount) < max }) {
              log.debug("next loop: count = {}, response.count = {}", count, response.getCount)
              loop(
                scanRequest,
                maxOpt,
                lastEvaluatedKey,
                combinedSource,
                count + response.getCount,
                index + 1
              )
            } else
              combinedSource
          } else {
            val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
            Source.failed(new IOException(s"statusCode: $statusCode"))
          }
        }
    }
    loop(scanRequest, maxOpt)
  }

}
