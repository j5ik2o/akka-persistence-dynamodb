package com.github.j5ik2o.akka.persistence.dynamodb.client.v2

import java.io.IOException
import java.util.concurrent.CompletableFuture
import akka.NotUsed
import akka.actor.ActorSystem
import akka.japi.function
import akka.stream.javadsl.{ Flow => JavaFlow }
import akka.stream.scaladsl.{ Concat, Flow, RestartFlow, Source }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ BackoffConfig, PluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils._
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  GetItemRequest,
  GetItemResponse,
  QueryRequest,
  QueryResponse,
  ScanRequest,
  ScanResponse
}
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbClient }

import scala.jdk.CollectionConverters._
import scala.compat.java8.OptionConverters._

class StreamReadClient(
    val system: ActorSystem,
    val asyncClient: Option[DynamoDbAsyncClient],
    val syncClient: Option[DynamoDbClient],
    val pluginConfig: PluginConfig,
    val readBackoffConfig: BackoffConfig
) {

  private val log = system.log

  def getFlow: Flow[GetItemRequest, GetItemResponse, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          JavaFlow
            .create[GetItemRequest]().mapAsync(
              1,
              new function.Function[GetItemRequest, CompletableFuture[GetItemResponse]] {
                override def apply(request: GetItemRequest): CompletableFuture[GetItemResponse] = c.getItem(request)
              }
            ).asScala
        case (None, Some(c)) =>
          Flow[GetItemRequest].map { request => c.getItem(request) }.withV2Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("getFlow")
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

  def recursiveQuerySource(
      queryRequest: QueryRequest,
      maxOpt: Option[Long]
  ): Source[Map[String, AttributeValue], NotUsed] = {
    def loop(
        queryRequest: QueryRequest,
        maxOpt: Option[Long],
        lastEvaluatedKey: Option[Map[String, AttributeValue]] = None,
        acc: Source[Map[String, AttributeValue], NotUsed] = Source.empty,
        count: Long = 0,
        index: Int = 1
    ): Source[Map[String, AttributeValue], NotUsed] = {
      val newQueryRequest = lastEvaluatedKey match {
        case None =>
          queryRequest
        case Some(_) =>
          queryRequest.toBuilder.exclusiveStartKey(lastEvaluatedKey.map(_.asJava).orNull).build()
      }
      Source
        .single(newQueryRequest).via(queryFlow).flatMapConcat { response =>
          if (response.sdkHttpResponse().isSuccessful) {
            val items =
              Option(response.items).map(_.asScala.toVector).map(_.map(_.asScala.toMap)).getOrElse(Vector.empty)
            val lastEvaluatedKey = Option(response.lastEvaluatedKey).map { _.asScala.toMap }.getOrElse(Map.empty)
            val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
            if (lastEvaluatedKey.nonEmpty && maxOpt.fold(true) { max => (count + response.count()) < max }) {
              log.debug("next loop: count = {}, response.count = {}", count, response.count())
              loop(
                queryRequest,
                maxOpt,
                Some(lastEvaluatedKey),
                combinedSource,
                count + response.count(),
                index + 1
              )
            } else
              combinedSource
          } else {
            val statusCode = response.sdkHttpResponse().statusCode()
            val statusText = response.sdkHttpResponse().statusText()
            Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
          }
        }
    }
    loop(queryRequest, maxOpt)
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

  def recursiveScanSource(
      scanRequest: ScanRequest,
      maxOpt: Option[Long]
  ): Source[ScanResponse, NotUsed] = {
    def loop(
        scanRequest: ScanRequest,
        maxOpt: Option[Long],
        lastEvaluatedKey: Option[Map[String, AttributeValue]] = None,
        acc: Source[ScanResponse, NotUsed] = Source.empty,
        count: Long = 0,
        index: Int = 1
    ): Source[ScanResponse, NotUsed] = {
      val newQueryRequest = lastEvaluatedKey match {
        case None => scanRequest
        case Some(_) =>
          scanRequest.toBuilder.exclusiveStartKey(lastEvaluatedKey.map(_.asJava).orNull).build()
      }
      Source
        .single(newQueryRequest).via(scanFlow).flatMapConcat { response =>
          if (response.sdkHttpResponse().isSuccessful) {
            Option(response.items).map(_.asScala.toVector).map(_.map(_.asScala.toMap)).getOrElse(Vector.empty)
            val lastEvaluatedKey = Option(response.lastEvaluatedKey).map { _.asScala.toMap }.getOrElse(Map.empty)
            val combinedSource   = Source.combine(acc, Source.single(response))(Concat(_))
            if (lastEvaluatedKey.nonEmpty && maxOpt.fold(true) { max => (count + response.count()) < max }) {
              log.debug("next loop: count = {}, response.count = {}", count, response.count())
              loop(
                scanRequest,
                maxOpt,
                Some(lastEvaluatedKey),
                combinedSource,
                count + response.count(),
                index + 1
              )
            } else
              combinedSource
          } else {
            val statusCode = response.sdkHttpResponse().statusCode()
            val statusText = response.sdkHttpResponse().statusText()
            Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
          }
        }
    }
    loop(scanRequest, maxOpt)
  }

}
