/*
 * Copyright 2020 Junichi Kato
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.j5ik2o.akka.persistence.dynamodb.client.v2

import akka.NotUsed
import akka.japi.function
import akka.stream.javadsl.{ Flow => JavaFlow }
import akka.stream.scaladsl.{ Concat, Flow, Source }
import com.github.j5ik2o.akka.persistence.dynamodb.client.StreamSupport
import com.github.j5ik2o.akka.persistence.dynamodb.config.BackoffConfig
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils._
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbClient }

import java.io.IOException
import java.util.concurrent.CompletableFuture
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

final class StreamReadClient(
    val pluginContext: PluginContext,
    val asyncClient: Option[DynamoDbAsyncClient],
    val syncClient: Option[DynamoDbClient],
    val readBackoffConfig: BackoffConfig
) extends StreamSupport {

  import pluginContext._

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
    flowWithBackoffSettings(readBackoffConfig, flow)
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
    flowWithBackoffSettings(readBackoffConfig, flow)
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
    flowWithBackoffSettings(readBackoffConfig, flow)
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
