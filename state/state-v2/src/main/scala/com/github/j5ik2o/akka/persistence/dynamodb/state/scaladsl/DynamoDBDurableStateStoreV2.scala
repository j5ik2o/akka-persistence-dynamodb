/*
 * Copyright 2022 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl

import akka.Done
import akka.actor.{ ActorSystem, CoordinatedShutdown }
import akka.annotation.ApiMayChange
import akka.persistence.state.scaladsl.GetObjectResult
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.scaladsl.{ Sink, Source }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v2.{ StreamReadClient, StreamWriteClient }
import com.github.j5ik2o.akka.persistence.dynamodb.config.BackoffConfig
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ Context, PersistenceId }
import com.github.j5ik2o.akka.persistence.dynamodb.state.DynamoDBDurableStateStoreProvider.Identifier
import com.github.j5ik2o.akka.persistence.dynamodb.state._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  DeleteItemRequest,
  GetItemRequest,
  PutItemRequest
}
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbClient }

import java.io.IOException
import java.util.UUID
import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success }

@ApiMayChange
final class DynamoDBDurableStateStoreV2[A](
    val pluginContext: StatePluginContext,
    val asyncClient: Option[DynamoDbAsyncClient],
    val syncClient: Option[DynamoDbClient]
) extends ScalaDurableStateUpdateStore[A]
    with LoggingSupport {

  val system = pluginContext.system

  import pluginContext._

  implicit val mat: ActorSystem     = pluginContext.system
  implicit val ec: ExecutionContext = pluginExecutor

  private val id: UUID = UUID.randomUUID()

  import pluginContext._

  CoordinatedShutdown(system).addTask(
    CoordinatedShutdown.PhaseBeforeActorSystemTerminate,
    s"$Identifier-$id"
  ) { () =>
    Future {
      dispose()
      Done
    }
  }

  private val writeBackoffConfig: BackoffConfig = pluginConfig.writeBackoffConfig
  private val readBackoffConfig: BackoffConfig  = pluginConfig.readBackoffConfig

  private val streamWriteClient: StreamWriteClient =
    new StreamWriteClient(pluginContext, asyncClient, syncClient, writeBackoffConfig)
  private val streamReadClient: StreamReadClient =
    new StreamReadClient(pluginContext, asyncClient, syncClient, readBackoffConfig)

  private val serialization: Serialization = SerializationExtension(system)
  private val akkaSerialization            = new StateSerializer(serialization, metricsReporter, traceReporter)

  override def getRawObject(persistenceId: String): Future[GetRawObjectResult[A]] = {
    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeStateStoreGetObject(context))

    def future = {
      val tableName = tableNameResolver.resolve(pid)
      val pkey      = partitionKeyResolver.resolve(pid)
      val request = GetItemRequest
        .builder().tableName(tableName.asString).key(
          Map(
            pluginConfig.columnsDefConfig.partitionKeyColumnName -> AttributeValue.builder().s(pkey.asString).build()
          ).asJava
        ).consistentRead(pluginConfig.consistentRead).build()
      Source
        .single(request).via(streamReadClient.getFlow).flatMapConcat { result =>
          if (result.sdkHttpResponse().isSuccessful) {
            Option(result.item)
              .map(_.asScala)
              .map { item =>
                if (item.nonEmpty) {
                  logger.debug(s"item = $item")
                  val persistenceId: String = item(pluginConfig.columnsDefConfig.persistenceIdColumnName).s
                  val serializerId: Int     = item(pluginConfig.columnsDefConfig.serializerIdColumnName).n.toInt
                  val serializerManifest: Option[String] =
                    item.get(pluginConfig.columnsDefConfig.serializerManifestColumnName).map(_.s)
                  val payloadAsArrayByte: Array[Byte] =
                    item(pluginConfig.columnsDefConfig.payloadColumnName).b.asByteArray()
                  val revision: Long = item(pluginConfig.columnsDefConfig.revisionColumnName).n.toLong
                  val tag            = item.get(pluginConfig.columnsDefConfig.tagsColumnName).map(_.s)
                  val ordering       = item(pluginConfig.columnsDefConfig.orderingColumnName).n.toLong
                  val akkaSerialized = AkkaSerialized(serializerId, serializerManifest, payloadAsArrayByte)
                  val payloadFuture: Future[GetRawObjectResult[A]] =
                    akkaSerialization
                      .deserialize(persistenceId, akkaSerialized).map { payload =>
                        GetRawObjectResult
                          .Just(
                            pkey.asString,
                            persistenceId,
                            payload.asInstanceOf[A],
                            revision,
                            serializerId,
                            serializerManifest,
                            tag,
                            ordering
                          )
                      }
                  Source.future(
                    payloadFuture
                  )
                } else {
                  Source.single(GetRawObjectResult.Empty)
                }
              }.getOrElse {
                Source.single(GetRawObjectResult.Empty)
              }
          } else {
            val statusCode = result.sdkHttpResponse().statusCode()
            Source.failed(new IOException(s"statusCode: $statusCode"))
          }
        }
        .runWith(Sink.head)
    }

    val traced = traceReporter.fold(future)(_.traceStateStoreGetObject(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterStateStoreGetObject(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorStateStoreGetObject(newContext, ex))
    }

    traced
  }

  override def getObject(persistenceId: String): Future[GetObjectResult[A]] = {
    getRawObject(persistenceId).map {
      case GetRawObjectResult.Empty =>
        GetObjectResult(None, 0)
      case GetRawObjectResult.Just(_, _, value, revision, _, _, _, _) =>
        GetObjectResult(Some(value), revision)
    }
  }

  override def upsertObject(persistenceId: String, revision: Long, value: A, tag: String): Future[Done] = {
    require(revision > 0)
    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeStateStoreUpsertObject(context))

    def future = {
      val tableName = tableNameResolver.resolve(pid)
      val pkey      = partitionKeyResolver.resolve(pid)
      val request = akkaSerialization.serialize(persistenceId, value).map { serialized =>
        PutItemRequest
          .builder().tableName(tableName.asString).item(
            (Map(
              pluginConfig.columnsDefConfig.partitionKeyColumnName -> AttributeValue.builder().s(pkey.asString).build(),
              pluginConfig.columnsDefConfig.persistenceIdColumnName -> AttributeValue
                .builder().s(persistenceId).build(),
              pluginConfig.columnsDefConfig.revisionColumnName -> AttributeValue
                .builder().n((revision + 1).toString).build(),
              pluginConfig.columnsDefConfig.payloadColumnName -> AttributeValue
                .builder().b(SdkBytes.fromByteArray(serialized.payload)).build(),
              pluginConfig.columnsDefConfig.serializerIdColumnName -> AttributeValue
                .builder().n(serialized.serializerId.toString).build(),
              pluginConfig.columnsDefConfig.orderingColumnName -> AttributeValue
                .builder().n(System.currentTimeMillis().toString).build()
            ) ++ (if (tag.isEmpty) Map.empty
                  else
                    Map(
                      pluginConfig.columnsDefConfig.tagsColumnName -> AttributeValue.builder().s(tag).build()
                    ))
            ++ serialized.serializerManifest
              .map(v =>
                Map(pluginConfig.columnsDefConfig.serializerManifestColumnName -> AttributeValue.builder().s(v).build())
              ).getOrElse(Map.empty)).asJava
          ).build()
      }
      Source
        .future(request).via(streamWriteClient.putItemFlow).flatMapConcat { response =>
          if (response.sdkHttpResponse().isSuccessful) {
            Source.single(Done)
          } else {
            val statusCode = response.sdkHttpResponse().statusCode()
            Source.failed(new IOException(s"statusCode: $statusCode"))
          }
        }.runWith(Sink.head)
    }

    val traced = traceReporter.fold(future)(_.traceStateStoreUpsertObject(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterStateStoreUpsertObject(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorStateStoreUpsertObject(newContext, ex))
    }

    traced
  }

  override def deleteObject(persistenceId: String): Future[Done] = {
    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeStateStoreDeleteObject(context))

    def future = {
      val tableName = tableNameResolver.resolve(pid)
      val pkey      = partitionKeyResolver.resolve(pid)
      val request = DeleteItemRequest
        .builder().tableName(tableName.asString).key(
          Map(
            pluginConfig.columnsDefConfig.partitionKeyColumnName -> AttributeValue.builder().s(pkey.asString).build()
          ).asJava
        ).build()
      Source
        .single(request).via(streamWriteClient.deleteItemFlow).flatMapConcat { response =>
          if (response.sdkHttpResponse().isSuccessful) {
            Source.single(Done)
          } else {
            val statusCode = response.sdkHttpResponse().statusCode()
            Source.failed(new IOException(s"statusCode: $statusCode"))
          }
        }.runWith(Sink.head)
    }

    val traced = traceReporter.fold(future)(_.traceStateStoreDeleteObject(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterStateStoreDeleteObject(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorStateStoreDeleteObject(newContext, ex))
    }

    traced
  }

  private def dispose(): Unit = {
    (asyncClient, syncClient) match {
      case (Some(a), _) => a.close()
      case (_, Some(s)) => s.close()
      case _            =>
    }
  }
}
