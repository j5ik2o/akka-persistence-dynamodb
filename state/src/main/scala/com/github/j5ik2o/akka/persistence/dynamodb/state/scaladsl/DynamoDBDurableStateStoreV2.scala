package com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.annotation.ApiMayChange
import akka.persistence.state.scaladsl.GetObjectResult
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.scaladsl.{ Sink, Source }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v2
import com.github.j5ik2o.akka.persistence.dynamodb.config.BackoffConfig
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ Context, PersistenceId }
import com.github.j5ik2o.akka.persistence.dynamodb.state.config.StatePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.state._
import com.github.j5ik2o.akka.persistence.dynamodb.trace.TraceReporter
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
    val system: ActorSystem,
    val pluginExecutor: ExecutionContext,
    val asyncClient: Option[DynamoDbAsyncClient],
    val syncClient: Option[DynamoDbClient],
    val partitionKeyResolver: PartitionKeyResolver,
    val tableNameResolver: TableNameResolver,
    val metricsReporter: Option[MetricsReporter],
    val traceReporter: Option[TraceReporter],
    val pluginConfig: StatePluginConfig
) extends ScalaDurableStateUpdateStore[A] {
  implicit val mat: ActorSystem     = system
  implicit val ec: ExecutionContext = pluginExecutor

  private val writeBackoffConfig: BackoffConfig = pluginConfig.writeBackoffConfig
  private val readBackoffConfig: BackoffConfig  = pluginConfig.readBackoffConfig

  private val streamWriteClient: v2.StreamWriteClient =
    new v2.StreamWriteClient(system, asyncClient, syncClient, pluginConfig, writeBackoffConfig)
  private val streamReadClient: v2.StreamReadClient =
    new v2.StreamReadClient(system, asyncClient, syncClient, pluginConfig, readBackoffConfig)

  private val serialization: Serialization = SerializationExtension(system)
  private val akkaSerialization            = new AkkaSerialization(serialization)

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
        ).build()
      Source
        .single(request).via(streamReadClient.getFlow).flatMapConcat { result =>
          if (result.sdkHttpResponse().isSuccessful) {
            val itemOpt = Option(result.item).map(_.asScala)
            itemOpt
              .map { item =>
                val pkey: String          = item(pluginConfig.columnsDefConfig.partitionKeyColumnName).s
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
                val payload =
                  akkaSerialization
                    .deserialize(akkaSerialized).toOption.asInstanceOf[Option[
                      A
                    ]]
                Source.single(
                  GetRawObjectResult
                    .Just(pkey, persistenceId, payload, revision, serializerId, serializerManifest, tag, ordering)
                )
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
        GetObjectResult(value, revision)
    }
  }

  override def upsertObject(persistenceId: String, revision: Long, value: A, tag: String): Future[Done] = {
    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeStateStoreUpsertObject(context))

    def future = {
      val tableName = tableNameResolver.resolve(pid)
      val pkey      = partitionKeyResolver.resolve(pid)
      val request = akkaSerialization.serialize(value).map { serialized =>
        PutItemRequest
          .builder().tableName(tableName.asString).item(
            (Map(
              pluginConfig.columnsDefConfig.partitionKeyColumnName -> AttributeValue.builder().s(pkey.asString).build(),
              pluginConfig.columnsDefConfig.persistenceIdColumnName -> AttributeValue
                .builder().s(persistenceId).build(),
              pluginConfig.columnsDefConfig.revisionColumnName -> AttributeValue.builder().n(revision.toString).build(),
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
        .future(Future.fromTry(request)).via(streamWriteClient.putItemFlow).flatMapConcat { response =>
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

}
