package com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.annotation.ApiMayChange
import akka.persistence.state.scaladsl.{ DurableStateUpdateStore, GetObjectResult }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.scaladsl.{ Sink, Source }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v2
import com.github.j5ik2o.akka.persistence.dynamodb.config.BackoffConfig
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ Context, PersistenceId }
import com.github.j5ik2o.akka.persistence.dynamodb.state.config.StatePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.state.{ AkkaSerialization, PartitionKeyResolver, TableNameResolver }
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
) extends DurableStateUpdateStore[A] {
  implicit val mat: ActorSystem     = system
  implicit val ec: ExecutionContext = pluginExecutor

  private val writeBackoffConfig: BackoffConfig = pluginConfig.writeBackoffConfig
  private val readBackoffConfig: BackoffConfig  = pluginConfig.readBackoffConfig
  private val streamWriteClient: v2.StreamWriteClient =
    new v2.StreamWriteClient(system, asyncClient, syncClient, pluginConfig, writeBackoffConfig)
  private val streamReadClient: v2.StreamReadClient =
    new v2.StreamReadClient(system, asyncClient, syncClient, pluginConfig, readBackoffConfig)

  private val serialization: Serialization = SerializationExtension(system)

  override def getObject(persistenceId: String): Future[GetObjectResult[A]] = {
    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeStateStoreGetObject(context))

    val tableName = tableNameResolver.resolve(pid)
    val pkey      = partitionKeyResolver.resolve(pid)
    val request = GetItemRequest
      .builder().tableName(tableName.asString).key(
        Map("pkey" -> AttributeValue.builder().s(pkey.asString).build()).asJava
      ).build()

    def future = Source
      .single(request).via(streamReadClient.getFlow).flatMapConcat { result =>
        if (result.sdkHttpResponse().isSuccessful) {
          val itemOpt = Option(result.item).map(_.asScala)
          itemOpt
            .map { item =>
              val payloadAsArrayByte: Array[Byte] = item("payload").b.asByteArray()
              val serId: Int                      = item("serializerId").n.toInt
              val manifest: Option[String]        = item.get("serializerManifest").map(_.s)
              val payload = AkkaSerialization
                .fromDurableStateRow(serialization)(payloadAsArrayByte, serId, manifest).toOption.asInstanceOf[Option[
                  A
                ]]
              Source.single(GetObjectResult(payload, item("revision").n.toLong))
            }.getOrElse {
              Source.single(GetObjectResult(None.asInstanceOf[Option[A]], 0))
            }
        } else {
          val statusCode = result.sdkHttpResponse().statusCode()
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }
      .runWith(Sink.head)

    val traced = traceReporter.fold(future)(_.traceStateStoreGetObject(context)(future))

    traced.onComplete {
      case Success(_) =>
        metricsReporter.foreach(_.afterStateStoreGetObject(newContext))
      case Failure(ex) =>
        metricsReporter.foreach(_.errorStateStoreGetObject(newContext, ex))
    }

    traced
  }

  override def upsertObject(persistenceId: String, revision: Long, value: A, tag: String): Future[Done] = {
    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeStateStoreUpsertObject(context))

    val tableName = tableNameResolver.resolve(pid)
    val pkey      = partitionKeyResolver.resolve(pid)
    val request = AkkaSerialization.serialize(serialization, value).map { serialized =>
      PutItemRequest
        .builder().tableName(tableName.asString).item(
          (Map(
            "pkey"          -> AttributeValue.builder().s(pkey.asString).build(),
            "persistenceId" -> AttributeValue.builder().s(persistenceId).build(),
            "revision"      -> AttributeValue.builder().n(revision.toString).build(),
            "payload"       -> AttributeValue.builder().b(SdkBytes.fromByteArray(serialized.payload)).build(),
            "tag"           -> AttributeValue.builder().ss(tag.split(pluginConfig.tagSeparator).toList.asJava).build(),
            "serializerId"  -> AttributeValue.builder().n(serialized.serializerId.toString).build(),
            "timestamp"     -> AttributeValue.builder().n(System.currentTimeMillis().toString).build()
          ) ++ serialized.serializerManifest
            .map(v => Map("serializerManifest" -> AttributeValue.builder().s(v).build())).getOrElse(Map.empty)).asJava
        ).build()
    }
    def future = Source
      .future(Future.fromTry(request)).via(streamWriteClient.putItemFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful) {
          Source.single(Done)
        } else {
          val statusCode = response.sdkHttpResponse().statusCode()
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }.runWith(Sink.head)

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

    val tableName = tableNameResolver.resolve(pid)
    val pkey      = partitionKeyResolver.resolve(pid)
    val request = DeleteItemRequest
      .builder().tableName(tableName.asString).key(
        Map(
          "pkey" -> AttributeValue.builder().s(pkey.asString).build()
        ).asJava
      ).build()
    def future = Source
      .single(request).via(streamWriteClient.deleteItemFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful) {
          Source.single(Done)
        } else {
          val statusCode = response.sdkHttpResponse().statusCode()
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }.runWith(Sink.head)

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
