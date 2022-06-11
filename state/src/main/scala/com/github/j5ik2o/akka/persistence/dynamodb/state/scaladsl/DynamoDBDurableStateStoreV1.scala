package com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.persistence.state.scaladsl.GetObjectResult
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.scaladsl.{ Sink, Source }
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue, DeleteItemRequest, GetItemRequest, PutItemRequest }
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v1.{ StreamReadClient, StreamWriteClient }
import com.github.j5ik2o.akka.persistence.dynamodb.config.BackoffConfig
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ Context, PersistenceId }
import com.github.j5ik2o.akka.persistence.dynamodb.state.config.StatePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.state.{
  AkkaSerialized,
  GetRawObjectResult,
  PartitionKeyResolver,
  StateSerializer,
  TableNameResolver
}
import com.github.j5ik2o.akka.persistence.dynamodb.trace.TraceReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport

import java.io.IOException
import java.nio.ByteBuffer
import java.util.UUID
import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success }

final class DynamoDBDurableStateStoreV1[A](
    val system: ActorSystem,
    val pluginExecutor: ExecutionContext,
    val asyncClient: Option[AmazonDynamoDBAsync],
    val syncClient: Option[AmazonDynamoDB],
    val partitionKeyResolver: PartitionKeyResolver,
    val tableNameResolver: TableNameResolver,
    val metricsReporter: Option[MetricsReporter],
    val traceReporter: Option[TraceReporter],
    val pluginConfig: StatePluginConfig
) extends ScalaDurableStateUpdateStore[A]
    with LoggingSupport {
  implicit val mat: ActorSystem     = system
  implicit val ec: ExecutionContext = pluginExecutor

  private val writeBackoffConfig: BackoffConfig = pluginConfig.writeBackoffConfig
  private val readBackoffConfig: BackoffConfig  = pluginConfig.readBackoffConfig

  private val streamWriteClient: StreamWriteClient =
    new StreamWriteClient(system, asyncClient, syncClient, pluginConfig, writeBackoffConfig)
  private val streamReadClient: StreamReadClient =
    new StreamReadClient(system, asyncClient, syncClient, pluginConfig, readBackoffConfig)

  protected val serialization: Serialization = SerializationExtension(system)
  private val akkaSerialization              = new StateSerializer(serialization, metricsReporter, traceReporter)

  override def getRawObject(persistenceId: String): Future[GetRawObjectResult[A]] = {
    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeStateStoreGetObject(context))

    def future = {
      val tableName = tableNameResolver.resolve(pid)
      val pkey      = partitionKeyResolver.resolve(pid)
      val request = new GetItemRequest()
        .withTableName(tableName.asString)
        .withKey(
          Map(
            pluginConfig.columnsDefConfig.partitionKeyColumnName -> new AttributeValue().withS(pkey.asString)
          ).asJava
        )
      Source
        .single(request).via(streamReadClient.getFlow).flatMapConcat { result =>
          if (result.getSdkHttpMetadata.getHttpStatusCode == 200) {
            Option(result.getItem)
              .map(_.asScala)
              .map { item =>
                if (item.nonEmpty) {
                  logger.debug(s"item = $item")
                  val persistenceId: String = item(pluginConfig.columnsDefConfig.persistenceIdColumnName).getS
                  val payloadAsArrayByte: Array[Byte] =
                    item(pluginConfig.columnsDefConfig.payloadColumnName).getB.array()
                  val serializerId: Int = item(pluginConfig.columnsDefConfig.serializerIdColumnName).getN.toInt
                  val serializerManifest: Option[String] =
                    item.get(pluginConfig.columnsDefConfig.serializerManifestColumnName).map(_.getS)
                  val revision       = item(pluginConfig.columnsDefConfig.revisionColumnName).getN.toLong
                  val tag            = item.get(pluginConfig.columnsDefConfig.tagsColumnName).map(_.getS)
                  val ordering       = item(pluginConfig.columnsDefConfig.orderingColumnName).getN.toLong
                  val akkaSerialized = AkkaSerialized(serializerId, serializerManifest, payloadAsArrayByte)
                  val payloadFuture: Future[GetRawObjectResult[A]] = akkaSerialization
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
            val statusCode = result.getSdkHttpMetadata.getHttpStatusCode
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
        new PutItemRequest()
          .withTableName(tableName.asString)
          .withItem(
            (Map(
              pluginConfig.columnsDefConfig.partitionKeyColumnName  -> new AttributeValue().withS(pkey.asString),
              pluginConfig.columnsDefConfig.persistenceIdColumnName -> new AttributeValue().withS(persistenceId),
              pluginConfig.columnsDefConfig.revisionColumnName      -> new AttributeValue().withN(revision.toString),
              pluginConfig.columnsDefConfig.payloadColumnName -> new AttributeValue()
                .withB(ByteBuffer.wrap(serialized.payload)),
              pluginConfig.columnsDefConfig.serializerIdColumnName -> new AttributeValue()
                .withN(serialized.serializerId.toString),
              pluginConfig.columnsDefConfig.orderingColumnName -> new AttributeValue()
                .withN(System.currentTimeMillis().toString)
            )
            ++ (if (tag.isEmpty) Map.empty
                else
                  Map(
                    pluginConfig.columnsDefConfig.tagsColumnName -> new AttributeValue().withS(tag)
                  ))
            ++ serialized.serializerManifest
              .map(v =>
                Map(pluginConfig.columnsDefConfig.serializerManifestColumnName -> new AttributeValue().withS(v))
              ).getOrElse(Map.empty)).asJava
          )
      }
      Source
        .future(request).via(streamWriteClient.putItemFlow).flatMapConcat { response =>
          if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
            Source.single(Done)
          } else {
            val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
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
      val request = new DeleteItemRequest()
        .withTableName(tableName.asString).withKey(
          Map(
            pluginConfig.columnsDefConfig.partitionKeyColumnName -> new AttributeValue().withS(pkey.asString)
          ).asJava
        )
      Source
        .single(request).via(streamWriteClient.deleteItemFlow).flatMapConcat { response =>
          if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
            Source.single(Done)
          } else {
            val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
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
