package com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.persistence.state.scaladsl.{ DurableStateUpdateStore, GetObjectResult }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.scaladsl.{ Sink, Source }
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue, DeleteItemRequest, GetItemRequest, PutItemRequest }
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v1
import com.github.j5ik2o.akka.persistence.dynamodb.config.BackoffConfig
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ Context, PersistenceId }
import com.github.j5ik2o.akka.persistence.dynamodb.state.{ AkkaSerialization, PartitionKeyResolver, TableNameResolver }
import com.github.j5ik2o.akka.persistence.dynamodb.state.config.StatePluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.trace.TraceReporter

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
) extends DurableStateUpdateStore[A] {
  implicit val mat: ActorSystem     = system
  implicit val ec: ExecutionContext = pluginExecutor

  private val writeBackoffConfig: BackoffConfig = pluginConfig.writeBackoffConfig
  private val readBackoffConfig: BackoffConfig  = pluginConfig.readBackoffConfig
  private val streamWriteClient: v1.StreamWriteClient =
    new v1.StreamWriteClient(system, asyncClient, syncClient, pluginConfig, writeBackoffConfig)
  private val streamReadClient: v1.StreamReadClient =
    new v1.StreamReadClient(system, asyncClient, syncClient, pluginConfig, readBackoffConfig)

  protected val serialization: Serialization = SerializationExtension(system)

  override def getObject(persistenceId: String): Future[GetObjectResult[A]] = {
    val pid        = PersistenceId(persistenceId)
    val context    = Context.newContext(UUID.randomUUID(), pid)
    val newContext = metricsReporter.fold(context)(_.beforeStateStoreGetObject(context))

    val tableName = tableNameResolver.resolve(pid)
    val pkey      = partitionKeyResolver.resolve(pid)
    val request = new GetItemRequest()
      .withTableName(tableName.asString)
      .withKey(
        Map(
          "pkey" -> new AttributeValue().withS(pkey.asString)
        ).asJava
      )
    def future = Source
      .single(request).via(streamReadClient.getFlow).flatMapConcat { result =>
        if (result.getSdkHttpMetadata.getHttpStatusCode == 200) {
          val itemOpt = Option(result.getItem).map(_.asScala)
          itemOpt
            .map { item =>
              val payloadAsArrayByte: Array[Byte] = item("payload").getB.array()
              val serId: Int                      = item("serializerId").getN.toInt
              val manifest: Option[String]        = item.get("serializerManifest").map(_.getS)
              val payload = AkkaSerialization
                .fromDurableStateRow(serialization)(payloadAsArrayByte, serId, manifest).toOption.asInstanceOf[Option[
                  A
                ]]
              Source.single(GetObjectResult(payload, item("revision").getN.toLong))
            }.getOrElse {
              Source.single(GetObjectResult(None.asInstanceOf[Option[A]], 0))
            }
        } else {
          val statusCode = result.getSdkHttpMetadata.getHttpStatusCode
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
      new PutItemRequest()
        .withTableName(tableName.asString)
        .withItem(
          (Map(
            "pkey"          -> new AttributeValue().withS(pkey.asString),
            "persistenceId" -> new AttributeValue().withS(persistenceId),
            "revision"      -> new AttributeValue().withN(revision.toString),
            "payload"       -> new AttributeValue().withB(ByteBuffer.wrap(serialized.payload)),
            "tag"           -> new AttributeValue().withSS(tag.split(pluginConfig.tagSeparator).toList.asJava),
            "serializerId"  -> new AttributeValue().withN(serialized.serializerId.toString),
            "timestamp"     -> new AttributeValue().withN(System.currentTimeMillis().toString)
          ) ++ serialized.serializerManifest
            .map(v => Map("serializerManifest" -> new AttributeValue().withS(v))).getOrElse(Map.empty)).asJava
        )
    }

    def future = Source
      .future(Future.fromTry(request)).via(streamWriteClient.putItemFlow).flatMapConcat { response =>
        if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
          Source.single(Done)
        } else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
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
    val request = new DeleteItemRequest()
      .withTableName(tableName.asString).withKey(
        Map(
          "pkey" -> new AttributeValue().withS(pkey.asString)
        ).asJava
      )
    def future = Source
      .single(request).via(streamWriteClient.deleteItemFlow).flatMapConcat { response =>
        if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
          Source.single(Done)
        } else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
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
