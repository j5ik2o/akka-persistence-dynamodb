/*
 * Copyright 2017 Dennis Vriend
 * Copyright 2019 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao

import akka.NotUsed
import akka.actor.ActorSystem
import akka.persistence.SnapshotMetadata
import akka.serialization.Serialization
import akka.stream.scaladsl.Source
import com.github.j5ik2o.akka.persistence.dynamodb.client.v2.{ StreamReadClient, StreamWriteClient }
import com.github.j5ik2o.akka.persistence.dynamodb.config.SnapshotPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.ByteArraySnapshotSerializer
import com.github.j5ik2o.akka.persistence.dynamodb.trace.TraceReporter
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbClient => JavaDynamoDbSyncClient
}

import java.io.IOException
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._

final class V2SnapshotDaoImpl(
    system: ActorSystem,
    asyncClient: Option[JavaDynamoDbAsyncClient],
    syncClient: Option[JavaDynamoDbSyncClient],
    serialization: Serialization,
    pluginConfig: SnapshotPluginConfig,
    metricsReporter: Option[MetricsReporter],
    traceReporter: Option[TraceReporter]
) extends SnapshotDao {
  (asyncClient, syncClient) match {
    case (None, None) =>
      throw new IllegalArgumentException("aws clients is both None")
    case _ =>
  }

  import pluginConfig._

  private val streamReadClient =
    new StreamReadClient(system, asyncClient, syncClient, pluginConfig, pluginConfig.readBackoffConfig)

  private val streamWriteClient =
    new StreamWriteClient(system, asyncClient, syncClient, pluginConfig, pluginConfig.writeBackoffConfig)

  private val serializer = new ByteArraySnapshotSerializer(serialization, metricsReporter, traceReporter)

  override def delete(persistenceId: PersistenceId, sequenceNr: SequenceNumber): Source[Unit, NotUsed] = {
    val req = DeleteItemRequest
      .builder()
      .tableName(tableName).key(
        Map(
          columnsDefConfig.persistenceIdColumnName -> AttributeValue.builder().s(persistenceId.asString).build(),
          columnsDefConfig.sequenceNrColumnName    -> AttributeValue.builder().n(sequenceNr.asString).build()
        ).asJava
      ).build()
    Source.single(req).via(streamWriteClient.deleteItemFlow).flatMapConcat { response =>
      if (response.sdkHttpResponse().isSuccessful)
        Source.single(())
      else {
        val statusCode = response.sdkHttpResponse().statusCode()
        val statusText = response.sdkHttpResponse().statusText()
        Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
      }
    }
  }

  override def deleteAllSnapshots(
      persistenceId: PersistenceId
  )(implicit ec: ExecutionContext): Source[Unit, NotUsed] = {
    val queryRequest = QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpression("#pid = :pid and #snr between :min and :max")
      .expressionAttributeNames(
        Map("#pid" -> columnsDefConfig.persistenceIdColumnName, "#snr" -> columnsDefConfig.sequenceNrColumnName).asJava
      ).expressionAttributeValues(
        Map(
          ":pid" -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min" -> AttributeValue.builder().n(0.toString).build(),
          ":max" -> AttributeValue.builder().n(Long.MaxValue.toString).build()
        ).asJava
      ).consistentRead(consistentRead).build()
    queryDelete(queryRequest)
  }

  override def deleteUpToMaxSequenceNr(
      persistenceId: PersistenceId,
      maxSequenceNr: SequenceNumber
  )(implicit ec: ExecutionContext): Source[Unit, NotUsed] = {
    val queryRequest = QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpression("#pid = :pid and #snr between :min and :max")
      .expressionAttributeNames(
        Map("#pid" -> columnsDefConfig.persistenceIdColumnName, "#snr" -> columnsDefConfig.sequenceNrColumnName).asJava
      ).expressionAttributeValues(
        Map(
          ":pid" -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min" -> AttributeValue.builder().n(0.toString).build(),
          ":max" -> AttributeValue.builder().n(maxSequenceNr.asString).build()
        ).asJava
      ).consistentRead(consistentRead).build()
    queryDelete(queryRequest)
  }

  override def deleteUpToMaxTimestamp(persistenceId: PersistenceId, maxTimestamp: Long)(implicit
      ec: ExecutionContext
  ): Source[Unit, NotUsed] = {
    val queryRequest = QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpression("#pid = :pid and #snr between :min and :max")
      .filterExpression("#created <= :maxTimestamp")
      .expressionAttributeNames(
        Map(
          "#pid"     -> columnsDefConfig.persistenceIdColumnName,
          "#snr"     -> columnsDefConfig.sequenceNrColumnName,
          "#created" -> columnsDefConfig.createdColumnName
        ).asJava
      ).expressionAttributeValues(
        Map(
          ":pid"          -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min"          -> AttributeValue.builder().n(0.toString).build(),
          ":max"          -> AttributeValue.builder().n(Long.MaxValue.toString).build(),
          ":maxTimestamp" -> AttributeValue.builder().n(maxTimestamp.toString).build()
        ).asJava
      ).consistentRead(consistentRead).build()
    queryDelete(queryRequest)
  }

  override def deleteUpToMaxSequenceNrAndMaxTimestamp(
      persistenceId: PersistenceId,
      maxSequenceNr: SequenceNumber,
      maxTimestamp: Long
  )(implicit ec: ExecutionContext): Source[Unit, NotUsed] = {
    val queryRequest = QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpression("#pid = :pid and #snr between :min and :max")
      .filterExpression("#created <= :maxTimestamp")
      .expressionAttributeNames(
        Map(
          "#pid"     -> columnsDefConfig.persistenceIdColumnName,
          "#snr"     -> columnsDefConfig.sequenceNrColumnName,
          "#created" -> columnsDefConfig.createdColumnName
        ).asJava
      ).expressionAttributeValues(
        Map(
          ":pid"          -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min"          -> AttributeValue.builder().n(0.toString).build(),
          ":max"          -> AttributeValue.builder().n(maxSequenceNr.asString).build(),
          ":maxTimestamp" -> AttributeValue.builder().n(maxTimestamp.toString).build()
        ).asJava
      ).consistentRead(consistentRead).build()
    queryDelete(queryRequest)
  }

  private def deserialize(
      rowOpt: Option[Map[String, AttributeValue]]
  )(implicit ec: ExecutionContext): Future[Option[(SnapshotMetadata, Any)]] = {
    rowOpt match {
      case Some(row) =>
        serializer
          .deserialize(
            SnapshotRow(
              persistenceId = PersistenceId(row(columnsDefConfig.persistenceIdColumnName).s),
              sequenceNumber = SequenceNumber(row(columnsDefConfig.sequenceNrColumnName).n.toLong),
              snapshot = row(columnsDefConfig.snapshotColumnName).b.asByteArray(),
              created = row(columnsDefConfig.createdColumnName).n.toLong
            )
          ).map(Some(_))
      case None =>
        Future.successful(None)
    }
  }

  override def latestSnapshot(
      persistenceId: PersistenceId
  )(implicit ec: ExecutionContext): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
    val queryRequest = QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpression("#pid = :pid and #snr between :min and :max")
      .expressionAttributeNames(
        Map("#pid" -> columnsDefConfig.persistenceIdColumnName, "#snr" -> columnsDefConfig.sequenceNrColumnName).asJava
      ).expressionAttributeValues(
        Map(
          ":pid" -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min" -> AttributeValue.builder().n(0.toString).build(),
          ":max" -> AttributeValue.builder().n(Long.MaxValue.toString).build()
        ).asJava
      )
      .scanIndexForward(false)
      .limit(1)
      .consistentRead(consistentRead)
      .build()
    Source
      .single(queryRequest).via(streamReadClient.queryFlow)
      .flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful)
          Source.single(Option(response.items).map(_.asScala).getOrElse(Seq.empty).map(_.asScala.toMap).headOption)
        else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
        }
      }.mapAsync(1)(deserialize)
  }

  override def snapshotForMaxTimestamp(
      persistenceId: PersistenceId,
      maxTimestamp: Long
  )(implicit ec: ExecutionContext): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
    val queryRequest = QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpression("#pid = :pid and #snr between :min and :max")
      .filterExpression("#created <= :maxTimestamp")
      .expressionAttributeNames(
        Map(
          "#pid"     -> columnsDefConfig.persistenceIdColumnName,
          "#snr"     -> columnsDefConfig.sequenceNrColumnName,
          "#created" -> columnsDefConfig.createdColumnName
        ).asJava
      ).expressionAttributeValues(
        Map(
          ":pid"          -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min"          -> AttributeValue.builder().n(0.toString).build(),
          ":max"          -> AttributeValue.builder().n(Long.MaxValue.toString).build(),
          ":maxTimestamp" -> AttributeValue.builder().n(maxTimestamp.toString).build()
        ).asJava
      ).scanIndexForward(false)
      .consistentRead(consistentRead)
      .build()
    Source
      .single(queryRequest).via(streamReadClient.queryFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful)
          Source.single(Option(response.items).map(_.asScala).getOrElse(Seq.empty).map(_.asScala.toMap).headOption)
        else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
        }
      }.mapAsync(1)(deserialize)
  }

  override def snapshotForMaxSequenceNr(
      persistenceId: PersistenceId,
      maxSequenceNr: SequenceNumber
  )(implicit ec: ExecutionContext): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
    val queryRequest = QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpression("#pid = :pid and #snr between :min and :max")
      .expressionAttributeNames(
        Map("#pid" -> columnsDefConfig.persistenceIdColumnName, "#snr" -> columnsDefConfig.sequenceNrColumnName).asJava
      ).expressionAttributeValues(
        Map(
          ":pid" -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min" -> AttributeValue.builder().n(0.toString).build(),
          ":max" -> AttributeValue.builder().n(maxSequenceNr.asString).build()
        ).asJava
      ).scanIndexForward(false)
      .consistentRead(consistentRead)
      .build()
    Source
      .single(queryRequest).via(streamReadClient.queryFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful)
          Source.single(Option(response.items).map(_.asScala).getOrElse(Seq.empty).map(_.asScala.toMap).headOption)
        else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
        }
      }.mapAsync(1)(deserialize)
  }

  override def snapshotForMaxSequenceNrAndMaxTimestamp(
      persistenceId: PersistenceId,
      maxSequenceNr: SequenceNumber,
      maxTimestamp: Long
  )(implicit ec: ExecutionContext): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
    val queryRequest = QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpression("#pid = :pid and #snr between :min and :max")
      .filterExpression("#created <= :maxTimestamp")
      .expressionAttributeNames(
        Map(
          "#pid"     -> columnsDefConfig.persistenceIdColumnName,
          "#snr"     -> columnsDefConfig.sequenceNrColumnName,
          "#created" -> columnsDefConfig.createdColumnName
        ).asJava
      ).expressionAttributeValues(
        Map(
          ":pid"          -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min"          -> AttributeValue.builder().n(0.toString).build(),
          ":max"          -> AttributeValue.builder().n(maxSequenceNr.asString).build(),
          ":maxTimestamp" -> AttributeValue.builder().n(maxTimestamp.toString).build()
        ).asJava
      ).scanIndexForward(false)
      .consistentRead(consistentRead)
      .build()
    Source
      .single(queryRequest).via(streamReadClient.queryFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful)
          Source.single(Option(response.items).map(_.asScala).getOrElse(Seq.empty).map(_.asScala.toMap).headOption)
        else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
        }
      }.mapAsync(1)(deserialize)
  }

  override def save(snapshotMetadata: SnapshotMetadata, snapshot: Any)(implicit
      ec: ExecutionContext
  ): Source[Unit, NotUsed] = {
    Source
      .future(serializer.serialize(snapshotMetadata, snapshot))
      .flatMapConcat { snapshotRow =>
        val req = PutItemRequest
          .builder()
          .tableName(tableName)
          .item(
            Map(
              columnsDefConfig.persistenceIdColumnName -> AttributeValue
                .builder()
                .s(snapshotRow.persistenceId.asString).build(),
              columnsDefConfig.sequenceNrColumnName -> AttributeValue
                .builder()
                .n(snapshotRow.sequenceNumber.asString).build(),
              columnsDefConfig.snapshotColumnName -> AttributeValue
                .builder().b(SdkBytes.fromByteArray(snapshotRow.snapshot)).build(),
              columnsDefConfig.createdColumnName -> AttributeValue.builder().n(snapshotRow.created.toString).build()
            ).asJava
          ).build()
        Source.single(req).via(streamWriteClient.putItemFlow).flatMapConcat { response =>
          if (response.sdkHttpResponse().isSuccessful)
            Source.single(())
          else {
            val statusCode = response.sdkHttpResponse().statusCode()
            val statusText = response.sdkHttpResponse().statusText()
            Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
          }
        }

      }
  }

  private def queryDelete(queryRequest: QueryRequest): Source[Unit, NotUsed] = {
    Source
      .single(queryRequest).via(streamReadClient.queryFlow).map { response =>
        Option(response.items).map(_.asScala).getOrElse(Seq.empty)
      }.mapConcat(_.toVector).grouped(clientConfig.batchWriteItemLimit).map { rows =>
        rows.map { javaRow =>
          val row = javaRow.asScala
          SnapshotRow(
            persistenceId = PersistenceId(row(columnsDefConfig.persistenceIdColumnName).s),
            sequenceNumber = SequenceNumber(row(columnsDefConfig.sequenceNrColumnName).n.toLong),
            snapshot = row(columnsDefConfig.snapshotColumnName).b.asByteArray(),
            created = row(columnsDefConfig.createdColumnName).n.toLong
          )
        }
      }.map { rows =>
        BatchWriteItemRequest
          .builder().requestItems(
            Map(
              tableName -> rows.map { row =>
                WriteRequest
                  .builder().deleteRequest(
                    DeleteRequest
                      .builder()
                      .key(
                        Map(
                          columnsDefConfig.persistenceIdColumnName -> AttributeValue
                            .builder()
                            .s(row.persistenceId.asString).build(),
                          columnsDefConfig.sequenceNrColumnName -> AttributeValue
                            .builder()
                            .n(row.sequenceNumber.asString).build()
                        ).asJava
                      ).build()
                  ).build()
              }.asJava
            ).asJava
          ).build()
      }.via(streamWriteClient.batchWriteItemFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful)
          Source.single(())
        else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
        }
      }
  }

}
