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

import java.io.IOException

import akka.NotUsed
import akka.persistence.SnapshotMetadata
import akka.serialization.Serialization
import akka.stream.javadsl.{ Flow => JavaFlow }
import akka.stream.scaladsl.{ Flow, RestartFlow, Source }
import com.github.j5ik2o.akka.persistence.dynamodb.config.SnapshotPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils._
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbClient => JavaDynamoDbSyncClient
}

import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

class V2SnapshotDaoImpl(
    asyncClient: Option[JavaDynamoDbAsyncClient],
    syncClient: Option[JavaDynamoDbSyncClient],
    serialization: Serialization,
    pluginConfig: SnapshotPluginConfig,
    metricsReporter: Option[MetricsReporter]
) extends SnapshotDao {
  (asyncClient, syncClient) match {
    case (None, None) =>
      throw new IllegalArgumentException("aws clients is both None")
    case _ =>
  }

  import pluginConfig._

  private val serializer = new ByteArraySnapshotSerializer(serialization)

  override def delete(persistenceId: PersistenceId, sequenceNr: SequenceNumber): Source[Unit, NotUsed] = {
    val req = DeleteItemRequest
      .builder()
      .tableName(tableName).key(
        Map(
          columnsDefConfig.persistenceIdColumnName -> AttributeValue.builder().s(persistenceId.asString).build(),
          columnsDefConfig.sequenceNrColumnName    -> AttributeValue.builder().n(sequenceNr.asString).build()
        ).asJava
      ).build()
    Source.single(req).via(deleteItemFlow).flatMapConcat { response =>
      if (response.sdkHttpResponse().isSuccessful)
        Source.single(())
      else {
        val statusCode = response.sdkHttpResponse().statusCode()
        val statusText = response.sdkHttpResponse().statusText()
        Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
      }
    }
  }

  override def deleteAllSnapshots(persistenceId: PersistenceId): Source[Unit, NotUsed] = {
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
  ): Source[Unit, NotUsed] = {
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

  override def deleteUpToMaxTimestamp(persistenceId: PersistenceId, maxTimestamp: Long): Source[Unit, NotUsed] = {
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
          ":max"          -> AttributeValue.builder().n(maxSequenceNr.asString).build(),
          ":maxTimestamp" -> AttributeValue.builder().n(maxTimestamp.toString).build()
        ).asJava
      ).consistentRead(consistentRead).build()
    queryDelete(queryRequest)
  }

  override def latestSnapshot(persistenceId: PersistenceId): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
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
      .single(queryRequest).via(queryFlow)
      .flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful)
          Source.single(Option(response.items).map(_.asScala).getOrElse(Seq.empty).headOption)
        else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
        }
      }.map { rows =>
        rows.map { javaRow =>
          val row = javaRow.asScala
          serializer
            .deserialize(
              SnapshotRow(
                persistenceId = PersistenceId(row(columnsDefConfig.persistenceIdColumnName).s),
                sequenceNumber = SequenceNumber(row(columnsDefConfig.sequenceNrColumnName).n.toLong),
                snapshot = row(columnsDefConfig.snapshotColumnName).b.asByteArray(),
                created = row(columnsDefConfig.createdColumnName).n.toLong
              )
            ) match {
            case Right(value) => value
            case Left(ex)     => throw ex
          }
        }
      }
  }

  override def snapshotForMaxTimestamp(
      persistenceId: PersistenceId,
      maxTimestamp: Long
  ): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
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
      .single(queryRequest).via(queryFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful)
          Source.single(Option(response.items).map(_.asScala).getOrElse(Seq.empty).headOption)
        else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
        }
      }.map { rows =>
        rows.map { javaRow =>
          val row = javaRow.asScala
          serializer
            .deserialize(
              SnapshotRow(
                persistenceId = PersistenceId(row(columnsDefConfig.persistenceIdColumnName).s),
                sequenceNumber = SequenceNumber(row(columnsDefConfig.sequenceNrColumnName).n.toLong),
                snapshot = row(columnsDefConfig.snapshotColumnName).b.asByteArray(),
                created = row(columnsDefConfig.createdColumnName).n.toLong
              )
            ) match {
            case Right(value) => value
            case Left(ex)     => throw ex
          }
        }
      }
  }

  override def snapshotForMaxSequenceNr(
      persistenceId: PersistenceId,
      maxSequenceNr: SequenceNumber
  ): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
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
      .single(queryRequest).via(queryFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful)
          Source.single(Option(response.items).map(_.asScala).getOrElse(Seq.empty).headOption)
        else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
        }
      }.map { rows =>
        rows.map { javaRow =>
          val row = javaRow.asScala
          serializer
            .deserialize(
              SnapshotRow(
                persistenceId = PersistenceId(row(columnsDefConfig.persistenceIdColumnName).s),
                sequenceNumber = SequenceNumber(row(columnsDefConfig.sequenceNrColumnName).n.toLong),
                snapshot = row(columnsDefConfig.snapshotColumnName).b.asByteArray(),
                created = row(columnsDefConfig.createdColumnName).n.toLong
              )
            ) match {
            case Right(value) => value
            case Left(ex)     => throw ex
          }
        }
      }
  }

  override def snapshotForMaxSequenceNrAndMaxTimestamp(
      persistenceId: PersistenceId,
      maxSequenceNr: SequenceNumber,
      maxTimestamp: Long
  ): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
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
      .single(queryRequest).via(queryFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful)
          Source.single(Option(response.items).map(_.asScala).getOrElse(Seq.empty).headOption)
        else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
        }
      }.map { rows =>
        rows.map { javaRow =>
          val row = javaRow.asScala
          serializer
            .deserialize(
              SnapshotRow(
                persistenceId = PersistenceId(row(columnsDefConfig.persistenceIdColumnName).s),
                sequenceNumber = SequenceNumber(row(columnsDefConfig.sequenceNrColumnName).n.toLong),
                snapshot = row(columnsDefConfig.snapshotColumnName).b.asByteArray(),
                created = row(columnsDefConfig.createdColumnName).n.toLong
              )
            ) match {
            case Right(value) => value
            case Left(ex)     => throw ex
          }
        }
      }
  }

  override def save(snapshotMetadata: SnapshotMetadata, snapshot: Any): Source[Unit, NotUsed] = {
    serializer
      .serialize(snapshotMetadata, snapshot) match {
      case Right(snapshotRow) =>
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
        Source.single(req).via(putItemFlow).flatMapConcat { response =>
          if (response.sdkHttpResponse().isSuccessful)
            Source.single(())
          else {
            val statusCode = response.sdkHttpResponse().statusCode()
            val statusText = response.sdkHttpResponse().statusText()
            Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
          }
        }
      case Left(ex) =>
        Source.failed(ex)
    }
  }

  private def queryDelete(queryRequest: QueryRequest): Source[Unit, NotUsed] = {
    Source
      .single(queryRequest).via(queryFlow).map { response =>
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
      }.via(batchWriteItemFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful)
          Source.single(())
        else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
        }
      }
  }

  private def queryFlow: Flow[QueryRequest, QueryResponse, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          JavaFlow.create[QueryRequest]().mapAsync(1, { request => c.query(request) }).asScala
        case (None, Some(c)) =>
          Flow[QueryRequest].map { request => c.query(request) }.withV2Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("queryFlow")
    if (pluginConfig.readBackoffConfig.enabled)
      RestartFlow
        .withBackoff(
          minBackoff = pluginConfig.readBackoffConfig.minBackoff,
          maxBackoff = pluginConfig.readBackoffConfig.maxBackoff,
          randomFactor = pluginConfig.readBackoffConfig.randomFactor,
          maxRestarts = pluginConfig.readBackoffConfig.maxRestarts
        ) { () => flow }
    else flow
  }

  private def putItemFlow: Flow[PutItemRequest, PutItemResponse, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          JavaFlow.create[PutItemRequest]().mapAsync(1, { request => c.putItem(request) }).asScala
        case (None, Some(c)) =>
          Flow[PutItemRequest].map { request => c.putItem(request) }.withV2Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("putItemFlow")
    if (pluginConfig.writeBackoffConfig.enabled)
      RestartFlow
        .withBackoff(
          minBackoff = pluginConfig.writeBackoffConfig.minBackoff,
          maxBackoff = pluginConfig.writeBackoffConfig.maxBackoff,
          randomFactor = pluginConfig.writeBackoffConfig.randomFactor,
          maxRestarts = pluginConfig.writeBackoffConfig.maxRestarts
        ) { () => flow }
    else flow
  }

  private def deleteItemFlow: Flow[DeleteItemRequest, DeleteItemResponse, NotUsed] = {
    val flow = (
      (asyncClient, syncClient) match {
        case (Some(c), None) =>
          JavaFlow.create[DeleteItemRequest]().mapAsync(1, { request => c.deleteItem(request) }).asScala
        case (None, Some(c)) =>
          Flow[DeleteItemRequest].map { request => c.deleteItem(request) }.withV2Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }
    ).log("deleteItemFlow")
    if (pluginConfig.writeBackoffConfig.enabled)
      RestartFlow
        .withBackoff(
          minBackoff = pluginConfig.writeBackoffConfig.minBackoff,
          maxBackoff = pluginConfig.writeBackoffConfig.maxBackoff,
          randomFactor = pluginConfig.writeBackoffConfig.randomFactor,
          maxRestarts = pluginConfig.writeBackoffConfig.maxRestarts
        ) { () => flow }
    else flow
  }

  private def batchWriteItemFlow: Flow[BatchWriteItemRequest, BatchWriteItemResponse, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          JavaFlow.create[BatchWriteItemRequest]().mapAsync(1, { request => c.batchWriteItem(request) }).asScala
        case (None, Some(c)) =>
          Flow[BatchWriteItemRequest].map { request => c.batchWriteItem(request) }.withV2Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("batchWriteItemFlow")
    if (pluginConfig.writeBackoffConfig.enabled)
      RestartFlow
        .withBackoff(
          minBackoff = pluginConfig.writeBackoffConfig.minBackoff,
          maxBackoff = pluginConfig.writeBackoffConfig.maxBackoff,
          randomFactor = pluginConfig.writeBackoffConfig.randomFactor,
          maxRestarts = pluginConfig.writeBackoffConfig.maxRestarts
        ) { () => flow }
    else flow
  }

}
