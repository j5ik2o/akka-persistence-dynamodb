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
import akka.stream.FlowShape
import akka.stream.scaladsl.{ Flow, GraphDSL, RestartFlow, Source, Unzip, Zip }
import com.github.j5ik2o.akka.persistence.dynamodb.config.SnapshotPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, Stopwatch }
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.github.j5ik2o.reactive.aws.dynamodb.implicits._
import com.github.j5ik2o.reactive.aws.dynamodb.{ DynamoDbAsyncClient, DynamoDbSyncClient }
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model._

class V2SnapshotDaoImpl(
    asyncClient: Option[DynamoDbAsyncClient],
    syncClient: Option[DynamoDbSyncClient],
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

  private val serializer   = new ByteArraySnapshotSerializer(serialization)
  private val streamClient = asyncClient.map(DynamoDbAkkaClient(_))

  override def delete(persistenceId: PersistenceId, sequenceNr: SequenceNumber): Source[Unit, NotUsed] = {
    val req = DeleteItemRequest
      .builder()
      .tableName(tableName).keyAsScala(
        Map(
          columnsDefConfig.persistenceIdColumnName -> AttributeValue.builder().s(persistenceId.asString).build(),
          columnsDefConfig.sequenceNrColumnName    -> AttributeValue.builder().n(sequenceNr.asString).build()
        )
      ).build()
    Source.single(req).via(deleteItemFlow).map(_ => ())
  }

  override def deleteAllSnapshots(persistenceId: PersistenceId): Source[Unit, NotUsed] = {
    val queryRequest = QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpression("#pid = :pid and #snr between :min and :max")
      .expressionAttributeNamesAsScala(
        Map("#pid" -> columnsDefConfig.persistenceIdColumnName, "#snr" -> columnsDefConfig.sequenceNrColumnName)
      ).expressionAttributeValuesAsScala(
        Map(
          ":pid" -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min" -> AttributeValue.builder().n(0.toString).build(),
          ":max" -> AttributeValue.builder().n(Long.MaxValue.toString).build()
        )
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
      .expressionAttributeNamesAsScala(
        Map("#pid" -> columnsDefConfig.persistenceIdColumnName, "#snr" -> columnsDefConfig.sequenceNrColumnName)
      ).expressionAttributeValuesAsScala(
        Map(
          ":pid" -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min" -> AttributeValue.builder().n(0.toString).build(),
          ":max" -> AttributeValue.builder().n(maxSequenceNr.asString).build()
        )
      ).consistentRead(consistentRead).build()
    queryDelete(queryRequest)
  }

  override def deleteUpToMaxTimestamp(persistenceId: PersistenceId, maxTimestamp: Long): Source[Unit, NotUsed] = {
    val queryRequest = QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpression("#pid = :pid and #snr between :min and :max")
      .filterExpression("#created <= :maxTimestamp")
      .expressionAttributeNamesAsScala(
        Map(
          "#pid"     -> columnsDefConfig.persistenceIdColumnName,
          "#snr"     -> columnsDefConfig.sequenceNrColumnName,
          "#created" -> columnsDefConfig.createdColumnName
        )
      ).expressionAttributeValuesAsScala(
        Map(
          ":pid"          -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min"          -> AttributeValue.builder().n(0.toString).build(),
          ":max"          -> AttributeValue.builder().n(Long.MaxValue.toString).build(),
          ":maxTimestamp" -> AttributeValue.builder().n(maxTimestamp.toString).build()
        )
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
      .expressionAttributeNamesAsScala(
        Map(
          "#pid"     -> columnsDefConfig.persistenceIdColumnName,
          "#snr"     -> columnsDefConfig.sequenceNrColumnName,
          "#created" -> columnsDefConfig.createdColumnName
        )
      ).expressionAttributeValuesAsScala(
        Map(
          ":pid"          -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min"          -> AttributeValue.builder().n(0.toString).build(),
          ":max"          -> AttributeValue.builder().n(maxSequenceNr.asString).build(),
          ":maxTimestamp" -> AttributeValue.builder().n(maxTimestamp.toString).build()
        )
      ).consistentRead(consistentRead).build()
    queryDelete(queryRequest)
  }

  override def latestSnapshot(persistenceId: PersistenceId): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
    val queryRequest = QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpression("#pid = :pid and #snr between :min and :max")
      .expressionAttributeNamesAsScala(
        Map("#pid" -> columnsDefConfig.persistenceIdColumnName, "#snr" -> columnsDefConfig.sequenceNrColumnName)
      ).expressionAttributeValuesAsScala(
        Map(
          ":pid" -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min" -> AttributeValue.builder().n(0.toString).build(),
          ":max" -> AttributeValue.builder().n(Long.MaxValue.toString).build()
        )
      )
      .scanIndexForward(false)
      .limit(1)
      .consistentRead(consistentRead)
      .build()
    Source
      .single(queryRequest).via(queryFlow)
      .flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful)
          Source.single(response.itemsAsScala.getOrElse(Seq.empty).headOption)
        else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
        }
      }.map { rows =>
        rows.map { row =>
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
      .expressionAttributeNamesAsScala(
        Map(
          "#pid"     -> columnsDefConfig.persistenceIdColumnName,
          "#snr"     -> columnsDefConfig.sequenceNrColumnName,
          "#created" -> columnsDefConfig.createdColumnName
        )
      ).expressionAttributeValuesAsScala(
        Map(
          ":pid"          -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min"          -> AttributeValue.builder().n(0.toString).build(),
          ":max"          -> AttributeValue.builder().n(Long.MaxValue.toString).build(),
          ":maxTimestamp" -> AttributeValue.builder().n(maxTimestamp.toString).build()
        )
      ).scanIndexForward(false)
      .consistentRead(consistentRead)
      .build()
    Source
      .single(queryRequest).via(queryFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful)
          Source.single(response.itemsAsScala.getOrElse(Seq.empty).headOption)
        else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
        }
      }.map { rows =>
        rows.map { row =>
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
      .expressionAttributeNamesAsScala(
        Map("#pid" -> columnsDefConfig.persistenceIdColumnName, "#snr" -> columnsDefConfig.sequenceNrColumnName)
      ).expressionAttributeValuesAsScala(
        Map(
          ":pid" -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min" -> AttributeValue.builder().n(0.toString).build(),
          ":max" -> AttributeValue.builder().n(maxSequenceNr.asString).build()
        )
      ).scanIndexForward(false)
      .consistentRead(consistentRead)
      .build()
    Source
      .single(queryRequest).via(queryFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful)
          Source.single(response.itemsAsScala.getOrElse(Seq.empty).headOption)
        else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
        }
      }.map { rows =>
        rows.map { row =>
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
      .expressionAttributeNamesAsScala(
        Map(
          "#pid"     -> columnsDefConfig.persistenceIdColumnName,
          "#snr"     -> columnsDefConfig.sequenceNrColumnName,
          "#created" -> columnsDefConfig.createdColumnName
        )
      ).expressionAttributeValuesAsScala(
        Map(
          ":pid"          -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min"          -> AttributeValue.builder().n(0.toString).build(),
          ":max"          -> AttributeValue.builder().n(maxSequenceNr.asString).build(),
          ":maxTimestamp" -> AttributeValue.builder().n(maxTimestamp.toString).build()
        )
      ).scanIndexForward(false)
      .consistentRead(consistentRead)
      .build()
    Source
      .single(queryRequest).via(queryFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful)
          Source.single(response.itemsAsScala.getOrElse(Seq.empty).headOption)
        else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
        }
      }.map { rows =>
        rows.map { row =>
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
          .itemAsScala(
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
            )
          ).build()
        Source.single(req).via(putItemFlow).map(_ => ())
      case Left(ex) =>
        Source.failed(ex)
    }
  }

  private def queryDelete(queryRequest: QueryRequest): Source[Unit, NotUsed] = {
    Source
      .single(queryRequest).via(queryFlow).map {
        _.itemsAsScala.getOrElse(Seq.empty)
      }.mapConcat(_.toVector).grouped(clientConfig.batchWriteItemLimit).map { rows =>
        rows.map { row =>
          SnapshotRow(
            persistenceId = PersistenceId(row(columnsDefConfig.persistenceIdColumnName).s),
            sequenceNumber = SequenceNumber(row(columnsDefConfig.sequenceNrColumnName).n.toLong),
            snapshot = row(columnsDefConfig.snapshotColumnName).b.asByteArray(),
            created = row(columnsDefConfig.createdColumnName).n.toLong
          )
        }
      }.map { rows =>
        BatchWriteItemRequest
          .builder().requestItemsAsScala(
            Map(
              tableName -> rows.map { row =>
                WriteRequest
                  .builder().deleteRequest(
                    DeleteRequest
                      .builder()
                      .keyAsScala(
                        Map(
                          columnsDefConfig.persistenceIdColumnName -> AttributeValue
                            .builder()
                            .s(row.persistenceId.asString).build(),
                          columnsDefConfig.sequenceNrColumnName -> AttributeValue
                            .builder()
                            .n(row.sequenceNumber.asString).build()
                        )
                      ).build()
                  ).build()
              }
            )
          ).build()
      }.via(batchWriteItemFlow).map(_ => ())
  }

  private def queryFlow: Flow[QueryRequest, QueryResponse, NotUsed] = {
    val flow = ((streamClient, syncClient) match {
      case (Some(c), None) =>
        c.queryFlow(1)
      case (None, Some(c)) =>
        val flow = Flow[QueryRequest].map { request =>
          c.query(request) match {
            case Right(value) => value
            case Left(ex)     => throw ex
          }
        }
        DispatcherUtils.applyV2Dispatcher(pluginConfig, flow)
      case _ =>
        throw new IllegalStateException("invalid state")
    }).log("query")
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
    val flow = ((streamClient, syncClient) match {
      case (Some(c), None) => c.putItemFlow(1)
      case (None, Some(c)) =>
        val flow = Flow[PutItemRequest].map { request =>
          c.putItem(request) match {
            case Right(value) => value
            case Left(ex)     => throw ex
          }
        }
        DispatcherUtils.applyV2Dispatcher(pluginConfig, flow)
      case _ =>
        throw new IllegalStateException("invalid state")
    }).log("putItem")
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
    val flow = ((streamClient, syncClient) match {
      case (Some(c), None) => c.deleteItemFlow(1)
      case (None, Some(c)) =>
        val flow = Flow[DeleteItemRequest].map { request =>
          c.deleteItem(request) match {
            case Right(value) => value
            case Left(ex)     => throw ex
          }
        }
        DispatcherUtils.applyV2Dispatcher(pluginConfig, flow)
      case _ =>
        throw new IllegalStateException("invalid state")
    }).log("deleteItem")
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
    val flow = ((streamClient, syncClient) match {
      case (Some(c), None) => c.batchWriteItemFlow(1)
      case (None, Some(c)) =>
        val flow = Flow[BatchWriteItemRequest].map { request =>
          c.batchWriteItem(request) match {
            case Right(value) => value
            case Left(ex)     => throw ex
          }
        }
        DispatcherUtils.applyV2Dispatcher(pluginConfig, flow)
      case _ =>
        throw new IllegalStateException("invalid state")
    }).log("batchWriteItem")
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
