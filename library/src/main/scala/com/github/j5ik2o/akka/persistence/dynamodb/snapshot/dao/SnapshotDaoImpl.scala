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
import akka.persistence.SnapshotMetadata
import akka.serialization.Serialization
import akka.stream.scaladsl.Source
import com.github.j5ik2o.akka.persistence.dynamodb.config.SnapshotPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.reactive.aws.dynamodb.implicits._
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model._

class SnapshotDaoImpl(
    asyncClient: DynamoDbAsyncClient,
    serialization: Serialization,
    pluginConfig: SnapshotPluginConfig
) extends SnapshotDao {
  import pluginConfig._

  private val serializer                       = new ByteArraySnapshotSerializer(serialization)
  private val streamClient: DynamoDbAkkaClient = DynamoDbAkkaClient(asyncClient)

  def toSnapshotData(row: SnapshotRow): (SnapshotMetadata, Any) =
    serializer.deserialize(row) match {
      case Right(deserialized) => deserialized
      case Left(cause)         => throw cause
    }

  override def delete(persistenceId: PersistenceId, sequenceNr: SequenceNumber): Source[Unit, NotUsed] = {
    val req = DeleteItemRequest
      .builder()
      .tableName(tableName).keyAsScala(
        Map(
          columnsDefConfig.persistenceIdColumnName -> AttributeValue.builder().s(persistenceId.asString).build(),
          columnsDefConfig.sequenceNrColumnName    -> AttributeValue.builder().n(sequenceNr.asString).build()
        )
      ).build()
    Source.single(req).via(streamClient.deleteItemFlow(1)).map(_ => ())
  }

  private def queryDelete(queryRequest: QueryRequest): Source[Unit, NotUsed] = {
    Source
      .single(queryRequest).via(streamClient.queryFlow(1)).map {
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
      }.via(streamClient.batchWriteItemFlow(1)).map(_ => ())
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
      ).consistentRead(pluginConfig.consistentRead).build()
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
      ).consistentRead(pluginConfig.consistentRead).build()
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
      ).build()
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
      ).consistentRead(pluginConfig.consistentRead).build()
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
      .limit(1).build()
    Source
      .single(queryRequest).via(streamClient.queryFlow(1)).map { response => response.itemsAsScala.get.headOption }.map {
        rows =>
          rows.map { row =>
            serializer
              .deserialize(
                SnapshotRow(
                  persistenceId = PersistenceId(row(columnsDefConfig.persistenceIdColumnName).s),
                  sequenceNumber = SequenceNumber(row(columnsDefConfig.sequenceNrColumnName).n.toLong),
                  snapshot = row(columnsDefConfig.snapshotColumnName).b.asByteArray(),
                  created = row(columnsDefConfig.createdColumnName).n.toLong
                )
              ).right.get
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
      ).scanIndexForward(false).build()
    Source
      .single(queryRequest).via(streamClient.queryFlow(1)).map { response => response.itemsAsScala.get.headOption }.map {
        rows =>
          rows.map { row =>
            serializer
              .deserialize(
                SnapshotRow(
                  persistenceId = PersistenceId(row(columnsDefConfig.persistenceIdColumnName).s),
                  sequenceNumber = SequenceNumber(row(columnsDefConfig.sequenceNrColumnName).n.toLong),
                  snapshot = row(columnsDefConfig.snapshotColumnName).b.asByteArray(),
                  created = row(columnsDefConfig.createdColumnName).n.toLong
                )
              ).right.get
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
      ).scanIndexForward(false).build()
    Source
      .single(queryRequest).via(streamClient.queryFlow(1)).map { response => response.itemsAsScala.get.headOption }.map {
        rows =>
          rows.map { row =>
            serializer
              .deserialize(
                SnapshotRow(
                  persistenceId = PersistenceId(row(columnsDefConfig.persistenceIdColumnName).s),
                  sequenceNumber = SequenceNumber(row(columnsDefConfig.sequenceNrColumnName).n.toLong),
                  snapshot = row(columnsDefConfig.snapshotColumnName).b.asByteArray(),
                  created = row(columnsDefConfig.createdColumnName).n.toLong
                )
              ).right.get
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
      ).scanIndexForward(false).build()
    Source
      .single(queryRequest).via(streamClient.queryFlow(1)).map { response => response.itemsAsScala.get.headOption }.map {
        rows =>
          rows.map { row =>
            serializer
              .deserialize(
                SnapshotRow(
                  persistenceId = PersistenceId(row(columnsDefConfig.persistenceIdColumnName).s),
                  sequenceNumber = SequenceNumber(row(columnsDefConfig.sequenceNrColumnName).n.toLong),
                  snapshot = row(columnsDefConfig.snapshotColumnName).b.asByteArray(),
                  created = row(columnsDefConfig.createdColumnName).n.toLong
                )
              ).right.get
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
        Source.single(req).via(streamClient.putItemFlow(1)).map(_ => ())
      case Left(ex) =>
        Source.failed(ex)
    }

  }
}
