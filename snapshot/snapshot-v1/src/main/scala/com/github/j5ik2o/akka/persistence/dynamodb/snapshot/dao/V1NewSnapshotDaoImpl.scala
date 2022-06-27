/*
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
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v1.{ StreamReadClient, StreamWriteClient }
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.SnapshotPluginContext
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.serialization.ByteArraySnapshotSerializer

import java.io.IOException
import java.nio.ByteBuffer
import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._

final class V1NewSnapshotDaoImpl(
    pluginContext: SnapshotPluginContext,
    asyncClient: Option[AmazonDynamoDBAsync],
    syncClient: Option[AmazonDynamoDB],
    serialization: Serialization
) extends SnapshotDao {
  (asyncClient, syncClient) match {
    case (None, None) =>
      throw new IllegalArgumentException("aws clients is both None")
    case _ =>
  }

  import pluginContext._
  import pluginContext.pluginConfig._

  private val streamReadClient =
    new StreamReadClient(pluginContext, asyncClient, syncClient, pluginConfig.readBackoffConfig)

  private val streamWriteClient =
    new StreamWriteClient(pluginContext, asyncClient, syncClient, pluginConfig.writeBackoffConfig)

  private val serializer = new ByteArraySnapshotSerializer(serialization, metricsReporter, traceReporter)

  override def delete(persistenceId: PersistenceId, sequenceNr: SequenceNumber): Source[Unit, NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
      .withIndexName(pluginConfig.getSnapshotRowsIndexName)
      .withKeyConditionExpression("#pid = :pid and #snr = :snr")
      .withExpressionAttributeNames(
        Map("#pid" -> columnsDefConfig.persistenceIdColumnName, "#snr" -> columnsDefConfig.sequenceNrColumnName).asJava
      ).withExpressionAttributeValues(
        Map(
          ":pid" -> new AttributeValue().withS(persistenceId.asString),
          ":snr" -> new AttributeValue().withN(sequenceNr.asString)
        ).asJava
      ).withConsistentRead(consistentRead)
    queryDelete(queryRequest)
  }

  override def deleteAllSnapshots(
      persistenceId: PersistenceId
  )(implicit ec: ExecutionContext): Source[Unit, NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
      .withIndexName(pluginConfig.getSnapshotRowsIndexName)
      .withKeyConditionExpression("#pid = :pid and #snr between :min and :max")
      .withExpressionAttributeNames(
        Map("#pid" -> columnsDefConfig.persistenceIdColumnName, "#snr" -> columnsDefConfig.sequenceNrColumnName).asJava
      ).withExpressionAttributeValues(
        Map(
          ":pid" -> new AttributeValue().withS(persistenceId.asString),
          ":min" -> new AttributeValue().withN(0.toString),
          ":max" -> new AttributeValue().withN(Long.MaxValue.toString)
        ).asJava
      ).withConsistentRead(consistentRead)
    queryDelete(queryRequest)
  }

  override def deleteUpToMaxSequenceNr(
      persistenceId: PersistenceId,
      maxSequenceNr: SequenceNumber
  )(implicit ec: ExecutionContext): Source[Unit, NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
      .withIndexName(pluginConfig.getSnapshotRowsIndexName)
      .withKeyConditionExpression("#pid = :pid and #snr between :min and :max")
      .withExpressionAttributeNames(
        Map("#pid" -> columnsDefConfig.persistenceIdColumnName, "#snr" -> columnsDefConfig.sequenceNrColumnName).asJava
      ).withExpressionAttributeValues(
        Map(
          ":pid" -> new AttributeValue().withS(persistenceId.asString),
          ":min" -> new AttributeValue().withN(0.toString),
          ":max" -> new AttributeValue().withN(maxSequenceNr.asString)
        ).asJava
      ).withConsistentRead(consistentRead)
    queryDelete(queryRequest)
  }

  override def deleteUpToMaxTimestamp(persistenceId: PersistenceId, maxTimestamp: Long)(implicit
      ec: ExecutionContext
  ): Source[Unit, NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
      .withIndexName(pluginConfig.getSnapshotRowsIndexName)
      .withKeyConditionExpression("#pid = :pid and #snr between :min and :max")
      .withFilterExpression("#created <= :maxTimestamp")
      .withExpressionAttributeNames(
        Map(
          "#pid"     -> columnsDefConfig.persistenceIdColumnName,
          "#snr"     -> columnsDefConfig.sequenceNrColumnName,
          "#created" -> columnsDefConfig.createdColumnName
        ).asJava
      ).withExpressionAttributeValues(
        Map(
          ":pid"          -> new AttributeValue().withS(persistenceId.asString),
          ":min"          -> new AttributeValue().withN(0.toString),
          ":max"          -> new AttributeValue().withN(Long.MaxValue.toString),
          ":maxTimestamp" -> new AttributeValue().withN(maxTimestamp.toString)
        ).asJava
      ).withConsistentRead(consistentRead)
    queryDelete(queryRequest)
  }

  override def deleteUpToMaxSequenceNrAndMaxTimestamp(
      persistenceId: PersistenceId,
      maxSequenceNr: SequenceNumber,
      maxTimestamp: Long
  )(implicit ec: ExecutionContext): Source[Unit, NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
      .withIndexName(pluginConfig.getSnapshotRowsIndexName)
      .withKeyConditionExpression("#pid = :pid and #snr between :min and :max")
      .withFilterExpression("#created <= :maxTimestamp")
      .withExpressionAttributeNames(
        Map(
          "#pid"     -> columnsDefConfig.persistenceIdColumnName,
          "#snr"     -> columnsDefConfig.sequenceNrColumnName,
          "#created" -> columnsDefConfig.createdColumnName
        ).asJava
      ).withExpressionAttributeValues(
        Map(
          ":pid"          -> new AttributeValue().withS(persistenceId.asString),
          ":min"          -> new AttributeValue().withN(0.toString),
          ":max"          -> new AttributeValue().withN(maxSequenceNr.asString),
          ":maxTimestamp" -> new AttributeValue().withN(maxTimestamp.toString)
        ).asJava
      ).withConsistentRead(consistentRead)
    queryDelete(queryRequest)
  }

  override def latestSnapshot(
      persistenceId: PersistenceId
  )(implicit ec: ExecutionContext): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
      .withIndexName(pluginConfig.getSnapshotRowsIndexName)
      .withKeyConditionExpression("#pid = :pid and #snr between :min and :max")
      .withExpressionAttributeNames(
        Map("#pid" -> columnsDefConfig.persistenceIdColumnName, "#snr" -> columnsDefConfig.sequenceNrColumnName).asJava
      ).withExpressionAttributeValues(
        Map(
          ":pid" -> new AttributeValue().withS(persistenceId.asString),
          ":min" -> new AttributeValue().withN(0.toString),
          ":max" -> new AttributeValue().withN(Long.MaxValue.toString)
        ).asJava
      )
      .withScanIndexForward(false)
      .withLimit(1)
      .withConsistentRead(consistentRead)
    Source
      .single(queryRequest).via(streamReadClient.queryFlow)
      .flatMapConcat { response =>
        if (response.getSdkHttpMetadata.getHttpStatusCode == 200)
          Source.single(Option(response.getItems).map(_.asScala).getOrElse(Seq.empty).map(_.asScala.toMap).headOption)
        else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }.mapAsync(1)(deserialize)
  }

  override def snapshotForMaxTimestamp(
      persistenceId: PersistenceId,
      maxTimestamp: Long
  )(implicit ec: ExecutionContext): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
      .withIndexName(pluginConfig.getSnapshotRowsIndexName)
      .withKeyConditionExpression("#pid = :pid and #snr between :min and :max")
      .withFilterExpression("#created <= :maxTimestamp")
      .withExpressionAttributeNames(
        Map(
          "#pid"     -> columnsDefConfig.persistenceIdColumnName,
          "#snr"     -> columnsDefConfig.sequenceNrColumnName,
          "#created" -> columnsDefConfig.createdColumnName
        ).asJava
      ).withExpressionAttributeValues(
        Map(
          ":pid"          -> new AttributeValue().withS(persistenceId.asString),
          ":min"          -> new AttributeValue().withN(0.toString),
          ":max"          -> new AttributeValue().withN(Long.MaxValue.toString),
          ":maxTimestamp" -> new AttributeValue().withN(maxTimestamp.toString)
        ).asJava
      ).withScanIndexForward(false)
      .withConsistentRead(consistentRead)
    Source
      .single(queryRequest).via(streamReadClient.queryFlow).flatMapConcat { response =>
        if (response.getSdkHttpMetadata.getHttpStatusCode == 200)
          Source.single(Option(response.getItems).map(_.asScala).getOrElse(Seq.empty).map(_.asScala.toMap).headOption)
        else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }.mapAsync(1)(deserialize)
  }

  override def snapshotForMaxSequenceNr(
      persistenceId: PersistenceId,
      maxSequenceNr: SequenceNumber
  )(implicit ec: ExecutionContext): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
      .withIndexName(pluginConfig.getSnapshotRowsIndexName)
      .withKeyConditionExpression("#pid = :pid and #snr between :min and :max")
      .withExpressionAttributeNames(
        Map("#pid" -> columnsDefConfig.persistenceIdColumnName, "#snr" -> columnsDefConfig.sequenceNrColumnName).asJava
      ).withExpressionAttributeValues(
        Map(
          ":pid" -> new AttributeValue().withS(persistenceId.asString),
          ":min" -> new AttributeValue().withN(0.toString),
          ":max" -> new AttributeValue().withN(maxSequenceNr.asString)
        ).asJava
      ).withScanIndexForward(false)
      .withConsistentRead(consistentRead)
    Source
      .single(queryRequest).via(streamReadClient.queryFlow).flatMapConcat { response =>
        if (response.getSdkHttpMetadata.getHttpStatusCode == 200)
          Source.single(Option(response.getItems).map(_.asScala).getOrElse(Seq.empty).map(_.asScala.toMap).headOption)
        else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }.mapAsync(1)(deserialize)
  }

  override def snapshotForMaxSequenceNrAndMaxTimestamp(
      persistenceId: PersistenceId,
      maxSequenceNr: SequenceNumber,
      maxTimestamp: Long
  )(implicit ec: ExecutionContext): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
      .withIndexName(pluginConfig.getSnapshotRowsIndexName)
      .withKeyConditionExpression("#pid = :pid and #snr between :min and :max")
      .withFilterExpression("#created <= :maxTimestamp")
      .withExpressionAttributeNames(
        Map(
          "#pid"     -> columnsDefConfig.persistenceIdColumnName,
          "#snr"     -> columnsDefConfig.sequenceNrColumnName,
          "#created" -> columnsDefConfig.createdColumnName
        ).asJava
      ).withExpressionAttributeValues(
        Map(
          ":pid"          -> new AttributeValue().withS(persistenceId.asString),
          ":min"          -> new AttributeValue().withN(0.toString),
          ":max"          -> new AttributeValue().withN(maxSequenceNr.asString),
          ":maxTimestamp" -> new AttributeValue().withN(maxTimestamp.toString)
        ).asJava
      ).withScanIndexForward(false)
      .withConsistentRead(consistentRead)
    Source
      .single(queryRequest).via(streamReadClient.queryFlow).flatMapConcat { response =>
        if (response.getSdkHttpMetadata.getHttpStatusCode == 200)
          Source.single(Option(response.getItems).map(_.asScala).getOrElse(Seq.empty).map(_.asScala.toMap).headOption)
        else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }.mapAsync(1)(deserialize)
  }

  override def save(snapshotMetadata: SnapshotMetadata, snapshot: Any)(implicit
      ec: ExecutionContext
  ): Source[Unit, NotUsed] = {
    Source
      .future(serializer.serialize(snapshotMetadata, snapshot))
      .flatMapConcat { snapshotRow =>
        val pkey = partitionKeyResolver.resolve(snapshotRow.persistenceId, snapshotRow.sequenceNumber)
        val skey = sortKeyResolver.resolve(snapshotRow.persistenceId, snapshotRow.sequenceNumber)
        val req = new PutItemRequest()
          .withTableName(tableName)
          .withItem(
            Map(
              columnsDefConfig.partitionKeyColumnName -> new AttributeValue().withS(pkey.asString),
              columnsDefConfig.sortKeyColumnName      -> new AttributeValue().withS(skey.asString),
              columnsDefConfig.persistenceIdColumnName -> new AttributeValue()
                .withS(snapshotRow.persistenceId.asString),
              columnsDefConfig.sequenceNrColumnName -> new AttributeValue().withN(snapshotRow.sequenceNumber.asString),
              columnsDefConfig.snapshotColumnName -> new AttributeValue().withB(ByteBuffer.wrap(snapshotRow.snapshot)),
              columnsDefConfig.createdColumnName  -> new AttributeValue().withN(snapshotRow.created.toString)
            ).asJava
          )
        Source.single(req).via(streamWriteClient.putItemFlow).flatMapConcat { response =>
          if (response.getSdkHttpMetadata.getHttpStatusCode == 200)
            Source.single(())
          else {
            val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
            Source.failed(new IOException(s"statusCode: $statusCode"))
          }
        }
      }
  }

  private def queryDelete(queryRequest: QueryRequest): Source[Unit, NotUsed] = {
    Source
      .single(queryRequest).via(streamReadClient.queryFlow).map { response =>
        Option(response.getItems).map(_.asScala).getOrElse(Seq.empty)
      }.mapConcat(_.toVector).grouped(clientConfig.batchWriteItemLimit).map { rows =>
        rows.map { javaRow =>
          val row = javaRow.asScala
          SnapshotRow(
            persistenceId = PersistenceId(row(columnsDefConfig.persistenceIdColumnName).getS),
            sequenceNumber = SequenceNumber(row(columnsDefConfig.sequenceNrColumnName).getN.toLong),
            snapshot = row(columnsDefConfig.snapshotColumnName).getB.array(),
            created = row(columnsDefConfig.createdColumnName).getN.toLong
          )
        }
      }.map { rows =>
        new BatchWriteItemRequest()
          .withRequestItems(
            Map(
              tableName -> rows.map { row =>
                val pkey = partitionKeyResolver.resolve(row.persistenceId, row.sequenceNumber)
                val skey = sortKeyResolver.resolve(row.persistenceId, row.sequenceNumber)
                new WriteRequest()
                  .withDeleteRequest(
                    new DeleteRequest()
                      .withKey(
                        Map(
                          columnsDefConfig.partitionKeyColumnName -> new AttributeValue()
                            .withS(pkey.asString),
                          columnsDefConfig.sortKeyColumnName -> new AttributeValue()
                            .withS(skey.asString)
                        ).asJava
                      )
                  )
              }.asJava
            ).asJava
          )
      }.via(streamWriteClient.batchWriteItemFlow).flatMapConcat { response =>
        if (response.getSdkHttpMetadata.getHttpStatusCode == 200)
          Source.single(())
        else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }
  }

  private def deserialize(rowOpt: Option[Map[String, AttributeValue]])(implicit ec: ExecutionContext) = {
    rowOpt match {
      case Some(row) =>
        serializer
          .deserialize(
            SnapshotRow(
              persistenceId = PersistenceId(row(columnsDefConfig.persistenceIdColumnName).getS),
              sequenceNumber = SequenceNumber(row(columnsDefConfig.sequenceNrColumnName).getN.toLong),
              snapshot = row(columnsDefConfig.snapshotColumnName).getB.array(),
              created = row(columnsDefConfig.createdColumnName).getN.toLong
            )
          ).map(Some(_))
      case None =>
        Future.successful(None)
    }
  }

  override def dispose(): Unit = {
    (asyncClient, syncClient) match {
      case (Some(a), _) => a.shutdown()
      case (_, Some(s)) => s.shutdown()
      case _            =>
    }
  }

}
