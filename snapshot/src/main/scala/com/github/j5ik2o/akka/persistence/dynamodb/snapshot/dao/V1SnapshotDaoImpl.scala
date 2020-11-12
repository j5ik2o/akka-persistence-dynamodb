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

import java.io.IOException
import java.nio.ByteBuffer

import akka.NotUsed
import akka.actor.ActorSystem
import akka.persistence.SnapshotMetadata
import akka.serialization.Serialization
import akka.stream.scaladsl.Source
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v1.{ StreamReadClient, StreamWriteClient }
import com.github.j5ik2o.akka.persistence.dynamodb.config.SnapshotPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class V1SnapshotDaoImpl(
    system: ActorSystem,
    asyncClient: Option[AmazonDynamoDBAsync],
    syncClient: Option[AmazonDynamoDB],
    serialization: Serialization,
    pluginConfig: SnapshotPluginConfig
)(implicit ec: ExecutionContext)
    extends SnapshotDao {
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

  private val serializer = new ByteArraySnapshotSerializer(serialization)

  override def deleteAllSnapshots(persistenceId: PersistenceId): Source[Unit, NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
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
  ): Source[Unit, NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
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

  override def deleteUpToMaxTimestamp(persistenceId: PersistenceId, maxTimestamp: Long): Source[Unit, NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
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
  ): Source[Unit, NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
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

  override def latestSnapshot(persistenceId: PersistenceId): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
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
          Source.single(Option(response.getItems).map(_.asScala).getOrElse(Seq.empty).headOption)
        else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }.map { rows =>
        rows.map { row =>
          val _row = row.asScala
          serializer
            .deserialize(
              SnapshotRow(
                persistenceId = PersistenceId(_row(columnsDefConfig.persistenceIdColumnName).getS),
                sequenceNumber = SequenceNumber(_row(columnsDefConfig.sequenceNrColumnName).getN.toLong),
                snapshot = _row(columnsDefConfig.snapshotColumnName).getB.array(),
                created = _row(columnsDefConfig.createdColumnName).getN.toLong
              )
            ) match {
            case Right(value) =>
              value
            case Left(ex) => throw ex
          }
        }
      }
  }

  override def snapshotForMaxTimestamp(
      persistenceId: PersistenceId,
      maxTimestamp: Long
  ): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
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
          Source.single(Option(response.getItems).map(_.asScala).getOrElse(Seq.empty).headOption)
        else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }.map { rows =>
        rows.map { row =>
          val _row = row.asScala
          serializer
            .deserialize(
              SnapshotRow(
                persistenceId = PersistenceId(_row(columnsDefConfig.persistenceIdColumnName).getS),
                sequenceNumber = SequenceNumber(_row(columnsDefConfig.sequenceNrColumnName).getN.toLong),
                snapshot = _row(columnsDefConfig.snapshotColumnName).getB.array(),
                created = _row(columnsDefConfig.createdColumnName).getN.toLong
              )
            ) match {
            case Right(value) =>
              value
            case Left(ex) => throw ex
          }
        }
      }
  }

  override def snapshotForMaxSequenceNr(
      persistenceId: PersistenceId,
      maxSequenceNr: SequenceNumber
  ): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
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
          Source.single(Option(response.getItems).map(_.asScala).getOrElse(Seq.empty).headOption)
        else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }.map { rows =>
        rows.map { row =>
          val _row = row.asScala
          serializer
            .deserialize(
              SnapshotRow(
                persistenceId = PersistenceId(_row(columnsDefConfig.persistenceIdColumnName).getS),
                sequenceNumber = SequenceNumber(_row(columnsDefConfig.sequenceNrColumnName).getN.toLong),
                snapshot = _row(columnsDefConfig.snapshotColumnName).getB.array(),
                created = _row(columnsDefConfig.createdColumnName).getN.toLong
              )
            ) match {
            case Right(value) =>
              value
            case Left(ex) => throw ex
          }
        }
      }
  }

  override def snapshotForMaxSequenceNrAndMaxTimestamp(
      persistenceId: PersistenceId,
      maxSequenceNr: SequenceNumber,
      maxTimestamp: Long
  ): Source[Option[(SnapshotMetadata, Any)], NotUsed] = {
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
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
          Source.single(Option(response.getItems).map(_.asScala).getOrElse(Seq.empty).headOption)
        else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }.map { rows =>
        rows.map { row =>
          val _row = row.asScala
          serializer
            .deserialize(
              SnapshotRow(
                persistenceId = PersistenceId(_row(columnsDefConfig.persistenceIdColumnName).getS),
                sequenceNumber = SequenceNumber(_row(columnsDefConfig.sequenceNrColumnName).getN.toLong),
                snapshot = _row(columnsDefConfig.snapshotColumnName).getB.array(),
                created = _row(columnsDefConfig.createdColumnName).getN.toLong
              )
            ) match {
            case Right(value) =>
              value
            case Left(ex) => throw ex
          }
        }
      }
  }

  override def delete(persistenceId: PersistenceId, sequenceNr: SequenceNumber): Source[Unit, NotUsed] = {
    val req = new DeleteItemRequest()
      .withTableName(tableName).withKey(
        Map(
          columnsDefConfig.persistenceIdColumnName -> new AttributeValue().withS(persistenceId.asString),
          columnsDefConfig.sequenceNrColumnName    -> new AttributeValue().withN(sequenceNr.asString)
        ).asJava
      )
    Source.single(req).via(streamWriteClient.deleteItemFlow).flatMapConcat { response =>
      if (response.getSdkHttpMetadata.getHttpStatusCode == 200)
        Source.single(())
      else {
        val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
        Source.failed(new IOException(s"statusCode: $statusCode"))
      }
    }
  }

  override def save(snapshotMetadata: SnapshotMetadata, snapshot: Any): Source[Unit, NotUsed] = {
    serializer
      .serialize(snapshotMetadata, snapshot) match {
      case Right(snapshotRow) =>
        val req = new PutItemRequest()
          .withTableName(tableName)
          .withItem(
            Map(
              columnsDefConfig.persistenceIdColumnName -> new AttributeValue()
                .withS(snapshotRow.persistenceId.asString),
              columnsDefConfig.sequenceNrColumnName -> new AttributeValue().withN(snapshotRow.sequenceNumber.asString),
              columnsDefConfig.snapshotColumnName   -> new AttributeValue().withB(ByteBuffer.wrap(snapshotRow.snapshot)),
              columnsDefConfig.createdColumnName    -> new AttributeValue().withN(snapshotRow.created.toString)
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
      case Left(ex) =>
        Source.failed(ex)
    }
  }

  private def queryDelete(queryRequest: QueryRequest): Source[Unit, NotUsed] = {
    Source
      .single(queryRequest).via(streamReadClient.queryFlow).map { v =>
        Option(v.getItems).map(_.asScala).getOrElse(Seq.empty)
      }
      .mapConcat(_.toVector)
      .grouped(clientConfig.batchWriteItemLimit)
      .map { rows =>
        rows.map { row =>
          val _row = row.asScala
          SnapshotRow(
            persistenceId = PersistenceId(_row(columnsDefConfig.persistenceIdColumnName).getS),
            sequenceNumber = SequenceNumber(_row(columnsDefConfig.sequenceNrColumnName).getN.toLong),
            snapshot = _row(columnsDefConfig.snapshotColumnName).getB.array(),
            created = _row(columnsDefConfig.createdColumnName).getN.toLong
          )
        }
      }.map { rows =>
        new BatchWriteItemRequest()
          .withRequestItems(
            Map(
              tableName -> rows.map { row =>
                new WriteRequest()
                  .withDeleteRequest(
                    new DeleteRequest()
                      .withKey(
                        Map(
                          columnsDefConfig.persistenceIdColumnName -> new AttributeValue()
                            .withS(row.persistenceId.asString),
                          columnsDefConfig.sequenceNrColumnName -> new AttributeValue()
                            .withN(row.sequenceNumber.asString)
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

}
