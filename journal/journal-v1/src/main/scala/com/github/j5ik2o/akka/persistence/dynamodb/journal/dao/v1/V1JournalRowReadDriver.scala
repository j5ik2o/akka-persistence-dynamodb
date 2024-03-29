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
package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v1

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue, QueryRequest }
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v1.StreamReadClient
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.JournalRowReadDriver
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalPluginContext, JournalRow }
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }

import java.io.IOException
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

final class V1JournalRowReadDriver(
    val pluginContext: JournalPluginContext,
    val asyncClient: Option[AmazonDynamoDBAsync],
    val syncClient: Option[AmazonDynamoDB]
) extends JournalRowReadDriver {

  override def system: ActorSystem = pluginContext.system

  import pluginContext._

  (asyncClient, syncClient) match {
    case (None, None) =>
      throw new IllegalArgumentException("aws clients is both None")
    case _ =>
  }

  override def dispose(): Unit = {
    (asyncClient, syncClient) match {
      case (Some(a), _) => a.shutdown()
      case (_, Some(s)) => s.shutdown()
      case _            =>
    }
  }

  private val streamClient =
    new StreamReadClient(pluginContext, asyncClient, syncClient, pluginConfig.readBackoffConfig)

  override def getJournalRows(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber,
      deleted: Boolean
  ): Source[Seq[JournalRow], NotUsed] = {
    val queryRequest = createGSIRequest(persistenceId, toSequenceNr, deleted)
    streamClient
      .recursiveQuerySource(queryRequest, None)
      .mapConcat { response =>
        Option(response.getItems).map(_.asScala.map(_.asScala.toMap).toVector).getOrElse(Vector.empty)
      }
      .map(convertToJournalRow)
      .fold(ArrayBuffer.empty[JournalRow])(_ += _)
      .map(_.toVector)
      .withAttributes(logLevels)
  }

  override def getJournalRows(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      max: Long,
      deleted: Option[Boolean] = Some(false)
  ): Source[JournalRow, NotUsed] = {
    if (max == 0L || fromSequenceNr > toSequenceNr)
      Source.empty
    else {
      val queryRequest = createGSIRequest(
        persistenceId,
        fromSequenceNr,
        toSequenceNr,
        deleted,
        pluginConfig.queryBatchSize
      )
      streamClient
        .recursiveQuerySource(queryRequest, Some(max))
        .mapConcat { response =>
          Option(response.getItems).map(_.asScala.map(_.asScala.toMap).toVector).getOrElse(Vector.empty)
        }
        .map(convertToJournalRow)
        .take(max)
        .withAttributes(logLevels)
    }
  }

  override def highestSequenceNr(
      persistenceId: PersistenceId,
      fromSequenceNr: Option[SequenceNumber],
      deleted: Option[Boolean]
  ): Source[Option[Long], NotUsed] = {
    val queryRequest = createHighestSequenceNrRequest(persistenceId, fromSequenceNr, deleted)
    Source
      .single(queryRequest)
      .via(streamClient.queryFlow)
      .flatMapConcat { response =>
        if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
          val result = Option(response.getItems)
            .map(_.asScala).map(_.map(_.asScala))
            .getOrElse(Seq.empty).toVector.headOption.map { head =>
              head(pluginConfig.columnsDefConfig.sequenceNrColumnName).getN.toLong
            }
          Source.single(result)
        } else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }
  }.withAttributes(logLevels)

  private def createHighestSequenceNrRequest(
      persistenceId: PersistenceId,
      fromSequenceNr: Option[SequenceNumber] = None,
      deleted: Option[Boolean] = None
  ): QueryRequest = {
    val limit = deleted.map(_ => Int.MaxValue).getOrElse(1)
    new QueryRequest()
      .withTableName(pluginConfig.tableName)
      .withIndexName(pluginConfig.getJournalRowsIndexName)
      .withKeyConditionExpression(
        fromSequenceNr.map(_ => "#pid = :id and #snr >= :nr").orElse(Some("#pid = :id")).orNull
      )
      .withFilterExpression(deleted.map(_ => "#d = :flg").orNull)
      .withProjectionExpression((Seq("#snr") ++ deleted.map(_ => "#d")).mkString(","))
      .withExpressionAttributeNames(
        (Map(
          "#pid" -> pluginConfig.columnsDefConfig.persistenceIdColumnName,
          "#snr" -> pluginConfig.columnsDefConfig.sequenceNrColumnName
        ) ++ deleted
          .map(_ => Map("#d" -> pluginConfig.columnsDefConfig.deletedColumnName)).getOrElse(Map.empty)).asJava
      )
      .withExpressionAttributeValues(
        (Map(
          ":id" -> new AttributeValue().withS(persistenceId.asString)
        ) ++ deleted
          .map(d => Map(":flg" -> new AttributeValue().withBOOL(d))).getOrElse(Map.empty) ++ fromSequenceNr
          .map(nr => Map(":nr" -> new AttributeValue().withN(nr.asString))).getOrElse(Map.empty)).asJava
      ).withScanIndexForward(false)
      .withLimit(limit)
      .withConsistentRead(pluginConfig.consistentRead)
  }

  private def createGSIRequest(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber,
      deleted: Boolean
  ): QueryRequest = {
    new QueryRequest()
      .withTableName(pluginConfig.tableName)
      .withIndexName(pluginConfig.getJournalRowsIndexName)
      .withKeyConditionExpression("#pid = :pid and #snr <= :snr")
      .withFilterExpression("#d = :flg")
      .withExpressionAttributeNames(
        Map(
          "#pid" -> pluginConfig.columnsDefConfig.persistenceIdColumnName,
          "#snr" -> pluginConfig.columnsDefConfig.sequenceNrColumnName,
          "#d"   -> pluginConfig.columnsDefConfig.deletedColumnName
        ).asJava
      )
      .withExpressionAttributeValues(
        Map(
          ":pid" -> new AttributeValue().withS(persistenceId.asString),
          ":snr" -> new AttributeValue().withN(toSequenceNr.asString),
          ":flg" -> new AttributeValue().withBOOL(deleted)
        ).asJava
      )
      .withLimit(pluginConfig.queryBatchSize)
      .withConsistentRead(pluginConfig.consistentRead)
  }

  private def createGSIRequest(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      deleted: Option[Boolean],
      limit: Int
  ): QueryRequest = {
    new QueryRequest()
      .withTableName(pluginConfig.tableName).withIndexName(
        pluginConfig.getJournalRowsIndexName
      ).withKeyConditionExpression(
        "#pid = :pid and #snr between :min and :max"
      ).withFilterExpression(deleted.map { _ => s"#flg = :flg" }.orNull)
      .withExpressionAttributeNames(
        (Map(
          "#pid" -> pluginConfig.columnsDefConfig.persistenceIdColumnName,
          "#snr" -> pluginConfig.columnsDefConfig.sequenceNrColumnName
        ) ++ deleted
          .map(_ => Map("#flg" -> pluginConfig.columnsDefConfig.deletedColumnName)).getOrElse(Map.empty)).asJava
      )
      .withExpressionAttributeValues(
        (Map(
          ":pid" -> new AttributeValue().withS(persistenceId.asString),
          ":min" -> new AttributeValue().withN(fromSequenceNr.asString),
          ":max" -> new AttributeValue().withN(toSequenceNr.asString)
        ) ++ deleted.map(b => Map(":flg" -> new AttributeValue().withBOOL(b))).getOrElse(Map.empty)).asJava
      ).withLimit(limit)
      .withConsistentRead(pluginConfig.consistentRead)
  }

  protected def convertToJournalRow(map: Map[String, AttributeValue]): JournalRow = {
    JournalRow(
      persistenceId = PersistenceId(map(pluginConfig.columnsDefConfig.persistenceIdColumnName).getS),
      sequenceNumber = SequenceNumber(map(pluginConfig.columnsDefConfig.sequenceNrColumnName).getN.toLong),
      deleted = map(pluginConfig.columnsDefConfig.deletedColumnName).getBOOL,
      message = map.get(pluginConfig.columnsDefConfig.messageColumnName).map(_.getB.array()).get,
      ordering = map(pluginConfig.columnsDefConfig.orderingColumnName).getN.toLong,
      tags = map.get(pluginConfig.columnsDefConfig.tagsColumnName).map(_.getS)
    )
  }

}
