package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v2

import java.io.IOException

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.github.j5ik2o.akka.persistence.dynamodb.client.v2.StreamReadClient
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginBaseConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.JournalRow
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.JournalRowReadDriver
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, QueryRequest }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbClient }

import scala.collection.mutable.ArrayBuffer
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

final class V2JournalRowReadDriver(
    val system: ActorSystem,
    val asyncClient: Option[DynamoDbAsyncClient],
    val syncClient: Option[DynamoDbClient],
    val pluginConfig: JournalPluginBaseConfig,
    val metricsReporter: Option[MetricsReporter]
) extends JournalRowReadDriver {
  (asyncClient, syncClient) match {
    case (None, None) =>
      throw new IllegalArgumentException("aws clients is both None")
    case _ =>
  }

  LoggerFactory.getLogger(getClass)

  private val streamClient =
    new StreamReadClient(system, asyncClient, syncClient, pluginConfig, pluginConfig.readBackoffConfig)

  override def getJournalRows(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber,
      deleted: Boolean
  ): Source[Seq[JournalRow], NotUsed] = {
    val queryRequest = createGSIRequest(persistenceId, toSequenceNr, deleted)
    streamClient
      .recursiveQuerySource(queryRequest, None)
      .map(convertToJournalRow)
      .fold(ArrayBuffer.empty[JournalRow])(_ += _)
      .map(_.toList)
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
        .map(convertToJournalRow)
        .take(max)
        .withAttributes(logLevels)
    }
  }

  def highestSequenceNr(
      persistenceId: PersistenceId,
      fromSequenceNr: Option[SequenceNumber] = None,
      deleted: Option[Boolean] = None
  ): Source[Long, NotUsed] = {
    val queryRequest = createHighestSequenceNrRequest(persistenceId, fromSequenceNr, deleted)
    Source
      .single(queryRequest)
      .via(streamClient.queryFlow)
      .flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful) {
          val result = Option(response.items)
            .map(_.asScala)
            .map(_.map(_.asScala.toMap))
            .getOrElse(Seq.empty).toVector.headOption.map { head =>
              head(pluginConfig.columnsDefConfig.sequenceNrColumnName).n().toLong
            }.getOrElse(0L)
          Source.single(result)
        } else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
        }
      }.withAttributes(logLevels)
  }

  private def convertToJournalRow(map: Map[String, AttributeValue]): JournalRow = {
    JournalRow(
      persistenceId = PersistenceId(map(pluginConfig.columnsDefConfig.persistenceIdColumnName).s),
      sequenceNumber = SequenceNumber(map(pluginConfig.columnsDefConfig.sequenceNrColumnName).n.toLong),
      deleted = map(pluginConfig.columnsDefConfig.deletedColumnName).bool,
      message = map.get(pluginConfig.columnsDefConfig.messageColumnName).map(_.b.asByteArray()).get,
      ordering = map(pluginConfig.columnsDefConfig.orderingColumnName).n.toLong,
      tags = map.get(pluginConfig.columnsDefConfig.tagsColumnName).map(_.s)
    )
  }

  private def createGSIRequest(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber,
      deleted: Boolean
  ): QueryRequest = {
    QueryRequest
      .builder()
      .tableName(pluginConfig.tableName)
      .indexName(pluginConfig.getJournalRowsIndexName)
      .keyConditionExpression("#pid = :pid and #snr <= :snr")
      .filterExpression("#d = :flg")
      .expressionAttributeNames(
        Map(
          "#pid" -> pluginConfig.columnsDefConfig.persistenceIdColumnName,
          "#snr" -> pluginConfig.columnsDefConfig.sequenceNrColumnName,
          "#d"   -> pluginConfig.columnsDefConfig.deletedColumnName
        ).asJava
      )
      .expressionAttributeValues(
        Map(
          ":pid" -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":snr" -> AttributeValue.builder().n(toSequenceNr.asString).build(),
          ":flg" -> AttributeValue.builder().bool(deleted).build()
        ).asJava
      )
      .limit(pluginConfig.queryBatchSize)
      .build()
  }

  private def createGSIRequest(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      deleted: Option[Boolean],
      limit: Int
  ): QueryRequest = {
    QueryRequest
      .builder()
      .tableName(pluginConfig.tableName)
      .indexName(pluginConfig.getJournalRowsIndexName)
      .keyConditionExpression(
        "#pid = :pid and #snr between :min and :max"
      )
      .filterExpression(deleted.map { _ => s"#flg = :flg" }.orNull)
      .expressionAttributeNames(
        (Map(
          "#pid" -> pluginConfig.columnsDefConfig.persistenceIdColumnName,
          "#snr" -> pluginConfig.columnsDefConfig.sequenceNrColumnName
        ) ++ deleted
          .map(_ => Map("#flg" -> pluginConfig.columnsDefConfig.deletedColumnName)).getOrElse(Map.empty)).asJava
      ).expressionAttributeValues(
        (Map(
          ":pid" -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min" -> AttributeValue.builder().n(fromSequenceNr.asString).build(),
          ":max" -> AttributeValue.builder().n(toSequenceNr.asString).build()
        ) ++ deleted.map(b => Map(":flg" -> AttributeValue.builder().bool(b).build())).getOrElse(Map.empty)).asJava
      )
      .limit(limit)
      .build()
  }

  private def createHighestSequenceNrRequest(
      persistenceId: PersistenceId,
      fromSequenceNr: Option[SequenceNumber] = None,
      deleted: Option[Boolean] = None
  ): QueryRequest = {
    QueryRequest
      .builder()
      .tableName(pluginConfig.tableName)
      .indexName(pluginConfig.getJournalRowsIndexName)
      .keyConditionExpression(
        fromSequenceNr.map(_ => "#pid = :id and #snr >= :nr").orElse(Some("#pid = :id")).orNull
      )
      .filterExpression(deleted.map(_ => "#d = :flg").orNull)
      .expressionAttributeNames(
        (Map(
          "#pid" -> pluginConfig.columnsDefConfig.persistenceIdColumnName
        ) ++ deleted
          .map(_ => Map("#d" -> pluginConfig.columnsDefConfig.deletedColumnName)).getOrElse(Map.empty) ++
        fromSequenceNr
          .map(_ => Map("#snr" -> pluginConfig.columnsDefConfig.sequenceNrColumnName)).getOrElse(Map.empty)).asJava
      )
      .expressionAttributeValues(
        (Map(
          ":id" -> AttributeValue.builder().s(persistenceId.asString).build()
        ) ++ deleted
          .map(d => Map(":flg" -> AttributeValue.builder().bool(d).build())).getOrElse(Map.empty) ++ fromSequenceNr
          .map(nr => Map(":nr" -> AttributeValue.builder().n(nr.asString).build())).getOrElse(Map.empty)).asJava
      ).scanIndexForward(false)
      .limit(1)
      .build()
  }

}
