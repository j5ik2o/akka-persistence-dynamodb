package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v2

import java.io.IOException

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.FlowShape
import akka.stream.scaladsl.{ Concat, Flow, GraphDSL, RestartFlow, Source, Unzip, Zip }
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginBaseConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.JournalRowReadDriver
import com.github.j5ik2o.akka.persistence.dynamodb.journal.JournalRow
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, Stopwatch }
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.github.j5ik2o.reactive.aws.dynamodb.implicits._
import com.github.j5ik2o.reactive.aws.dynamodb.{ DynamoDbAsyncClient, DynamoDbSyncClient }
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, QueryRequest, QueryResponse }

import scala.collection.mutable.ArrayBuffer

final class V2JournalRowReadDriver(
    val system: ActorSystem,
    val asyncClient: Option[DynamoDbAsyncClient],
    val syncClient: Option[DynamoDbSyncClient],
    val pluginConfig: JournalPluginBaseConfig,
    val metricsReporter: Option[MetricsReporter]
) extends JournalRowReadDriver {
  (asyncClient, syncClient) match {
    case (None, None) =>
      throw new IllegalArgumentException("aws clients is both None")
    case _ =>
  }

  val streamClient: Option[DynamoDbAkkaClient] = asyncClient.map { c => DynamoDbAkkaClient(c) }

  private val logger = LoggerFactory.getLogger(getClass)

  private def queryFlow: Flow[QueryRequest, QueryResponse, NotUsed] = {
    val flow = ((streamClient, syncClient) match {
      case (Some(c), None) =>
        c.queryFlow(1)
      case (None, Some(c)) =>
        val flow = Flow[QueryRequest].map { request =>
          c.query(request) match {
            case Right(r) => r
            case Left(ex) => throw ex
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

  override def getJournalRows(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber,
      deleted: Boolean
  ): Source[Seq[JournalRow], NotUsed] = {
    def loop(
        lastEvaluatedKey: Option[Map[String, AttributeValue]],
        acc: Source[Map[String, AttributeValue], NotUsed],
        count: Long,
        index: Int
    ): Source[Map[String, AttributeValue], NotUsed] = {
      val queryRequest = createGSIRequest(persistenceId, toSequenceNr, deleted, lastEvaluatedKey)
      Source
        .single(queryRequest).via(queryFlow).flatMapConcat { response =>
          if (response.sdkHttpResponse().isSuccessful) {
            val items            = response.itemsAsScala.getOrElse(Seq.empty).toVector
            val lastEvaluatedKey = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
            val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
            if (lastEvaluatedKey.nonEmpty) {
              loop(lastEvaluatedKey, combinedSource, count + response.count(), index + 1)
            } else
              combinedSource
          } else {
            val statusCode = response.sdkHttpResponse().statusCode()
            val statusText = response.sdkHttpResponse().statusText()
            Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
          }
        }
    }
    loop(None, Source.empty, 0, 1)
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
    def loop(
        lastEvaluatedKey: Option[Map[String, AttributeValue]],
        acc: Source[Map[String, AttributeValue], NotUsed],
        count: Long,
        index: Int
    ): Source[Map[String, AttributeValue], NotUsed] = {
      val queryRequest =
        createGSIRequest(
          persistenceId,
          fromSequenceNr,
          toSequenceNr,
          deleted,
          pluginConfig.queryBatchSize,
          lastEvaluatedKey
        )
      Source
        .single(queryRequest).via(queryFlow).flatMapConcat { response =>
          if (response.sdkHttpResponse().isSuccessful) {
            val items            = response.itemsAsScala.getOrElse(Seq.empty).toVector
            val lastEvaluatedKey = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
            val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
            if (lastEvaluatedKey.nonEmpty && (count + response.count()) < max) {
              logger.debug("next loop: count = {}, response.count = {}", count, response.count())
              loop(lastEvaluatedKey, combinedSource, count + response.count(), index + 1)
            } else
              combinedSource
          } else {
            val statusCode = response.sdkHttpResponse().statusCode()
            val statusText = response.sdkHttpResponse().statusText()
            Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
          }
        }
    }
    if (max == 0L || fromSequenceNr > toSequenceNr)
      Source.empty
    else {
      loop(None, Source.empty, 0L, 1)
        .map(convertToJournalRow)
        .take(max)
        .withAttributes(logLevels)
    }
  }

  private def createGSIRequest(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber,
      deleted: Boolean,
      lastEvaluatedKey: Option[Map[String, AttributeValue]]
  ): QueryRequest = {
    QueryRequest
      .builder()
      .tableName(pluginConfig.tableName)
      .indexName(pluginConfig.getJournalRowsIndexName)
      .keyConditionExpression("#pid = :pid and #snr <= :snr")
      .filterExpression("#d = :flg")
      .expressionAttributeNamesAsScala(
        Map(
          "#pid" -> pluginConfig.columnsDefConfig.persistenceIdColumnName,
          "#snr" -> pluginConfig.columnsDefConfig.sequenceNrColumnName,
          "#d"   -> pluginConfig.columnsDefConfig.deletedColumnName
        )
      )
      .expressionAttributeValuesAsScala(
        Some(
          Map(
            ":pid" -> AttributeValue.builder().s(persistenceId.asString).build(),
            ":snr" -> AttributeValue.builder().n(toSequenceNr.asString).build(),
            ":flg" -> AttributeValue.builder().bool(deleted).build()
          )
        )
      )
      .limit(pluginConfig.queryBatchSize)
      .exclusiveStartKeyAsScala(lastEvaluatedKey)
      .build()
  }

  def highestSequenceNr(
      persistenceId: PersistenceId,
      fromSequenceNr: Option[SequenceNumber] = None,
      deleted: Option[Boolean] = None
  ): Source[Long, NotUsed] = {
    val queryRequest = createHighestSequenceNrRequest(persistenceId, fromSequenceNr, deleted)
    Source
      .single(queryRequest)
      .via(queryFlow)
      .flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful) {
          val result = response.itemsAsScala
            .getOrElse(Seq.empty).toVector.headOption.map { head =>
              head(pluginConfig.columnsDefConfig.sequenceNrColumnName).n().toLong
            }.getOrElse(0L)
          Source.single(result)
        } else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
        }
      }.withAttributes(logLevels)
  }

  private def convertToJournalRow(map: Map[String, AttributeValue]): JournalRow = {
    JournalRow(
      persistenceId = PersistenceId(map(pluginConfig.columnsDefConfig.persistenceIdColumnName).s),
      sequenceNumber = SequenceNumber(map(pluginConfig.columnsDefConfig.sequenceNrColumnName).n.toLong),
      deleted = map(pluginConfig.columnsDefConfig.deletedColumnName).bool.get,
      message = map.get(pluginConfig.columnsDefConfig.messageColumnName).map(_.b.asByteArray()).get,
      ordering = map(pluginConfig.columnsDefConfig.orderingColumnName).n.toLong,
      tags = map.get(pluginConfig.columnsDefConfig.tagsColumnName).map(_.s)
    )
  }

  private def createGSIRequest(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      deleted: Option[Boolean],
      limit: Int,
      lastEvaluatedKey: Option[Map[String, AttributeValue]]
  ): QueryRequest = {
    QueryRequest
      .builder()
      .tableName(pluginConfig.tableName)
      .indexName(pluginConfig.getJournalRowsIndexName)
      .keyConditionExpression(
        "#pid = :pid and #snr between :min and :max"
      )
      .filterExpressionAsScala(deleted.map { _ => s"#flg = :flg" })
      .expressionAttributeNamesAsScala(
        Map(
          "#pid" -> pluginConfig.columnsDefConfig.persistenceIdColumnName,
          "#snr" -> pluginConfig.columnsDefConfig.sequenceNrColumnName
        ) ++ deleted
          .map(_ => Map("#flg" -> pluginConfig.columnsDefConfig.deletedColumnName)).getOrElse(Map.empty)
      ).expressionAttributeValuesAsScala(
        Map(
          ":pid" -> AttributeValue.builder().s(persistenceId.asString).build(),
          ":min" -> AttributeValue.builder().n(fromSequenceNr.asString).build(),
          ":max" -> AttributeValue.builder().n(toSequenceNr.asString).build()
        ) ++ deleted.map(b => Map(":flg" -> AttributeValue.builder().bool(b).build())).getOrElse(Map.empty)
      )
      .limit(limit)
      .exclusiveStartKeyAsScala(lastEvaluatedKey)
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
      .keyConditionExpressionAsScala(
        fromSequenceNr.map(_ => "#pid = :id and #snr >= :nr").orElse(Some("#pid = :id"))
      )
      .filterExpressionAsScala(deleted.map(_ => "#d = :flg"))
      .expressionAttributeNamesAsScala(
        Map(
          "#pid" -> pluginConfig.columnsDefConfig.persistenceIdColumnName
        ) ++ deleted
          .map(_ => Map("#d" -> pluginConfig.columnsDefConfig.deletedColumnName)).getOrElse(Map.empty) ++
        fromSequenceNr
          .map(_ => Map("#snr" -> pluginConfig.columnsDefConfig.sequenceNrColumnName)).getOrElse(Map.empty)
      )
      .expressionAttributeValuesAsScala(
        Map(
          ":id" -> AttributeValue.builder().s(persistenceId.asString).build()
        ) ++ deleted
          .map(d => Map(":flg" -> AttributeValue.builder().bool(d).build())).getOrElse(Map.empty) ++ fromSequenceNr
          .map(nr => Map(":nr" -> AttributeValue.builder().n(nr.asString).build())).getOrElse(Map.empty)
      ).scanIndexForward(false)
      .limit(1)
      .build()
  }

}
