package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao

import java.io.IOException

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.scaladsl.{ Concat, Source }
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalColumnsDefConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.github.j5ik2o.reactive.aws.dynamodb.implicits._
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, QueryRequest }

trait DaoSupport {
  protected val streamClient: DynamoDbAkkaClient
  protected val tableName: String
  protected val getJournalRowsIndexName: String
  protected val parallelism: Int
  protected val columnsDefConfig: JournalColumnsDefConfig

  protected val metricsReporter: MetricsReporter

  private val logger = LoggerFactory.getLogger(getClass)

  protected val logLevels: Attributes = Attributes.logLevels(
    onElement = Attributes.LogLevels.Debug,
    onFailure = Attributes.LogLevels.Error,
    onFinish = Attributes.LogLevels.Debug
  )

  val startTimeSource: Source[Long, NotUsed] = Source
    .lazily(() => Source.single(System.nanoTime())).mapMaterializedValue(_ => NotUsed)

  // def startTimeSource: Source[Long, NotUsed] = Source.single(System.nanoTime())

  def getMessages(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      max: Long,
      deleted: Option[Boolean] = Some(false)
  ): Source[JournalRow, NotUsed] = {
    startTimeSource.flatMapConcat { callStart =>
      def loop(
          lastKey: Option[Map[String, AttributeValue]],
          acc: Source[Map[String, AttributeValue], NotUsed],
          count: Long,
          index: Int
      ): Source[Map[String, AttributeValue], NotUsed] = {
        startTimeSource
          .flatMapConcat { itemStart =>
            val limit = if ((max - count) > Int.MaxValue.toLong) Int.MaxValue else (max - count).toInt
            logger.debug(s"index = $index, max = $max, count = $count, limit = $limit")
            val queryRequest = QueryRequest
              .builder()
              .tableName(tableName).indexName(getJournalRowsIndexName).keyConditionExpression(
                "#pid = :pid and #snr between :min and :max"
              )
              .filterExpressionAsScala(deleted.map { _ =>
                s"#flg = :flg"
              })
              .expressionAttributeNamesAsScala(
                Map(
                  "#pid" -> columnsDefConfig.persistenceIdColumnName,
                  "#snr" -> columnsDefConfig.sequenceNrColumnName
                ) ++ deleted.map(_ => Map("#flg" -> columnsDefConfig.deletedColumnName)).getOrElse(Map.empty)
              ).expressionAttributeValuesAsScala(
                Map(
                  ":pid" -> AttributeValue.builder().s(persistenceId.asString).build(),
                  ":min" -> AttributeValue.builder().n(fromSequenceNr.asString).build(),
                  ":max" -> AttributeValue.builder().n(toSequenceNr.asString).build()
                ) ++ deleted.map(b => Map(":flg" -> AttributeValue.builder().bool(b).build())).getOrElse(Map.empty)
              )
              .limit(limit)
              .exclusiveStartKeyAsScala(lastKey).build()
            Source.single(queryRequest).via(streamClient.queryFlow(1)).map((_, itemStart))
          }.flatMapConcat {
            case (response, itemStart) =>
              metricsReporter.setGetMessagesItemDuration(System.nanoTime() - itemStart)
              if (response.sdkHttpResponse().isSuccessful) {
                metricsReporter.incrementGetMessagesItemCallCounter()
                if (response.count() > 0)
                  metricsReporter.addGetMessagesItemCounter(response.count().toLong)
                val last  = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
                val items = response.itemsAsScala.getOrElse(Seq.empty).toVector
                logger.debug(
                  s"index = $index, item.size = ${items.size}, max = $max, count = $count, (max - count) = ${max - count}"
                )
                val combinedSource = Source.combine(acc, Source(items))(Concat(_))
                if (response.count() > 0 && last.nonEmpty && (count + response.count()) < max) {
                  logger.debug(s"index = $index, next loop")
                  loop(response.lastEvaluatedKeyAsScala, combinedSource, count + response.count(), index + 1)
                } else
                  combinedSource
              } else {
                metricsReporter.incrementGetMessagesItemCallErrorCounter()
                val statusCode = response.sdkHttpResponse().statusCode()
                val statusText = response.sdkHttpResponse().statusText()
                Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
              }
          }
      }
      if (max == 0L || fromSequenceNr > toSequenceNr)
        Source.empty
      else
        loop(None, Source.empty, 0L, 1)
          .map(convertToJournalRow)
          .withAttributes(logLevels)
          .map { response =>
            metricsReporter.setGetMessagesCallDuration(System.nanoTime() - callStart)
            metricsReporter.incrementGetMessagesCallCounter()
            response
          }.recoverWithRetries(
            1, {
              case t: Throwable =>
                metricsReporter.setGetMessagesCallDuration(System.nanoTime() - callStart)
                metricsReporter.incrementGetMessagesCallErrorCounter()
                Source.failed(t)
            }
          )
    }
  }

  protected def convertToJournalRow(map: Map[String, AttributeValue]): JournalRow = {
    JournalRow(
      persistenceId = PersistenceId(map(columnsDefConfig.persistenceIdColumnName).s),
      sequenceNumber = SequenceNumber(map(columnsDefConfig.sequenceNrColumnName).n.toLong),
      deleted = map(columnsDefConfig.deletedColumnName).bool.get,
      message = map.get(columnsDefConfig.messageColumnName).map(_.b.asByteArray()).get,
      ordering = map(columnsDefConfig.orderingColumnName).n.toLong,
      tags = map.get(columnsDefConfig.tagsColumnName).map(_.s)
    )
  }

}
