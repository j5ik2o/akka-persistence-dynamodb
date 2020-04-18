package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao

import java.io.IOException

import akka.persistence.PersistentRepr
import akka.stream.scaladsl.{ Concat, Sink, Source, SourceUtils }
import akka.stream.{ Attributes, Materializer }
import akka.{ actor, NotUsed }
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalColumnsDefConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.DaoSupport.{
  Continue,
  ContinueDelayed,
  FlowControl,
  Stop
}
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.FlowPersistentReprSerializer
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.github.j5ik2o.reactive.aws.dynamodb.implicits._
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, QueryRequest }

import scala.collection.immutable._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object DaoSupport {
  private sealed trait FlowControl

  /** Keep querying - used when we are sure that there is more events to fetch */
  private case object Continue extends FlowControl

  /**
    * Keep querying with delay - used when we have consumed all events,
    * but want to poll for future events
    */
  private case object ContinueDelayed extends FlowControl

  /** Stop querying - used when we reach the desired offset  */
  private case object Stop extends FlowControl
}

trait DaoSupport {
  protected val streamClient: DynamoDbAkkaClient
  protected val shardCount: Int
  protected val tableName: String
  protected val getJournalRowsIndexName: String
  protected val columnsDefConfig: JournalColumnsDefConfig
  protected val queryBatchSize: Int
  protected val scanBatchSize: Int
  protected val consistentRead: Boolean

  protected val serializer: FlowPersistentReprSerializer[JournalRow]

  protected val metricsReporter: MetricsReporter

  private val logger = LoggerFactory.getLogger(getClass)

  implicit val ec: ExecutionContext
  implicit val mat: Materializer

  protected val logLevels: Attributes = Attributes.logLevels(
    onElement = Attributes.LogLevels.Debug,
    onFailure = Attributes.LogLevels.Error,
    onFinish = Attributes.LogLevels.Debug
  )

  protected val startTimeSource: Source[Long, NotUsed] =
    SourceUtils
      .lazySource(() => Source.single(System.nanoTime())).mapMaterializedValue(_ => NotUsed)

  def getMessages(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      max: Long,
      deleted: Option[Boolean] = Some(false)
  ): Source[Try[PersistentRepr], NotUsed] = {
    getMessagesAsJournalRow(persistenceId, fromSequenceNr, toSequenceNr, max, deleted)
      .via(serializer.deserializeFlowWithoutTagsAsTry)
  }

  def getMessagesAsJournalRow(
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
      startTimeSource
        .flatMapConcat { itemStart =>
          // logger.debug(s"index = $index, count = $count")
          // logger.debug(s"query-batch-size = $queryBatchSize")
          val queryRequest =
            if (shardCount == 1)
              createNonGSIRequest(
                lastEvaluatedKey,
                queryBatchSize,
                persistenceId,
                fromSequenceNr,
                toSequenceNr,
                deleted
              )
            else
              createGSIRequest(lastEvaluatedKey, queryBatchSize, persistenceId, fromSequenceNr, toSequenceNr, deleted)
          Source
            .single(queryRequest).via(streamClient.queryFlow(1)).flatMapConcat { response =>
              metricsReporter.setGetMessagesItemDuration(System.nanoTime() - itemStart)
              if (response.sdkHttpResponse().isSuccessful) {
                metricsReporter.incrementGetMessagesItemCallCounter()
                if (response.count() > 0)
                  metricsReporter.addGetMessagesItemCounter(response.count().toLong)
                val items            = response.itemsAsScala.getOrElse(Seq.empty).toVector
                val lastEvaluatedKey = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
                val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
                if (lastEvaluatedKey.nonEmpty && (count + response.count()) < max) {
                  //logger.debug(s"index = $index, next loop")
                  loop(lastEvaluatedKey, combinedSource, count + response.count(), index + 1)
                } else
                  combinedSource
              } else {
                metricsReporter.incrementGetMessagesItemCallErrorCounter()
                val statusCode = response.sdkHttpResponse().statusCode()
                val statusText = response.sdkHttpResponse().statusText()
                logger.debug(s"getMessages(max = $max): finished")
                Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
              }
            }
        }
    }
    startTimeSource.flatMapConcat { callStart =>
      logger.debug(s"getMessages(max = $max): start")
      if (max == 0L || fromSequenceNr > toSequenceNr)
        Source.empty
      else {
        loop(None, Source.empty, 0L, 1)
          .map(convertToJournalRow)
          .take(max)
          .withAttributes(logLevels)
      }
    }
  }

  def getMessagesWithBatch(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      batchSize: Int,
      refreshInterval: Option[(FiniteDuration, actor.Scheduler)]
  ): Source[Try[PersistentRepr], NotUsed] = {
    Source
      .unfoldAsync[(Long, FlowControl), Seq[Try[PersistentRepr]]]((Math.max(1, fromSequenceNr), Continue)) {
        case (from, control) =>
          def retrieveNextBatch(): Future[Option[((Long, FlowControl), Seq[Try[PersistentRepr]])]] = {
            for {
              xs <- getMessages(
                PersistenceId(persistenceId),
                SequenceNumber(from),
                SequenceNumber(toSequenceNr),
                batchSize
              ).runWith(Sink.seq)
            } yield {
              val hasMoreEvents = xs.size == batchSize
              // Events are ordered by sequence number, therefore the last one is the largest)
              val lastSeqNrInBatch: Option[Long] = xs.lastOption match {
                case Some(Success(repr)) => Some(repr.sequenceNr)
                case Some(Failure(e))    => throw e // fail the returned Future
                case None                => None
              }
              val hasLastEvent = lastSeqNrInBatch.exists(_ >= toSequenceNr)
              val nextControl: FlowControl =
                if (hasLastEvent || from > toSequenceNr) Stop
                else if (hasMoreEvents) Continue
                else if (refreshInterval.isEmpty) Stop
                else ContinueDelayed

              val nextFrom: Long = lastSeqNrInBatch match {
                // Continue querying from the last sequence number (the events are ordered)
                case Some(lastSeqNr) => lastSeqNr + 1
                case None            => from
              }
              Some((nextFrom, nextControl), xs)
            }
          }

          control match {
            case Stop     => Future.successful(None)
            case Continue => retrieveNextBatch()
            case ContinueDelayed =>
              val (delay, scheduler) = refreshInterval.get
              akka.pattern.after(delay, scheduler)(retrieveNextBatch())
          }
      }
      .mapConcat(identity)

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

  private def createNonGSIRequest(
      lastEvaluatedKey: Option[Map[String, AttributeValue]],
      limit: Int,
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      deleted: Option[Boolean]
  ): QueryRequest = {
    QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpression(
        "#pid = :pid and #snr between :min and :max"
      )
      .filterExpressionAsScala(deleted.map { _ => s"#flg = :flg" })
      .expressionAttributeNamesAsScala(
        Map(
          "#pid" -> columnsDefConfig.partitionKeyColumnName,
          "#snr" -> columnsDefConfig.sequenceNrColumnName
        ) ++ deleted.map(_ => Map("#flg" -> columnsDefConfig.deletedColumnName)).getOrElse(Map.empty)
      ).expressionAttributeValuesAsScala(
        Map(
          ":pid" -> AttributeValue.builder().s(persistenceId.asString + "-0").build(),
          ":min" -> AttributeValue.builder().n(fromSequenceNr.asString).build(),
          ":max" -> AttributeValue.builder().n(toSequenceNr.asString).build()
        ) ++ deleted.map(b => Map(":flg" -> AttributeValue.builder().bool(b).build())).getOrElse(Map.empty)
      )
      .limit(limit)
      .exclusiveStartKeyAsScala(lastEvaluatedKey)
      .build()

  }

  private def createGSIRequest(
      lastEvaluatedKey: Option[Map[String, AttributeValue]],
      limit: Int,
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      deleted: Option[Boolean]
  ): QueryRequest = {
    QueryRequest
      .builder()
      .tableName(tableName)
      .indexName(getJournalRowsIndexName)
      .keyConditionExpression(
        "#pid = :pid and #snr between :min and :max"
      )
      .filterExpressionAsScala(deleted.map { _ => s"#flg = :flg" })
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
      .exclusiveStartKeyAsScala(lastEvaluatedKey)
      .consistentRead(consistentRead)
      .build()

  }

}
