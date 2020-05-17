package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v2

import java.io.IOException

import akka.NotUsed
import akka.stream.scaladsl.{ Concat, Flow, RestartFlow, Source, SourceUtils }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalPluginConfig, PluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal._
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.{
  JournalRowReadDriver,
  JournalRowWriteDriver,
  PersistenceIdWithSeqNr
}
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.github.j5ik2o.reactive.aws.dynamodb.implicits._
import com.github.j5ik2o.reactive.aws.dynamodb.{ DynamoDbAsyncClient, DynamoDbSyncClient }
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model._

import scala.collection.mutable.ArrayBuffer

final class V2JournalRowReadDriver(
    val asyncClient: Option[DynamoDbAsyncClient],
    val syncClient: Option[DynamoDbSyncClient],
    val pluginConfig: PluginConfig,
    val metricsReporter: MetricsReporter
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
      case (None, Some(c)) =>
        val flow = Flow[QueryRequest].map { request =>
          c.query(request) match {
            case Right(r) => r
            case Left(ex) => throw ex
          }
        }
        DispatcherUtils.applyV2Dispatcher(pluginConfig, flow)
      case (Some(c), None) =>
        c.queryFlow(1)
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
    startTimeSource
      .flatMapConcat { callStart =>
        def loop(
            lastEvaluatedKey: Option[Map[String, AttributeValue]],
            acc: Source[Map[String, AttributeValue], NotUsed],
            count: Long,
            index: Int
        ): Source[Map[String, AttributeValue], NotUsed] =
          startTimeSource
            .flatMapConcat { itemStart =>
              val queryRequest = createGSIRequest(persistenceId, toSequenceNr, deleted, lastEvaluatedKey)
              Source
                .single(queryRequest).via(queryFlow).flatMapConcat { response =>
                  metricsReporter.setGetJournalRowsItemDuration(System.nanoTime() - itemStart)
                  if (response.sdkHttpResponse().isSuccessful) {
                    metricsReporter.incrementGetJournalRowsItemCallCounter()
                    if (response.count() > 0)
                      metricsReporter.addGetJournalRowsItemCounter(response.count().toLong)
                    val items            = response.itemsAsScala.getOrElse(Seq.empty).toVector
                    val lastEvaluatedKey = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
                    val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
                    if (lastEvaluatedKey.nonEmpty) {
                      loop(lastEvaluatedKey, combinedSource, count + response.count(), index + 1)
                    } else
                      combinedSource
                  } else {
                    metricsReporter.incrementGetJournalRowsItemCallErrorCounter()
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
          .map { journalRows =>
            metricsReporter.setGetJournalRowsCallDuration(System.nanoTime() - callStart)
            metricsReporter.incrementGetJournalRowsCallCounter()
            journalRows
          }.recoverWithRetries(
            attempts = 1, {
              case t: Throwable =>
                metricsReporter.setGetJournalRowsCallDuration(System.nanoTime() - callStart)
                metricsReporter.incrementGetJournalRowsCallErrorCounter()
                Source.failed(t)
            }
          )
          .withAttributes(logLevels)
      }
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
      startTimeSource
        .flatMapConcat { itemStart =>
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
              metricsReporter.setGetMessagesItemDuration(System.nanoTime() - itemStart)
              if (response.sdkHttpResponse().isSuccessful) {
                metricsReporter.incrementGetMessagesItemCallCounter()
                if (response.count() > 0)
                  metricsReporter.addGetMessagesItemCounter(response.count().toLong)
                val items            = response.itemsAsScala.getOrElse(Seq.empty).toVector
                val lastEvaluatedKey = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
                val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
                if (lastEvaluatedKey.nonEmpty && (count + response.count()) < max) {
                  logger.debug("next loop: count = {}, response.count = {}", count, response.count())
                  loop(lastEvaluatedKey, combinedSource, count + response.count(), index + 1)
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
    startTimeSource.flatMapConcat { callStat =>
      startTimeSource
        .flatMapConcat { itemStart =>
          val queryRequest = createHighestSequenceNrRequest(persistenceId, fromSequenceNr, deleted)
          Source
            .single(queryRequest)
            .via(queryFlow)
            .flatMapConcat { response =>
              metricsReporter
                .setHighestSequenceNrItemDuration(System.nanoTime() - itemStart)
              if (response.sdkHttpResponse().isSuccessful) {
                metricsReporter.incrementHighestSequenceNrItemCallCounter()
                if (response.count() > 0)
                  metricsReporter.addHighestSequenceNrItemCounter(response.count().toLong)
                val result = response.itemsAsScala
                  .getOrElse(Seq.empty).toVector.headOption.map { head =>
                    head(pluginConfig.columnsDefConfig.sequenceNrColumnName).n().toLong
                  }.getOrElse(0L)
                Source.single(result)
              } else {
                metricsReporter.incrementHighestSequenceNrItemCallErrorCounter()
                val statusCode = response.sdkHttpResponse().statusCode()
                val statusText = response.sdkHttpResponse().statusText()
                Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
              }
            }
        }
        .map { seqNr =>
          metricsReporter.setHighestSequenceNrCallDuration(System.nanoTime() - callStat)
          metricsReporter.incrementHighestSequenceNrCallCounter()
          seqNr
        }.recoverWithRetries(
          attempts = 1, {
            case t: Throwable =>
              metricsReporter.setHighestSequenceNrCallDuration(System.nanoTime() - callStat)
              metricsReporter.incrementHighestSequenceNrCallErrorCounter()
              logger.debug(
                s"highestSequenceNr(persistenceId = $persistenceId, fromSequenceNr = $fromSequenceNr, deleted = $deleted): finished"
              )
              Source.failed(t)
          }
        )
        .withAttributes(logLevels)
    }
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

final class V2JournalRowWriteDriver(
    val asyncClient: Option[DynamoDbAsyncClient],
    val syncClient: Option[DynamoDbSyncClient],
    val pluginConfig: PluginConfig,
    val partitionKeyResolver: PartitionKeyResolver,
    val sortKeyResolver: SortKeyResolver,
    val metricsReporter: MetricsReporter
) extends JournalRowWriteDriver {
  (asyncClient, syncClient) match {
    case (None, None) =>
      throw new IllegalArgumentException("aws clients is both None")
    case _ =>
  }
  val streamClient: Option[DynamoDbAkkaClient] = asyncClient.map { c => DynamoDbAkkaClient(c) }

  private val logger = LoggerFactory.getLogger(getClass)

  private val readDriver = new V2JournalRowReadDriver(
    asyncClient,
    syncClient,
    pluginConfig,
    metricsReporter
  )

  override def getJournalRows(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      max: Long,
      deleted: Option[Boolean] = Some(false)
  ): Source[JournalRow, NotUsed] = {
    readDriver.getJournalRows(persistenceId, fromSequenceNr, toSequenceNr, max, deleted)
  }

  override def getJournalRows(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber,
      deleted: Boolean
  ): Source[Seq[JournalRow], NotUsed] =
    readDriver.getJournalRows(persistenceId, toSequenceNr, deleted)

  override def highestSequenceNr(
      persistenceId: PersistenceId,
      fromSequenceNr: Option[SequenceNumber],
      deleted: Option[Boolean]
  ): Source[Long, NotUsed] =
    readDriver.highestSequenceNr(persistenceId, fromSequenceNr, deleted)

  override def singleDeleteJournalRowFlow: Flow[PersistenceIdWithSeqNr, Long, NotUsed] = {
    Flow[PersistenceIdWithSeqNr].flatMapConcat { persistenceIdWithSeqNr =>
      startTimeSource
        .flatMapConcat { callStart =>
          startTimeSource
            .flatMapConcat { start =>
              val deleteRequest = DeleteItemRequest
                .builder().keyAsScala(
                  Map(
                    pluginConfig.columnsDefConfig.persistenceIdColumnName -> AttributeValue
                      .builder()
                      .s(persistenceIdWithSeqNr.persistenceId.asString).build(),
                    pluginConfig.columnsDefConfig.sequenceNrColumnName -> AttributeValue
                      .builder().n(
                        persistenceIdWithSeqNr.sequenceNumber.asString
                      ).build()
                  )
                ).build()
              Source.single(deleteRequest).via(deleteItemFlow).flatMapConcat { response =>
                metricsReporter.setDeleteJournalRowsItemDuration(System.nanoTime() - start)
                if (response.sdkHttpResponse().isSuccessful) {
                  metricsReporter.incrementDeleteJournalRowsItemCallCounter()
                  Source.single(1L)
                } else {
                  metricsReporter.incrementDeleteJournalRowsItemCallErrorCounter()
                  val statusCode = response.sdkHttpResponse().statusCode()
                  val statusText = response.sdkHttpResponse().statusText()
                  Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
                }
              }
            }.map { n =>
              metricsReporter.setDeleteJournalRowsCallDuration(System.nanoTime() - callStart)
              metricsReporter.incrementDeleteJournalRowsCallCounter()
              n
            }.recoverWithRetries(
              attempts = 1, {
                case t: Throwable =>
                  metricsReporter.setDeleteJournalRowsCallDuration(System.nanoTime() - callStart)
                  metricsReporter.incrementDeleteJournalRowsCallErrorCounter()
                  Source.failed(t)
              }
            )
        }
    }
  }

  override def multiDeleteJournalRowsFlow: Flow[Seq[PersistenceIdWithSeqNr], Long, NotUsed] =
    Flow[Seq[PersistenceIdWithSeqNr]]
      .flatMapConcat { persistenceIdWithSeqNrs =>
        startTimeSource
          .flatMapConcat { callStart =>
            persistenceIdWithSeqNrs
              .map { case PersistenceIdWithSeqNr(pid, seqNr) => s"pid = $pid, seqNr = $seqNr" }.foreach(logger.debug)
            def loopFlow: Flow[Seq[WriteRequest], Long, NotUsed] =
              Flow[Seq[WriteRequest]].flatMapConcat { requestItems =>
                startTimeSource
                  .flatMapConcat { start =>
                    Source
                      .single(requestItems).map { requests =>
                        BatchWriteItemRequest
                          .builder().requestItemsAsScala(Map(pluginConfig.tableName -> requests)).build()
                      }.via(batchWriteItemFlow).flatMapConcat { response =>
                        metricsReporter.setDeleteJournalRowsItemDuration(System.nanoTime() - start)
                        if (response.sdkHttpResponse().isSuccessful) {
                          metricsReporter.incrementDeleteJournalRowsItemCallCounter()
                          if (response.unprocessedItemsAsScala.get.nonEmpty) {
                            val n =
                              requestItems.size - response.unprocessedItems.get(pluginConfig.tableName).size
                            metricsReporter.addDeleteJournalRowsItemCounter(n)
                            Source
                              .single(response.unprocessedItemsAsScala.get(pluginConfig.tableName)).via(loopFlow).map(
                                _ + n
                              )
                          } else {
                            metricsReporter.addDeleteJournalRowsItemCounter(requestItems.size)
                            Source.single(requestItems.size)
                          }
                        } else {
                          metricsReporter.incrementDeleteJournalRowsItemCallErrorCounter()
                          val statusCode = response.sdkHttpResponse().statusCode()
                          val statusText = response.sdkHttpResponse().statusText()
                          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
                        }
                      }
                  }
              }
            if (persistenceIdWithSeqNrs.isEmpty)
              Source.single(0L)
            else
              SourceUtils
                .lazySource { () =>
                  val requestItems = persistenceIdWithSeqNrs.map { persistenceIdWithSeqNr =>
                    WriteRequest
                      .builder().deleteRequest(
                        DeleteRequest
                          .builder().keyAsScala(
                            Map(
                              pluginConfig.columnsDefConfig.persistenceIdColumnName -> AttributeValue
                                .builder()
                                .s(persistenceIdWithSeqNr.persistenceId.asString).build(),
                              pluginConfig.columnsDefConfig.sequenceNrColumnName -> AttributeValue
                                .builder().n(
                                  persistenceIdWithSeqNr.sequenceNumber.asString
                                ).build()
                            )
                          ).build()
                      ).build()
                  }
                  Source
                    .single(requestItems)
                }.via(loopFlow).map { n =>
                  metricsReporter.setDeleteJournalRowsCallDuration(System.nanoTime() - callStart)
                  metricsReporter.incrementDeleteJournalRowsCallCounter()
                  n
                }.recoverWithRetries(
                  attempts = 1, {
                    case t: Throwable =>
                      metricsReporter.setDeleteJournalRowsCallDuration(System.nanoTime() - callStart)
                      metricsReporter.incrementDeleteJournalRowsCallErrorCounter()
                      Source.failed(t)
                  }
                )
          }
      }.withAttributes(logLevels)

  override def updateMessage(journalRow: JournalRow): Source[Unit, NotUsed] = {
    startTimeSource
      .flatMapConcat { callStart =>
        logger.debug(s"updateMessage(journalRow = $journalRow): start")
        val pkey = journalRow.partitionKey(partitionKeyResolver).asString
        val skey = journalRow.sortKey(sortKeyResolver).asString
        val updateRequest = UpdateItemRequest
          .builder()
          .tableName(pluginConfig.tableName).keyAsScala(
            Map(
              pluginConfig.columnsDefConfig.partitionKeyColumnName -> AttributeValue
                .builder()
                .s(pkey).build(),
              pluginConfig.columnsDefConfig.sortKeyColumnName -> AttributeValue
                .builder()
                .s(skey).build()
            )
          ).attributeUpdatesAsScala(
            Map(
              pluginConfig.columnsDefConfig.messageColumnName -> AttributeValueUpdate
                .builder()
                .action(AttributeAction.PUT).value(
                  AttributeValue.builder().b(SdkBytes.fromByteArray(journalRow.message)).build()
                ).build(),
              pluginConfig.columnsDefConfig.orderingColumnName ->
              AttributeValueUpdate
                .builder()
                .action(AttributeAction.PUT).value(
                  AttributeValue.builder().n(journalRow.ordering.toString).build()
                ).build(),
              pluginConfig.columnsDefConfig.deletedColumnName -> AttributeValueUpdate
                .builder()
                .action(AttributeAction.PUT).value(
                  AttributeValue.builder().bool(journalRow.deleted).build()
                ).build()
            ) ++ journalRow.tags
              .map { tag =>
                Map(
                  pluginConfig.columnsDefConfig.tagsColumnName -> AttributeValueUpdate
                    .builder()
                    .action(AttributeAction.PUT).value(AttributeValue.builder().s(tag).build()).build()
                )
              }.getOrElse(Map.empty)
          ).build()
        Source
          .single(updateRequest)
          .via(updateItemFlow).flatMapConcat { response =>
            if (response.sdkHttpResponse().isSuccessful) {
              Source.single(())
            } else {
              val statusCode = response.sdkHttpResponse().statusCode()
              val statusText = response.sdkHttpResponse().statusText()
              logger.debug(s"updateMessage(journalRow = $journalRow): finished")
              Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
            }
          }
          .map { _ =>
            metricsReporter.setUpdateMessageCallDuration(System.nanoTime() - callStart)
            metricsReporter.incrementUpdateMessageCallCounter()
            logger.debug(s"updateMessage(journalRow = $journalRow): finished")
            ()
          }.recoverWithRetries(
            attempts = 1, {
              case t: Throwable =>
                metricsReporter.setUpdateMessageCallDuration(System.nanoTime() - callStart)
                metricsReporter.incrementUpdateMessageCallErrorCounter()
                logger.debug(s"updateMessage(journalRow = $journalRow): finished")
                Source.failed(t)
            }
          )
      }.withAttributes(logLevels)
  }

  override def singlePutJournalRowFlow: Flow[JournalRow, Long, NotUsed] = Flow[JournalRow].flatMapConcat { journalRow =>
    startTimeSource
      .flatMapConcat { itemStart =>
        val pkey = partitionKeyResolver.resolve(journalRow.persistenceId, journalRow.sequenceNumber).asString
        val skey = sortKeyResolver.resolve(journalRow.persistenceId, journalRow.sequenceNumber).asString
        val request = PutItemRequest
          .builder().tableName(pluginConfig.tableName)
          .itemAsScala(
            Map(
              pluginConfig.columnsDefConfig.partitionKeyColumnName -> AttributeValue
                .builder()
                .s(pkey).build(),
              pluginConfig.columnsDefConfig.sortKeyColumnName -> AttributeValue
                .builder()
                .s(skey).build(),
              pluginConfig.columnsDefConfig.persistenceIdColumnName -> AttributeValue
                .builder()
                .s(journalRow.persistenceId.asString).build(),
              pluginConfig.columnsDefConfig.sequenceNrColumnName -> AttributeValue
                .builder()
                .n(journalRow.sequenceNumber.asString).build(),
              pluginConfig.columnsDefConfig.orderingColumnName -> AttributeValue
                .builder()
                .n(journalRow.ordering.toString).build(),
              pluginConfig.columnsDefConfig.deletedColumnName -> AttributeValue
                .builder().bool(journalRow.deleted).build(),
              pluginConfig.columnsDefConfig.messageColumnName -> AttributeValue
                .builder().b(SdkBytes.fromByteArray(journalRow.message)).build()
            ) ++ journalRow.tags
              .map { tag =>
                Map(pluginConfig.columnsDefConfig.tagsColumnName -> AttributeValue.builder().s(tag).build())
              }.getOrElse(
                Map.empty
              )
          ).build()
        Source.single(request).via(putItemFlow).flatMapConcat { response =>
          metricsReporter.setPutJournalRowsItemDuration(System.nanoTime() - itemStart)
          if (response.sdkHttpResponse().isSuccessful) {
            // metricsReporter.setPutJournalRowsCallDuration(System.nanoTime() - callStart)
            metricsReporter.incrementPutJournalRowsCallCounter()
            metricsReporter.addPutJournalRowsItemCallCounter()
            metricsReporter.incrementPutJournalRowsItemCounter()
            Source.single(1L)
          } else {
            metricsReporter.incrementPutJournalRowsCallErrorCounter()
            metricsReporter.incrementPutJournalRowsItemCallErrorCounter()
            val statusCode = response.sdkHttpResponse().statusCode()
            val statusText = response.sdkHttpResponse().statusText()
            Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
          }
        }
      }
  }

  override def multiPutJournalRowsFlow: Flow[Seq[JournalRow], Long, NotUsed] = Flow[Seq[JournalRow]].flatMapConcat {
    journalRows =>
      startTimeSource
        .flatMapConcat { callStart =>
          def loopFlow: Flow[Seq[WriteRequest], Long, NotUsed] =
            Flow[Seq[WriteRequest]].flatMapConcat { requestItems =>
              startTimeSource
                .flatMapConcat { itemStart =>
                  Source
                    .single(requestItems).map { requests =>
                      BatchWriteItemRequest
                        .builder().requestItemsAsScala(Map(pluginConfig.tableName -> requests)).build
                    }.via(batchWriteItemFlow).map((_, itemStart))
                }
                .flatMapConcat {
                  case (response, itemStart) =>
                    metricsReporter.setPutJournalRowsItemDuration(System.nanoTime() - itemStart)
                    if (response.sdkHttpResponse().isSuccessful) {
                      metricsReporter.addPutJournalRowsItemCallCounter()
                      if (response.unprocessedItemsAsScala.get.nonEmpty) {
                        val n = requestItems.size - response.unprocessedItems.get(pluginConfig.tableName).size
                        metricsReporter.addPutJournalRowsItemCounter(n)
                        Source
                          .single(response.unprocessedItemsAsScala.get(pluginConfig.tableName)).via(loopFlow).map(
                            _ + n
                          )
                      } else {
                        metricsReporter.addPutJournalRowsItemCounter(requestItems.size)
                        Source.single(requestItems.size)
                      }
                    } else {
                      metricsReporter.incrementPutJournalRowsItemCallErrorCounter()
                      val statusCode = response.sdkHttpResponse().statusCode()
                      val statusText = response.sdkHttpResponse().statusText()
                      Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
                    }
                }
            }

          if (journalRows.isEmpty)
            Source.single(0L)
          else
            SourceUtils
              .lazySource { () =>
                require(journalRows.size == journalRows.toSet.size, "journalRows: keys contains duplicates")
                val journalRowWithPKeyWithSKeys = journalRows.map { journalRow =>
                  val pkey = partitionKeyResolver
                    .resolve(journalRow.persistenceId, journalRow.sequenceNumber).asString
                  val skey = sortKeyResolver.resolve(journalRow.persistenceId, journalRow.sequenceNumber).asString
                  (journalRow, pkey, skey)
                }
                logger.debug(
                  s"journalRowWithPKeyWithSKeys = ${journalRowWithPKeyWithSKeys.mkString("\n", ",\n", "\n")}"
                )
                require(
                  journalRowWithPKeyWithSKeys.map { case (_, p, s) => (p, s) }.toSet.size == journalRows.size,
                  "journalRowWithPKeyWithSKeys: keys contains duplicates"
                )

                val requestItems = journalRowWithPKeyWithSKeys.map {
                  case (journalRow, pkey, skey) =>
                    val pid      = journalRow.persistenceId.asString
                    val seqNr    = journalRow.sequenceNumber.asString
                    val ordering = journalRow.ordering.toString
                    val deleted  = journalRow.deleted
                    val message  = SdkBytes.fromByteArray(journalRow.message)
                    val tagsOpt  = journalRow.tags
                    WriteRequest
                      .builder().putRequest(
                        PutRequest
                          .builder()
                          .itemAsScala(
                            Map(
                              pluginConfig.columnsDefConfig.partitionKeyColumnName -> AttributeValue
                                .builder()
                                .s(pkey).build(),
                              pluginConfig.columnsDefConfig.sortKeyColumnName -> AttributeValue
                                .builder()
                                .s(skey).build(),
                              pluginConfig.columnsDefConfig.persistenceIdColumnName -> AttributeValue
                                .builder()
                                .s(pid).build(),
                              pluginConfig.columnsDefConfig.sequenceNrColumnName -> AttributeValue
                                .builder()
                                .n(seqNr).build(),
                              pluginConfig.columnsDefConfig.orderingColumnName -> AttributeValue
                                .builder()
                                .n(ordering).build(),
                              pluginConfig.columnsDefConfig.deletedColumnName -> AttributeValue
                                .builder().bool(deleted).build(),
                              pluginConfig.columnsDefConfig.messageColumnName -> AttributeValue
                                .builder().b(message).build()
                            ) ++ tagsOpt
                              .map { tags =>
                                Map(
                                  pluginConfig.columnsDefConfig.tagsColumnName -> AttributeValue
                                    .builder().s(tags).build()
                                )
                              }.getOrElse(Map.empty)
                          ).build()
                      ).build()
                }
                Source.single(requestItems)
              }
              .via(loopFlow).map { n =>
                metricsReporter.setPutJournalRowsCallDuration(System.nanoTime() - callStart)
                metricsReporter.incrementPutJournalRowsCallCounter()
                n
              }
              .recoverWithRetries(
                attempts = 1, {
                  case t: Throwable =>
                    metricsReporter.setPutJournalRowsCallDuration(System.nanoTime() - callStart)
                    metricsReporter.incrementPutJournalRowsCallErrorCounter()
                    Source.failed(t)
                }
              )
        }.withAttributes(logLevels)
  }

  private def deleteItemFlow: Flow[DeleteItemRequest, DeleteItemResponse, NotUsed] = {
    (streamClient, syncClient) match {
      case (Some(c), None) =>
        c.deleteItemFlow(1)
      case (None, Some(c)) =>
        val flow = Flow[DeleteItemRequest].map { request =>
          c.deleteItem(request) match {
            case Right(r) => r
            case Left(ex) => throw ex
          }
        }
        DispatcherUtils.applyV2Dispatcher(pluginConfig, flow)
      case _ =>
        throw new IllegalStateException("invalid state")
    }
  }.log("deleteItem")

  private def batchWriteItemFlow = {
    (streamClient, syncClient) match {
      case (Some(c), None) =>
        c.batchWriteItemFlow(1)
      case (None, Some(c)) =>
        val flow = Flow[BatchWriteItemRequest].map { request =>
          c.batchWriteItem(request) match {
            case Right(r) => r
            case Left(ex) => throw ex
          }
        }
        DispatcherUtils.applyV2Dispatcher(pluginConfig, flow)
      case _ =>
        throw new IllegalStateException("invalid state")
    }
  }.log("batchWriteItem")

  private def putItemFlow: Flow[PutItemRequest, PutItemResponse, NotUsed] = {
    (streamClient, syncClient) match {
      case (Some(c), None) =>
        c.putItemFlow(1).log("putItem")
      case (None, Some(c)) =>
        val flow = Flow[PutItemRequest].map { request =>
          c.putItem(request) match {
            case Right(r) => r
            case Left(ex) => throw ex
          }
        }
        DispatcherUtils.applyV2Dispatcher(pluginConfig, flow)
      case _ =>
        throw new IllegalStateException("invalid state")
    }
  }

  private def updateItemFlow: Flow[UpdateItemRequest, UpdateItemResponse, NotUsed] = {
    (streamClient, syncClient) match {
      case (Some(c), None) =>
        c.updateItemFlow(1)
      case (None, Some(c)) =>
        val flow = Flow[UpdateItemRequest].map { request =>
          c.updateItem(request) match {
            case Right(r) => r
            case Left(ex) => throw ex
          }
        }
        DispatcherUtils.applyV2Dispatcher(pluginConfig, flow)
      case _ =>
        throw new IllegalStateException("invalid state")
    }
  }.log("updateItem")
}
