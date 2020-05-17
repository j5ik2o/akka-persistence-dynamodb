package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v1

import java.io.IOException
import java.nio.ByteBuffer

import akka.NotUsed
import akka.stream.scaladsl.{ Concat, Flow, RestartFlow, Source, SourceUtils }
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.config.{ JournalPluginConfig, PluginConfig }
import com.github.j5ik2o.akka.persistence.dynamodb.journal._
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.{
  JournalRowReadDriver,
  JournalRowWriteDriver,
  PersistenceIdWithSeqNr
}
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils
import com.github.j5ik2o.akka.persistence.dynamodb.utils.JavaFutureConverter._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

final class V1JournalRowReadDriver(
    val asyncClient: Option[AmazonDynamoDBAsync],
    val syncClient: Option[AmazonDynamoDB],
    val pluginConfig: PluginConfig,
    val metricsReporter: MetricsReporter
)(implicit ec: ExecutionContext)
    extends JournalRowReadDriver {
  (asyncClient, syncClient) match {
    case (None, None) =>
      throw new IllegalArgumentException("aws clients is both None")
    case _ =>
  }
  private val logger = LoggerFactory.getLogger(getClass)

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
                  if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
                    metricsReporter.incrementGetJournalRowsItemCallCounter()
                    if (response.getCount > 0)
                      metricsReporter.addGetJournalRowsItemCounter(response.getCount.toLong)
                    val items =
                      Option(response.getItems).map(_.asScala.map(_.asScala.toMap)).getOrElse(Seq.empty).toVector
                    val lastEvaluatedKey = Option(response.getLastEvaluatedKey).map(_.asScala.toMap)
                    val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
                    if (lastEvaluatedKey.nonEmpty) {
                      loop(lastEvaluatedKey, combinedSource, count + response.getCount, index + 1)
                    } else
                      combinedSource
                  } else {
                    metricsReporter.incrementGetJournalRowsItemCallErrorCounter()
                    val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
                    Source.failed(new IOException(s"statusCode: $statusCode"))
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
              if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
                metricsReporter.incrementGetMessagesItemCallCounter()
                if (response.getCount > 0)
                  metricsReporter.addGetMessagesItemCounter(response.getCount.toLong)
                val items            = Option(response.getItems).map(_.asScala.map(_.asScala.toMap)).getOrElse(Seq.empty).toVector
                val lastEvaluatedKey = Option(response.getLastEvaluatedKey).map(_.asScala.toMap)
                val combinedSource   = Source.combine(acc, Source(items))(Concat(_))
                if (lastEvaluatedKey.nonEmpty && (count + response.getCount) < max) {
                  logger.debug("next loop: count = {}, response.count = {}", count, response.getCount)
                  loop(lastEvaluatedKey, combinedSource, count + response.getCount, index + 1)
                } else
                  combinedSource
              } else {
                metricsReporter.incrementGetMessagesItemCallErrorCounter()
                val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
                Source.failed(new IOException(s"statusCode: $statusCode"))
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

  override def highestSequenceNr(
      persistenceId: PersistenceId,
      fromSequenceNr: Option[SequenceNumber],
      deleted: Option[Boolean]
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
              if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
                metricsReporter.incrementHighestSequenceNrItemCallCounter()
                if (response.getCount > 0)
                  metricsReporter.addHighestSequenceNrItemCounter(response.getCount.toLong)
                val result = Option(response.getItems)
                  .map(_.asScala).map(_.map(_.asScala))
                  .getOrElse(Seq.empty).toVector.headOption.map { head =>
                    head(pluginConfig.columnsDefConfig.sequenceNrColumnName).getN.toLong
                  }.getOrElse(0L)
                Source.single(result)
              } else {
                metricsReporter.incrementHighestSequenceNrItemCallErrorCounter()
                val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
                Source.failed(new IOException(s"statusCode: $statusCode"))
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

  private def createHighestSequenceNrRequest(
      persistenceId: PersistenceId,
      fromSequenceNr: Option[SequenceNumber] = None,
      deleted: Option[Boolean] = None
  ): QueryRequest = {
    new QueryRequest()
      .withTableName(pluginConfig.tableName)
      .withIndexName(pluginConfig.getJournalRowsIndexName)
      .withKeyConditionExpression(
        fromSequenceNr.map(_ => "#pid = :id and #snr >= :nr").orElse(Some("#pid = :id")).orNull
      )
      .withFilterExpression(deleted.map(_ => "#d = :flg").orNull)
      .withExpressionAttributeNames(
        (Map(
          "#pid" -> pluginConfig.columnsDefConfig.persistenceIdColumnName
        ) ++ deleted
          .map(_ => Map("#d" -> pluginConfig.columnsDefConfig.deletedColumnName)).getOrElse(Map.empty) ++
        fromSequenceNr
          .map(_ => Map("#snr" -> pluginConfig.columnsDefConfig.sequenceNrColumnName)).getOrElse(Map.empty)).asJava
      )
      .withExpressionAttributeValues(
        (Map(
          ":id" -> new AttributeValue().withS(persistenceId.asString)
        ) ++ deleted
          .map(d => Map(":flg" -> new AttributeValue().withBOOL(d))).getOrElse(Map.empty) ++ fromSequenceNr
          .map(nr => Map(":nr" -> new AttributeValue().withN(nr.asString))).getOrElse(Map.empty)).asJava
      ).withScanIndexForward(false)
      .withLimit(1)
  }

  private def queryFlow: Flow[QueryRequest, QueryResult, NotUsed] = {
    val flow = (
      (syncClient, asyncClient) match {
        case (Some(c), None) =>
          val flow = Flow[QueryRequest]
            .map { request => c.query(request) }
          DispatcherUtils.applyV1Dispatcher(pluginConfig, flow)
        case (None, Some(c)) =>
          Flow[QueryRequest].mapAsync(1) { request => c.queryAsync(request).toScala }
        case _ =>
          throw new IllegalStateException("invalid state")
      }
    ).log("query")
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

  private def createGSIRequest(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber,
      deleted: Boolean,
      lastEvaluatedKey: Option[Map[String, AttributeValue]]
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
      .withExclusiveStartKey(lastEvaluatedKey.map(_.asJava).orNull)
  }

  private def createGSIRequest(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      deleted: Option[Boolean],
      limit: Int,
      lastEvaluatedKey: Option[Map[String, AttributeValue]]
  ): QueryRequest = {
    new QueryRequest()
      .withTableName(pluginConfig.tableName).withIndexName(pluginConfig.getJournalRowsIndexName).withKeyConditionExpression(
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
      .withExclusiveStartKey(lastEvaluatedKey.map(_.asJava).orNull)
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

final class V1JournalRowWriteDriver(
    val asyncClient: Option[AmazonDynamoDBAsync],
    val syncClient: Option[AmazonDynamoDB],
    val pluginConfig: JournalPluginConfig,
    val partitionKeyResolver: PartitionKeyResolver,
    val sortKeyResolver: SortKeyResolver,
    val metricsReporter: MetricsReporter
)(implicit ec: ExecutionContext)
    extends JournalRowWriteDriver {
  (asyncClient, syncClient) match {
    case (None, None) =>
      throw new IllegalArgumentException("aws clients is both None")
    case _ =>
  }
  private val logger = LoggerFactory.getLogger(getClass)

  private val readDriver = new V1JournalRowReadDriver(
    asyncClient,
    syncClient,
    pluginConfig,
    metricsReporter
  )

  override def getJournalRows(
      persistenceId: PersistenceId,
      toSequenceNr: SequenceNumber,
      deleted: Boolean
  ): Source[Seq[JournalRow], NotUsed] = readDriver.getJournalRows(persistenceId, toSequenceNr, deleted)

  override def getJournalRows(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      max: Long,
      deleted: Option[Boolean] = Some(false)
  ): Source[JournalRow, NotUsed] =
    readDriver.getJournalRows(persistenceId, fromSequenceNr, toSequenceNr, max, deleted)

  override def highestSequenceNr(
      persistenceId: PersistenceId,
      fromSequenceNr: Option[SequenceNumber],
      deleted: Option[Boolean]
  ): Source[Long, NotUsed] = readDriver.highestSequenceNr(persistenceId, fromSequenceNr, deleted)

  override def singlePutJournalRowFlow: Flow[JournalRow, Long, NotUsed] = Flow[JournalRow].flatMapConcat { journalRow =>
    startTimeSource
      .flatMapConcat { itemStart =>
        val pkey = partitionKeyResolver.resolve(journalRow.persistenceId, journalRow.sequenceNumber).asString
        val skey = sortKeyResolver.resolve(journalRow.persistenceId, journalRow.sequenceNumber).asString
        val request = new PutItemRequest()
          .withTableName(pluginConfig.tableName)
          .withItem(
            (Map(
              pluginConfig.columnsDefConfig.partitionKeyColumnName -> new AttributeValue()
                .withS(pkey),
              pluginConfig.columnsDefConfig.sortKeyColumnName -> new AttributeValue()
                .withS(skey),
              pluginConfig.columnsDefConfig.persistenceIdColumnName -> new AttributeValue()
                .withS(journalRow.persistenceId.asString),
              pluginConfig.columnsDefConfig.sequenceNrColumnName -> new AttributeValue()
                .withN(journalRow.sequenceNumber.asString),
              pluginConfig.columnsDefConfig.orderingColumnName -> new AttributeValue()
                .withN(journalRow.ordering.toString),
              pluginConfig.columnsDefConfig.deletedColumnName -> new AttributeValue()
                .withBOOL(journalRow.deleted),
              pluginConfig.columnsDefConfig.messageColumnName -> new AttributeValue()
                .withB(ByteBuffer.wrap(journalRow.message))
            ) ++ journalRow.tags
              .map { tag => Map(pluginConfig.columnsDefConfig.tagsColumnName -> new AttributeValue().withS(tag)) }.getOrElse(
                Map.empty
              )).asJava
          )
        Source.single(request).via(putItemFlow).flatMapConcat { response =>
          metricsReporter.setPutJournalRowsItemDuration(System.nanoTime() - itemStart)
          if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
            // metricsReporter.setPutJournalRowsCallDuration(System.nanoTime() - callStart)
            metricsReporter.incrementPutJournalRowsCallCounter()
            metricsReporter.addPutJournalRowsItemCallCounter()
            metricsReporter.incrementPutJournalRowsItemCounter()
            Source.single(1L)
          } else {
            metricsReporter.incrementPutJournalRowsCallErrorCounter()
            metricsReporter.incrementPutJournalRowsItemCallErrorCounter()
            val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
            Source.failed(new IOException(s"statusCode: $statusCode"))
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
                      new BatchWriteItemRequest()
                        .withRequestItems(Map(pluginConfig.tableName -> requests.asJava).asJava)
                    }.via(batchWriteItemFlow).map((_, itemStart))
                }
                .flatMapConcat {
                  case (response, itemStart) =>
                    metricsReporter.setPutJournalRowsItemDuration(System.nanoTime() - itemStart)
                    if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
                      metricsReporter.addPutJournalRowsItemCallCounter()
                      if (response.getUnprocessedItems.asScala.nonEmpty) {
                        val n = requestItems.size - response.getUnprocessedItems.get(pluginConfig.tableName).size
                        metricsReporter.addPutJournalRowsItemCounter(n)
                        val s = response.getUnprocessedItems.asScala
                          .map { case (k, v) => (k, v.asScala.toVector) }
                        val ss = s(pluginConfig.tableName)
                        Source
                          .single(ss).via(loopFlow).map(
                            _ + n
                          )
                      } else {
                        metricsReporter.addPutJournalRowsItemCounter(requestItems.size)
                        Source.single(requestItems.size)
                      }
                    } else {
                      metricsReporter.incrementPutJournalRowsItemCallErrorCounter()
                      val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
                      Source.failed(new IOException(s"statusCode: $statusCode"))
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
                    val message  = ByteBuffer.wrap(journalRow.message)
                    val tagsOpt  = journalRow.tags
                    new WriteRequest()
                      .withPutRequest(
                        new PutRequest()
                          .withItem(
                            (Map(
                              pluginConfig.columnsDefConfig.partitionKeyColumnName -> new AttributeValue()
                                .withS(pkey),
                              pluginConfig.columnsDefConfig.sortKeyColumnName -> new AttributeValue()
                                .withS(skey),
                              pluginConfig.columnsDefConfig.persistenceIdColumnName -> new AttributeValue()
                                .withS(pid),
                              pluginConfig.columnsDefConfig.sequenceNrColumnName -> new AttributeValue()
                                .withN(seqNr),
                              pluginConfig.columnsDefConfig.orderingColumnName -> new AttributeValue()
                                .withN(ordering),
                              pluginConfig.columnsDefConfig.deletedColumnName -> new AttributeValue()
                                .withBOOL(deleted),
                              pluginConfig.columnsDefConfig.messageColumnName -> new AttributeValue()
                                .withB(message)
                            ) ++ tagsOpt
                              .map { tags =>
                                Map(
                                  pluginConfig.columnsDefConfig.tagsColumnName -> new AttributeValue()
                                    .withS(tags)
                                )
                              }.getOrElse(Map.empty)).asJava
                          )
                      )
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

  override def singleDeleteJournalRowFlow: Flow[PersistenceIdWithSeqNr, Long, NotUsed] = {
    Flow[PersistenceIdWithSeqNr].flatMapConcat { persistenceIdWithSeqNr =>
      startTimeSource
        .flatMapConcat { callStart =>
          startTimeSource
            .flatMapConcat { start =>
              val deleteRequest = new DeleteItemRequest()
                .withKey(
                  Map(
                    pluginConfig.columnsDefConfig.persistenceIdColumnName -> new AttributeValue()
                      .withS(persistenceIdWithSeqNr.persistenceId.asString),
                    pluginConfig.columnsDefConfig.sequenceNrColumnName -> new AttributeValue().withN(
                      persistenceIdWithSeqNr.sequenceNumber.asString
                    )
                  ).asJava
                )
              Source.single(deleteRequest).via(deleteItemFlow).flatMapConcat { response =>
                metricsReporter.setDeleteJournalRowsItemDuration(System.nanoTime() - start)
                if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
                  metricsReporter.incrementDeleteJournalRowsItemCallCounter()
                  Source.single(1L)
                } else {
                  metricsReporter.incrementDeleteJournalRowsItemCallErrorCounter()
                  val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
                  Source.failed(new IOException(s"statusCode: $statusCode"))
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
                        new BatchWriteItemRequest().withRequestItems(
                          Map(pluginConfig.tableName -> requests.asJava).asJava
                        )
                      }.via(batchWriteItemFlow).flatMapConcat { response =>
                        metricsReporter.setDeleteJournalRowsItemDuration(System.nanoTime() - start)
                        if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
                          metricsReporter.incrementDeleteJournalRowsItemCallCounter()
                          if (response.getUnprocessedItems.asScala.nonEmpty) {
                            val n =
                              requestItems.size - response.getUnprocessedItems.get(pluginConfig.tableName).size
                            metricsReporter.addDeleteJournalRowsItemCounter(n)
                            val s  = response.getUnprocessedItems.asScala.map { case (k, v) => (k, v.asScala.toVector) }
                            val ss = s(pluginConfig.tableName)
                            Source.single(ss).via(loopFlow).map(_ + n)
                          } else {
                            metricsReporter.addDeleteJournalRowsItemCounter(requestItems.size)
                            Source.single(requestItems.size)
                          }
                        } else {
                          metricsReporter.incrementDeleteJournalRowsItemCallErrorCounter()
                          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
                          Source.failed(new IOException(s"statusCode: $statusCode"))
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
                    new WriteRequest().withDeleteRequest(
                      new DeleteRequest().withKey(
                        Map(
                          pluginConfig.columnsDefConfig.persistenceIdColumnName -> new AttributeValue()
                            .withS(persistenceIdWithSeqNr.persistenceId.asString),
                          pluginConfig.columnsDefConfig.sequenceNrColumnName -> new AttributeValue().withN(
                            persistenceIdWithSeqNr.sequenceNumber.asString
                          )
                        ).asJava
                      )
                    )
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
        val updateRequest = new UpdateItemRequest()
          .withTableName(pluginConfig.tableName).withKey(
            Map(
              pluginConfig.columnsDefConfig.partitionKeyColumnName -> new AttributeValue()
                .withS(pkey),
              pluginConfig.columnsDefConfig.sortKeyColumnName -> new AttributeValue()
                .withS(skey)
            ).asJava
          ).withAttributeUpdates(
            (Map(
              pluginConfig.columnsDefConfig.messageColumnName -> new AttributeValueUpdate()
                .withAction(AttributeAction.PUT).withValue(
                  new AttributeValue().withB(ByteBuffer.wrap(journalRow.message))
                ),
              pluginConfig.columnsDefConfig.orderingColumnName ->
              new AttributeValueUpdate()
                .withAction(AttributeAction.PUT).withValue(
                  new AttributeValue().withN(journalRow.ordering.toString)
                ),
              pluginConfig.columnsDefConfig.deletedColumnName -> new AttributeValueUpdate()
                .withAction(AttributeAction.PUT).withValue(
                  new AttributeValue().withBOOL(journalRow.deleted)
                )
            ) ++ journalRow.tags
              .map { tag =>
                Map(
                  pluginConfig.columnsDefConfig.tagsColumnName -> new AttributeValueUpdate()
                    .withAction(AttributeAction.PUT).withValue(new AttributeValue().withS(tag))
                )
              }.getOrElse(Map.empty)).asJava
          )
        Source
          .single(updateRequest)
          .via(updateItemFlow).flatMapConcat { response =>
            if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
              Source.single(())
            } else {
              val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
              logger.debug(s"updateMessage(journalRow = $journalRow): finished")
              Source.failed(new IOException(s"statusCode: $statusCode"))
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

  private def putItemFlow: Flow[PutItemRequest, PutItemResult, NotUsed] = {
    val flow = ((syncClient, asyncClient) match {
      case (Some(c), None) =>
        val flow = Flow[PutItemRequest]
          .map { request => c.putItem(request) }
        DispatcherUtils.applyV1Dispatcher(pluginConfig, flow)
      case (None, Some(c)) =>
        Flow[PutItemRequest].mapAsync(1) { request => c.putItemAsync(request).toScala }
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

  private def batchWriteItemFlow: Flow[BatchWriteItemRequest, BatchWriteItemResult, NotUsed] = {
    val flow = ((syncClient, asyncClient) match {
      case (Some(c), None) =>
        val flow = Flow[BatchWriteItemRequest]
          .map { request => c.batchWriteItem(request) }
        DispatcherUtils.applyV1Dispatcher(pluginConfig, flow)
      case (None, Some(c)) =>
        Flow[BatchWriteItemRequest].mapAsync(1) { request => c.batchWriteItemAsync(request).toScala }
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

  private def updateItemFlow: Flow[UpdateItemRequest, UpdateItemResult, NotUsed] = {
    val flow = (
      (syncClient, asyncClient) match {
        case (Some(c), None) =>
          val flow = Flow[UpdateItemRequest]
            .map { request => c.updateItem(request) }
          DispatcherUtils.applyV1Dispatcher(pluginConfig, flow)
        case (None, Some(c)) =>
          Flow[UpdateItemRequest].mapAsync(1) { request => c.updateItemAsync(request).toScala }
        case _ =>
          throw new IllegalStateException("invalid state")
      }
    ).log("updateItem")
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

  private def deleteItemFlow: Flow[DeleteItemRequest, DeleteItemResult, NotUsed] = {
    val flow = (
      (syncClient, asyncClient) match {
        case (Some(c), None) =>
          val flow = Flow[DeleteItemRequest]
            .map { request => c.deleteItem(request) }
          DispatcherUtils.applyV1Dispatcher(pluginConfig, flow)
        case (None, Some(c)) =>
          Flow[DeleteItemRequest].mapAsync(1) { request => c.deleteItemAsync(request).toScala }
        case _ =>
          throw new IllegalStateException("invalid state")
      }
    ).log("deleteItem")
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
