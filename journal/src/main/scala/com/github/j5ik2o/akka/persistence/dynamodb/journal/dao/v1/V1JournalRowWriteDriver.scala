package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v1

import java.io.IOException
import java.nio.ByteBuffer

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, RestartFlow, Source, SourceUtils }
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal._
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.{ JournalRowWriteDriver, PersistenceIdWithSeqNr }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, Stopwatch }
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils
import com.github.j5ik2o.akka.persistence.dynamodb.utils.JavaFutureConverter._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

final class V1JournalRowWriteDriver(
    val system: ActorSystem,
    val asyncClient: Option[AmazonDynamoDBAsync],
    val syncClient: Option[AmazonDynamoDB],
    val pluginConfig: JournalPluginConfig,
    val partitionKeyResolver: PartitionKeyResolver,
    val sortKeyResolver: SortKeyResolver,
    val metricsReporter: Option[MetricsReporter]
)(implicit ec: ExecutionContext)
    extends JournalRowWriteDriver {
  (asyncClient, syncClient) match {
    case (None, None) =>
      throw new IllegalArgumentException("aws clients is both None")
    case _ =>
  }
  private val logger = LoggerFactory.getLogger(getClass)

  private val readDriver = new V1JournalRowReadDriver(
    system,
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
          if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
            Source.single(1L)
          } else {
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
                    if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
                      if (response.getUnprocessedItems.asScala.nonEmpty) {
                        val n = requestItems.size - response.getUnprocessedItems.get(pluginConfig.tableName).size
                        val s = response.getUnprocessedItems.asScala
                          .map { case (k, v) => (k, v.asScala.toVector) }
                        val ss = s(pluginConfig.tableName)
                        Source
                          .single(ss).via(loopFlow).map(
                            _ + n
                          )
                      } else {
                        Source.single(requestItems.size)
                      }
                    } else {
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
              .via(loopFlow)
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
                if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
                  Source.single(1L)
                } else {
                  val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
                  Source.failed(new IOException(s"statusCode: $statusCode"))
                }
              }
            }
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
                        if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
                          if (response.getUnprocessedItems.asScala.nonEmpty) {
                            val n =
                              requestItems.size - response.getUnprocessedItems.get(pluginConfig.tableName).size
                            val s  = response.getUnprocessedItems.asScala.map { case (k, v) => (k, v.asScala.toVector) }
                            val ss = s(pluginConfig.tableName)
                            Source.single(ss).via(loopFlow).map(_ + n)
                          } else {
                            Source.single(requestItems.size)
                          }
                        } else {
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
                }.via(loopFlow)
          }
      }.withAttributes(logLevels)

  override def updateMessage(journalRow: JournalRow): Source[Unit, NotUsed] = {
    logger.debug(s"updateMessage(journalRow = $journalRow): start")
    val pkey = journalRow.partitionKey(partitionKeyResolver).asString
    val skey = journalRow.sortKey(sortKeyResolver).asString
    def createUpdateRequest =
      new UpdateItemRequest()
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
      .single(createUpdateRequest)
      .via(updateItemFlow).flatMapConcat { response =>
        if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
          Source.single(())
        } else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
          logger.debug(s"updateMessage(journalRow = $journalRow): finished")
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }.withAttributes(logLevels)

  }

  private def putItemFlow: Flow[PutItemRequest, PutItemResult, NotUsed] = {
    val flow = ((syncClient, asyncClient) match {
      case (Some(c), None) =>
        val flow = Flow[PutItemRequest]
          .map { request =>
            val sw     = Stopwatch.start()
            val result = c.putItem(request)
            metricsReporter.foreach(_.setDynamoDBClientPutItemDuration(sw.elapsed()))
            result
          }
        DispatcherUtils.applyV1Dispatcher(pluginConfig, flow)
      case (None, Some(c)) =>
        Flow[PutItemRequest].mapAsync(1) { request =>
          val sw     = Stopwatch.start()
          val future = c.putItemAsync(request).toScala
          future.onComplete { _ => metricsReporter.foreach(_.setDynamoDBClientPutItemDuration(sw.elapsed())) }
          future
        }
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
          .map { request =>
            val sw     = Stopwatch.start()
            val result = c.batchWriteItem(request)
            metricsReporter.foreach(_.setDynamoDBClientBatchWriteItemDuration(sw.elapsed()))
            result
          }
        DispatcherUtils.applyV1Dispatcher(pluginConfig, flow)
      case (None, Some(c)) =>
        Flow[BatchWriteItemRequest].mapAsync(1) { request =>
          val sw     = Stopwatch.start()
          val future = c.batchWriteItemAsync(request).toScala
          future.onComplete { _ => metricsReporter.foreach(_.setDynamoDBClientBatchWriteItemDuration(sw.elapsed())) }
          future
        }
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
            .map { request =>
              val sw     = Stopwatch.start()
              val result = c.updateItem(request)
              metricsReporter.foreach(_.setDynamoDBClientUpdateItemDuration(sw.elapsed()))
              result
            }
          DispatcherUtils.applyV1Dispatcher(pluginConfig, flow)
        case (None, Some(c)) =>
          Flow[UpdateItemRequest].mapAsync(1) { request =>
            val sw     = Stopwatch.start()
            val future = c.updateItemAsync(request).toScala
            future.onComplete { _ => metricsReporter.foreach(_.setDynamoDBClientUpdateItemDuration(sw.elapsed())) }
            future
          }
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
            .map { request =>
              val sw     = Stopwatch.start()
              val result = c.deleteItem(request)
              metricsReporter.foreach(_.setDynamoDBClientDeleteItemDuration(sw.elapsed()))
              result
            }
          DispatcherUtils.applyV1Dispatcher(pluginConfig, flow)
        case (None, Some(c)) =>
          Flow[DeleteItemRequest].mapAsync(1) { request =>
            val sw     = Stopwatch.start()
            val future = c.deleteItemAsync(request).toScala
            future.onComplete { _ => metricsReporter.foreach(_.setDynamoDBClientDeleteItemDuration(sw.elapsed())) }
            future
          }
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
