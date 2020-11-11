package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v2

import java.io.IOException

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.javadsl.{ Flow => JavaFlow }
import akka.stream.scaladsl.{ Flow, RestartFlow, Source, SourceUtils }
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal._
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.{ JournalRowWriteDriver, PersistenceIdWithSeqNr }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils._
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbClient => JavaDynamoDbSyncClient
}

import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

final class V2JournalRowWriteDriver(
    val system: ActorSystem,
    val asyncClient: Option[JavaDynamoDbAsyncClient],
    val syncClient: Option[JavaDynamoDbSyncClient],
    val pluginConfig: JournalPluginConfig,
    val partitionKeyResolver: PartitionKeyResolver,
    val sortKeyResolver: SortKeyResolver,
    val metricsReporter: Option[MetricsReporter]
) extends JournalRowWriteDriver {
  (asyncClient, syncClient) match {
    case (None, None) =>
      throw new IllegalArgumentException("aws clients is both None")
    case _ =>
  }

  private val logger = LoggerFactory.getLogger(getClass)

  private val readDriver: V2JournalRowReadDriver = new V2JournalRowReadDriver(
    system,
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

  override def singleDeleteJournalRowFlow: Flow[PersistenceIdWithSeqNr, Long, NotUsed] =
    Flow[PersistenceIdWithSeqNr].flatMapConcat { persistenceIdWithSeqNr =>
      val deleteRequest = DeleteItemRequest
        .builder().key(
          Map(
            pluginConfig.columnsDefConfig.persistenceIdColumnName -> AttributeValue
              .builder()
              .s(persistenceIdWithSeqNr.persistenceId.asString).build(),
            pluginConfig.columnsDefConfig.sequenceNrColumnName -> AttributeValue
              .builder().n(
                persistenceIdWithSeqNr.sequenceNumber.asString
              ).build()
          ).asJava
        ).build()
      Source.single(deleteRequest).via(deleteItemFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful) {
          Source.single(1L)
        } else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
        }
      }
    }

  override def multiDeleteJournalRowsFlow: Flow[Seq[PersistenceIdWithSeqNr], Long, NotUsed] =
    Flow[Seq[PersistenceIdWithSeqNr]]
      .flatMapConcat { persistenceIdWithSeqNrs =>
        persistenceIdWithSeqNrs
          .map { case PersistenceIdWithSeqNr(pid, seqNr) => s"pid = $pid, seqNr = $seqNr" }.foreach(logger.debug)
        def loopFlow: Flow[Seq[WriteRequest], Long, NotUsed] =
          Flow[Seq[WriteRequest]].flatMapConcat { requestItems =>
            Source
              .single(requestItems).map { requests =>
                BatchWriteItemRequest
                  .builder().requestItems(Map(pluginConfig.tableName -> requests.asJava).asJava).build()
              }.via(batchWriteItemFlow).flatMapConcat { response =>
                if (response.sdkHttpResponse().isSuccessful) {
                  if (Option(response.unprocessedItems).map(_.asScala).get.nonEmpty) {
                    val n =
                      requestItems.size - response.unprocessedItems.get(pluginConfig.tableName).size
                    Source
                      .single(
                        Option(response.unprocessedItems)
                          .map(_.asScala.toMap).map(_.map { case (k, v) => (k, v.asScala.toVector) }).get(
                            pluginConfig.tableName
                          )
                      ).via(loopFlow).map(
                        _ + n
                      )
                  } else {
                    Source.single(requestItems.size)
                  }
                } else {
                  val statusCode = response.sdkHttpResponse().statusCode()
                  val statusText = response.sdkHttpResponse().statusText()
                  Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
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
                      .builder().key(
                        Map(
                          pluginConfig.columnsDefConfig.persistenceIdColumnName -> AttributeValue
                            .builder()
                            .s(persistenceIdWithSeqNr.persistenceId.asString).build(),
                          pluginConfig.columnsDefConfig.sequenceNrColumnName -> AttributeValue
                            .builder().n(
                              persistenceIdWithSeqNr.sequenceNumber.asString
                            ).build()
                        ).asJava
                      ).build()
                  ).build()
              }
              Source
                .single(requestItems)
            }.via(loopFlow)
      }.withAttributes(logLevels)

  override def updateMessage(journalRow: JournalRow): Source[Unit, NotUsed] = {
    val pkey = journalRow.partitionKey(partitionKeyResolver).asString
    val skey = journalRow.sortKey(sortKeyResolver).asString
    val updateRequest = UpdateItemRequest
      .builder()
      .tableName(pluginConfig.tableName).key(
        Map(
          pluginConfig.columnsDefConfig.partitionKeyColumnName -> AttributeValue
            .builder()
            .s(pkey).build(),
          pluginConfig.columnsDefConfig.sortKeyColumnName -> AttributeValue
            .builder()
            .s(skey).build()
        ).asJava
      ).attributeUpdates(
        (Map(
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
          }.getOrElse(Map.empty)).asJava
      ).build()
    Source
      .single(updateRequest)
      .via(updateItemFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful) {
          Source.single(())
        } else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
        }
      }
  }.withAttributes(logLevels)

  override def singlePutJournalRowFlow: Flow[JournalRow, Long, NotUsed] = Flow[JournalRow].flatMapConcat { journalRow =>
    val pkey = partitionKeyResolver.resolve(journalRow.persistenceId, journalRow.sequenceNumber).asString
    val skey = sortKeyResolver.resolve(journalRow.persistenceId, journalRow.sequenceNumber).asString
    val request = PutItemRequest
      .builder().tableName(pluginConfig.tableName)
      .item(
        (Map(
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
          .map { tag => Map(pluginConfig.columnsDefConfig.tagsColumnName -> AttributeValue.builder().s(tag).build()) }.getOrElse(
            Map.empty
          )).asJava
      ).build()
    Source.single(request).via(putItemFlow).flatMapConcat { response =>
      if (response.sdkHttpResponse().isSuccessful) {
        Source.single(1L)
      } else {
        val statusCode = response.sdkHttpResponse().statusCode()
        val statusText = response.sdkHttpResponse().statusText()
        Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
      }
    }
  }

  override def multiPutJournalRowsFlow: Flow[Seq[JournalRow], Long, NotUsed] = Flow[Seq[JournalRow]].flatMapConcat {
    journalRows =>
      def loopFlow: Flow[Seq[WriteRequest], Long, NotUsed] =
        Flow[Seq[WriteRequest]].flatMapConcat { requestItems =>
          Source
            .single(requestItems).map { requests =>
              BatchWriteItemRequest
                .builder().requestItems(Map(pluginConfig.tableName -> requests.asJava).asJava).build
            }.via(batchWriteItemFlow)
            .flatMapConcat {
              case response =>
                if (response.sdkHttpResponse().isSuccessful) {
                  if (Option(response.unprocessedItems).map(_.asScala).get.nonEmpty) {
                    val n = requestItems.size - response.unprocessedItems.get(pluginConfig.tableName).size
                    Source
                      .single(
                        Option(response.unprocessedItems)
                          .map(_.asScala).map(_.map { case (k, v) => (k, v.asScala.toVector) }).get(
                            pluginConfig.tableName
                          )
                      ).via(loopFlow).map(
                        _ + n
                      )
                  } else {
                    Source.single(requestItems.size)
                  }
                } else {
                  val statusCode = response.sdkHttpResponse().statusCode()
                  val statusText = response.sdkHttpResponse().statusText()
                  Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
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
                      .item(
                        (Map(
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
                          }.getOrElse(Map.empty)).asJava
                      ).build()
                  ).build()
            }
            Source.single(requestItems)
          }
          .via(loopFlow).withAttributes(logLevels)

  }

  private def deleteItemFlow: Flow[DeleteItemRequest, DeleteItemResponse, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          JavaFlow.create[DeleteItemRequest]().mapAsync(1, { request => c.deleteItem(request) }).asScala
        case (None, Some(c)) =>
          Flow[DeleteItemRequest].map { request => c.deleteItem(request) }.withV2Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("deleteItemFlow")
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

  private def batchWriteItemFlow: Flow[BatchWriteItemRequest, BatchWriteItemResponse, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          JavaFlow.create[BatchWriteItemRequest]().mapAsync(1, { request => c.batchWriteItem(request) }).asScala
        case (None, Some(c)) =>
          Flow[BatchWriteItemRequest].map { request => c.batchWriteItem(request) }.withV2Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("batchWriteItemFlow")
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

  private def putItemFlow: Flow[PutItemRequest, PutItemResponse, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          JavaFlow.create[PutItemRequest]().mapAsync(1, { request => c.putItem(request) }).asScala
        case (None, Some(c)) =>
          Flow[PutItemRequest].map { request => c.putItem(request) }.withV2Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("putItemFlow")
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

  private def updateItemFlow: Flow[UpdateItemRequest, UpdateItemResponse, NotUsed] = {
    val flow =
      ((asyncClient, syncClient) match {
        case (Some(c), None) =>
          JavaFlow.create[UpdateItemRequest]().mapAsync(1, { request => c.updateItem(request) }).asScala
        case (None, Some(c)) =>
          Flow[UpdateItemRequest].map { request => c.updateItem(request) }.withV2Dispatcher(pluginConfig)
        case _ =>
          throw new IllegalStateException("invalid state")
      }).log("updateItemFlow")
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
