package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v2

import java.io.IOException

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.FlowShape
import akka.stream.scaladsl.{ Flow, GraphDSL, RestartFlow, Source, SourceUtils, Unzip, Zip }
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal._
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.{ JournalRowWriteDriver, PersistenceIdWithSeqNr }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.{ MetricsReporter, Stopwatch }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DispatcherUtils
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import com.github.j5ik2o.reactive.aws.dynamodb.implicits._
import com.github.j5ik2o.reactive.aws.dynamodb.{ DynamoDbAsyncClient, DynamoDbSyncClient }
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model._

final class V2JournalRowWriteDriver(
    val system: ActorSystem,
    val asyncClient: Option[DynamoDbAsyncClient],
    val syncClient: Option[DynamoDbSyncClient],
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
  val streamClient: Option[DynamoDbAkkaClient] = asyncClient.map { c => DynamoDbAkkaClient(c) }

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
        if (response.sdkHttpResponse().isSuccessful) {
          Source.single(1L)
        } else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
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
                  .builder().requestItemsAsScala(Map(pluginConfig.tableName -> requests)).build()
              }.via(batchWriteItemFlow).flatMapConcat { response =>
                if (response.sdkHttpResponse().isSuccessful) {
                  if (response.unprocessedItemsAsScala.get.nonEmpty) {
                    val n =
                      requestItems.size - response.unprocessedItems.get(pluginConfig.tableName).size
                    Source
                      .single(response.unprocessedItemsAsScala.get(pluginConfig.tableName)).via(loopFlow).map(
                        _ + n
                      )
                  } else {
                    Source.single(requestItems.size)
                  }
                } else {
                  val statusCode = response.sdkHttpResponse().statusCode()
                  val statusText = response.sdkHttpResponse().statusText()
                  Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
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
            }.via(loopFlow)
      }.withAttributes(logLevels)

  override def updateMessage(journalRow: JournalRow): Source[Unit, NotUsed] = {
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
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
        }
      }
  }.withAttributes(logLevels)

  override def singlePutJournalRowFlow: Flow[JournalRow, Long, NotUsed] = Flow[JournalRow].flatMapConcat { journalRow =>
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
          .map { tag => Map(pluginConfig.columnsDefConfig.tagsColumnName -> AttributeValue.builder().s(tag).build()) }.getOrElse(
            Map.empty
          )
      ).build()
    Source.single(request).via(putItemFlow).flatMapConcat { response =>
      if (response.sdkHttpResponse().isSuccessful) {
        Source.single(1L)
      } else {
        val statusCode = response.sdkHttpResponse().statusCode()
        val statusText = response.sdkHttpResponse().statusText()
        Source.failed(new IOException(s"statusCode: $statusCode" + statusText.fold("")(s => s", $s")))
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
                .builder().requestItemsAsScala(Map(pluginConfig.tableName -> requests)).build
            }.via(batchWriteItemFlow)
            .flatMapConcat {
              case response =>
                if (response.sdkHttpResponse().isSuccessful) {
                  if (response.unprocessedItemsAsScala.get.nonEmpty) {
                    val n = requestItems.size - response.unprocessedItems.get(pluginConfig.tableName).size
                    Source
                      .single(response.unprocessedItemsAsScala.get(pluginConfig.tableName)).via(loopFlow).map(
                        _ + n
                      )
                  } else {
                    Source.single(requestItems.size)
                  }
                } else {
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
          .via(loopFlow).withAttributes(logLevels)

  }

  private def deleteItemFlow: Flow[DeleteItemRequest, DeleteItemResponse, NotUsed] = {
    val flow = ((streamClient, syncClient) match {
      case (Some(c), None) =>
        Flow[DeleteItemRequest].flatMapConcat { request =>
          Source
            .single((request, Stopwatch.start())).via(Flow.fromGraph(GraphDSL.create() { implicit b =>
              import GraphDSL.Implicits._
              val unzip = b.add(Unzip[DeleteItemRequest, Stopwatch]())
              val zip   = b.add(Zip[DeleteItemResponse, Stopwatch]())
              unzip.out0 ~> c.deleteItemFlow(1) ~> zip.in0
              unzip.out1 ~> zip.in1
              FlowShape(unzip.in, zip.out)
            })).map {
              case (response, sw) =>
                metricsReporter.foreach(_.setDynamoDBClientDeleteItemDuration(sw.elapsed()))
                response
            }
        }
      case (None, Some(c)) =>
        val flow = Flow[DeleteItemRequest].map { request =>
          val sw     = Stopwatch.start()
          val result = c.deleteItem(request)
          metricsReporter.foreach(_.setDynamoDBClientDeleteItemDuration(sw.elapsed()))
          result match {
            case Right(r) => r
            case Left(ex) => throw ex
          }
        }
        DispatcherUtils.applyV2Dispatcher(pluginConfig, flow)
      case _ =>
        throw new IllegalStateException("invalid state")
    }).log("deleteItem")
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
    val flow = ((streamClient, syncClient) match {
      case (Some(c), None) =>
        Flow[BatchWriteItemRequest].flatMapConcat { request =>
          Source
            .single((request, Stopwatch.start())).via(Flow.fromGraph(GraphDSL.create() { implicit b =>
              import GraphDSL.Implicits._
              val unzip = b.add(Unzip[BatchWriteItemRequest, Stopwatch]())
              val zip   = b.add(Zip[BatchWriteItemResponse, Stopwatch]())
              unzip.out0 ~> c.batchWriteItemFlow(1) ~> zip.in0
              unzip.out1 ~> zip.in1
              FlowShape(unzip.in, zip.out)
            })).map {
              case (response, sw) =>
                metricsReporter.foreach(_.setDynamoDBClientBatchWriteItemDuration(sw.elapsed()))
                response
            }
        }
      case (None, Some(c)) =>
        val flow = Flow[BatchWriteItemRequest].map { request =>
          val sw     = Stopwatch.start()
          val result = c.batchWriteItem(request)
          metricsReporter.foreach(_.setDynamoDBClientBatchWriteItemDuration(sw.elapsed()))
          result match {
            case Right(r) => r
            case Left(ex) => throw ex
          }
        }
        DispatcherUtils.applyV2Dispatcher(pluginConfig, flow)
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

  private def putItemFlow: Flow[PutItemRequest, PutItemResponse, NotUsed] = {
    val flow = ((streamClient, syncClient) match {
      case (Some(c), None) =>
        Flow[PutItemRequest].flatMapConcat { request =>
          Source
            .single((request, Stopwatch.start())).via(Flow.fromGraph(GraphDSL.create() { implicit b =>
              import GraphDSL.Implicits._
              val unzip = b.add(Unzip[PutItemRequest, Stopwatch]())
              val zip   = b.add(Zip[PutItemResponse, Stopwatch]())
              unzip.out0 ~> c.putItemFlow(1) ~> zip.in0
              unzip.out1 ~> zip.in1
              FlowShape(unzip.in, zip.out)
            })).map {
              case (response, sw) =>
                metricsReporter.foreach(_.setDynamoDBClientPutItemDuration(sw.elapsed()))
                response
            }
        }
      case (None, Some(c)) =>
        val flow = Flow[PutItemRequest].map { request =>
          val sw     = Stopwatch.start()
          val result = c.putItem(request)
          metricsReporter.foreach(_.setDynamoDBClientPutItemDuration(sw.elapsed()))
          result match {
            case Right(r) => r
            case Left(ex) => throw ex
          }
        }
        DispatcherUtils.applyV2Dispatcher(pluginConfig, flow)
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

  private def updateItemFlow: Flow[UpdateItemRequest, UpdateItemResponse, NotUsed] = {
    val flow = ((streamClient, syncClient) match {
      case (Some(c), None) =>
        Flow[UpdateItemRequest].flatMapConcat { request =>
          Source
            .single((request, Stopwatch.start())).via(Flow.fromGraph(GraphDSL.create() { implicit b =>
              import GraphDSL.Implicits._
              val unzip = b.add(Unzip[UpdateItemRequest, Stopwatch]())
              val zip   = b.add(Zip[UpdateItemResponse, Stopwatch]())
              unzip.out0 ~> c.updateItemFlow(1) ~> zip.in0
              unzip.out1 ~> zip.in1
              FlowShape(unzip.in, zip.out)
            })).map {
              case (response, sw) =>
                metricsReporter.foreach(_.setDynamoDBClientUpdateItemDuration(sw.elapsed()))
                response
            }
        }
      case (None, Some(c)) =>
        val flow = Flow[UpdateItemRequest].map { request =>
          val sw     = Stopwatch.start()
          val result = c.updateItem(request)
          metricsReporter.foreach(_.setDynamoDBClientUpdateItemDuration(sw.elapsed()))
          result match {
            case Right(r) => r
            case Left(ex) => throw ex
          }
        }
        DispatcherUtils.applyV2Dispatcher(pluginConfig, flow)
      case _ =>
        throw new IllegalStateException("invalid state")
    }).log("updateItem")
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
