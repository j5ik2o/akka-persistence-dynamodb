package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v1

import java.io.IOException
import java.nio.ByteBuffer

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Source }
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v1.StreamWriteClient
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal._
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.{ JournalRowWriteDriver, PersistenceIdWithSeqNr }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
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

  private val streamClient =
    new StreamWriteClient(system, asyncClient, syncClient, pluginConfig, pluginConfig.writeBackoffConfig)

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
          .map { tag =>
            Map(pluginConfig.columnsDefConfig.tagsColumnName -> new AttributeValue().withS(tag))
          }.getOrElse(
            Map.empty
          )).asJava
      )
    Source.single(request).via(streamClient.putItemFlow).flatMapConcat { response =>
      if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
        Source.single(1L)
      } else {
        val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
        Source.failed(new IOException(s"statusCode: $statusCode"))
      }
    }
  }

  override def multiPutJournalRowsFlow: Flow[Seq[JournalRow], Long, NotUsed] = Flow[Seq[JournalRow]].flatMapConcat {
    journalRows =>
      if (journalRows.isEmpty)
        Source.single(0L)
      else {
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

        Source
          .single(journalRowWithPKeyWithSKeys.map { case (journalRow, pkey, skey) =>
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
          }).flatMapConcat { requestItems =>
            Source
              .single(
                new BatchWriteItemRequest()
                  .withRequestItems(Map(pluginConfig.tableName -> requestItems.asJava).asJava)
              ).via(streamClient.recursiveBatchWriteItemFlow).map { _ => requestItems.size.toLong }
          }
      }.withAttributes(logLevels)
  }

  override def multiDeleteJournalRowsFlow: Flow[Seq[PersistenceIdWithSeqNr], Long, NotUsed] =
    Flow[Seq[PersistenceIdWithSeqNr]]
      .flatMapConcat { persistenceIdWithSeqNrs =>
        persistenceIdWithSeqNrs
          .map { case PersistenceIdWithSeqNr(pid, seqNr) => s"pid = $pid, seqNr = $seqNr" }.foreach(logger.debug)
        if (persistenceIdWithSeqNrs.isEmpty)
          Source.single(0L)
        else {
          Source
            .single(persistenceIdWithSeqNrs.map { persistenceIdWithSeqNr =>
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
            }).flatMapConcat { requestItems =>
              Source
                .single(
                  new BatchWriteItemRequest()
                    .withRequestItems(Map(pluginConfig.tableName -> requestItems.asJava).asJava)
                ).via(streamClient.recursiveBatchWriteItemFlow).map { _ => requestItems.size.toLong }
            }
        }
      }
      .withAttributes(logLevels)

  override def singleDeleteJournalRowFlow: Flow[PersistenceIdWithSeqNr, Long, NotUsed] = {
    Flow[PersistenceIdWithSeqNr].flatMapConcat { persistenceIdWithSeqNr =>
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
      Source.single(deleteRequest).via(streamClient.deleteItemFlow).flatMapConcat { response =>
        if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
          Source.single(1L)
        } else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }
    }
  }

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
      .via(streamClient.updateItemFlow).flatMapConcat { response =>
        if (response.getSdkHttpMetadata.getHttpStatusCode == 200) {
          Source.single(())
        } else {
          val statusCode = response.getSdkHttpMetadata.getHttpStatusCode
          logger.debug(s"updateMessage(journalRow = $journalRow): finished")
          Source.failed(new IOException(s"statusCode: $statusCode"))
        }
      }.withAttributes(logLevels)

  }

}
