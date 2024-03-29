/*
 * Copyright 2019 Junichi Kato
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v2

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Source }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v2.StreamWriteClient
import com.github.j5ik2o.akka.persistence.dynamodb.journal._
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.{ JournalRowWriteDriver, PersistenceIdWithSeqNr }
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient => JavaDynamoDbAsyncClient,
  DynamoDbClient => JavaDynamoDbSyncClient
}

import java.io.IOException
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

final class V2JournalRowWriteDriver(
    val pluginContext: JournalPluginContext,
    val asyncClient: Option[JavaDynamoDbAsyncClient],
    val syncClient: Option[JavaDynamoDbSyncClient]
) extends JournalRowWriteDriver {
  override def system: ActorSystem = pluginContext.system

  import pluginContext._
  (asyncClient, syncClient) match {
    case (None, None) =>
      throw new IllegalArgumentException("aws clients is both None")
    case _ =>
  }

  private val logger = LoggerFactory.getLogger(getClass)

  private val streamClient =
    new StreamWriteClient(pluginContext, asyncClient, syncClient, pluginConfig.writeBackoffConfig)

  private val readDriver: V2JournalRowReadDriver = new V2JournalRowReadDriver(
    pluginContext,
    asyncClient,
    syncClient
  )

  override def dispose(): Unit = {
    (asyncClient, syncClient) match {
      case (Some(a), _) => a.close()
      case (_, Some(s)) => s.close()
      case _            =>
    }
  }

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
  ): Source[Option[Long], NotUsed] =
    readDriver.highestSequenceNr(persistenceId, fromSequenceNr, deleted)

  override def singleDeleteJournalRowFlow: Flow[PersistenceIdWithSeqNr, Long, NotUsed] =
    Flow[PersistenceIdWithSeqNr].flatMapConcat { persistenceIdWithSeqNr =>
      val pkey =
        partitionKeyResolver
          .resolve(persistenceIdWithSeqNr.persistenceId, persistenceIdWithSeqNr.sequenceNumber).asString
      val skey =
        sortKeyResolver.resolve(persistenceIdWithSeqNr.persistenceId, persistenceIdWithSeqNr.sequenceNumber).asString
      val deleteRequest = DeleteItemRequest
        .builder().key(
          Map(
            pluginConfig.columnsDefConfig.partitionKeyColumnName -> AttributeValue.builder().s(pkey).build(),
            pluginConfig.columnsDefConfig.sortKeyColumnName      -> AttributeValue.builder().s(skey).build()
          ).asJava
        ).build()
      Source.single(deleteRequest).via(streamClient.deleteItemFlow).flatMapConcat { response =>
        if (response.sdkHttpResponse().isSuccessful) {
          Source.single(1L)
        } else {
          val statusCode = response.sdkHttpResponse().statusCode()
          val statusText = response.sdkHttpResponse().statusText()
          Source.failed(new IOException(s"statusCode: $statusCode" + statusText.asScala.fold("")(s => s", $s")))
        }
      }
    }

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
      .via(streamClient.updateItemFlow).flatMapConcat { response =>
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
          .map { tag =>
            Map(pluginConfig.columnsDefConfig.tagsColumnName -> AttributeValue.builder().s(tag).build())
          }.getOrElse(
            Map.empty
          )).asJava
      ).build()
    Source.single(request).via(streamClient.putItemFlow).flatMapConcat { response =>
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
        if (persistenceIdWithSeqNrs.isEmpty)
          Source.single(0L)
        else
          Source
            .single(persistenceIdWithSeqNrs.map { persistenceIdWithSeqNr =>
              val pkey = partitionKeyResolver
                .resolve(persistenceIdWithSeqNr.persistenceId, persistenceIdWithSeqNr.sequenceNumber).asString
              val skey = sortKeyResolver
                .resolve(persistenceIdWithSeqNr.persistenceId, persistenceIdWithSeqNr.sequenceNumber).asString
              WriteRequest
                .builder().deleteRequest(
                  DeleteRequest
                    .builder().key(
                      Map(
                        pluginConfig.columnsDefConfig.partitionKeyColumnName -> AttributeValue
                          .builder().s(pkey).build(),
                        pluginConfig.columnsDefConfig.sortKeyColumnName -> AttributeValue.builder().s(skey).build()
                      ).asJava
                    ).build()
                ).build()
            }).flatMapConcat { requestItems =>
              Source
                .single(
                  BatchWriteItemRequest
                    .builder().requestItems(Map(pluginConfig.tableName -> requestItems.asJava).asJava).build()
                )
                .via(streamClient.recursiveBatchWriteItemFlow).map { _ =>
                  requestItems.size.toLong
                }
            }
      }.withAttributes(logLevels)

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
          s"multiPutJournalRowsFlow: journalRowWithPKeyWithSKeys = ${journalRowWithPKeyWithSKeys.mkString("\n", ",\n", "\n")}"
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
          }).flatMapConcat { requestItems =>
            Source
              .single(
                BatchWriteItemRequest
                  .builder().requestItems(Map(pluginConfig.tableName -> requestItems.asJava).asJava).build()
              )
              .via(streamClient.recursiveBatchWriteItemFlow).map { _ =>
                requestItems.size.toLong
              }
          }.withAttributes(logLevels)
      }

  }

}
