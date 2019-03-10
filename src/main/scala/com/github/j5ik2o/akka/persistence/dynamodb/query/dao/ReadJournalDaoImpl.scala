package com.github.j5ik2o.akka.persistence.dynamodb.query.dao

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.serialization.Serialization
import akka.stream.Attributes
import akka.stream.scaladsl.Source
import com.github.j5ik2o.akka.persistence.dynamodb.Columns._
import com.github.j5ik2o.akka.persistence.dynamodb.config.QueryPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.{ Columns, JournalRow }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDBAsyncClientV2
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDBStreamClient
import com.github.j5ik2o.reactive.aws.dynamodb.model._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ ExecutionContext, Future }

class ReadJournalDaoImpl(asyncClient: DynamoDBAsyncClientV2,
                         serialization: Serialization,
                         dynamoDBConfig: QueryPluginConfig)(implicit ec: ExecutionContext)
    extends ReadJournalDao {

  type State = Option[Map[String, AttributeValue]]
  type Elm   = Seq[Map[String, AttributeValue]]
  private val logger            = LoggerFactory.getLogger(getClass)
  private val tableName: String = dynamoDBConfig.tableName
  private val batchSize: Int    = dynamoDBConfig.batchSize
  private val parallelism: Int  = dynamoDBConfig.parallelism

  private val logLevels = Attributes.logLevels(onElement = Attributes.LogLevels.Info,
                                               onFailure = Attributes.LogLevels.Error,
                                               onFinish = Attributes.LogLevels.Info)

  private val streamClient: DynamoDBStreamClient = DynamoDBStreamClient(asyncClient)

  private def scan(lastKey: Option[Map[String, AttributeValue]]): Future[ScanResponse] = {
    asyncClient.scan(
      ScanRequest()
        .withTableName(Some(tableName))
        .withSelect(Some(Select.ALL_ATTRIBUTES))
        .withLimit(Some(batchSize))
        .withExclusiveStartKey(lastKey)
    )
  }

  override def allPersistenceIdsSource(max: Long): Source[String, NotUsed] = {
    logger.info("allPersistenceIdsSource: max = {}", max)
    Source
      .unfoldAsync[Option[State], Elm](None) {
        case None =>
          scan(None)
            .map { v =>
              if (v.isSuccessful)
                if (v.lastEvaluatedKey.isEmpty) Some(None, v.items.get)
                else Some(Some(v.lastEvaluatedKey), v.items.get)
              else
                throw new Exception
            }
        case Some(Some(lastKey)) if lastKey.nonEmpty =>
          scan(Some(lastKey))
            .map { v =>
              if (v.isSuccessful)
                if (v.lastEvaluatedKey.isEmpty) Some(None, v.items.get)
                else Some(Some(v.lastEvaluatedKey), v.items.get)
              else
                throw new Exception
            }
        case _ =>
          Future.successful(None)
      }.log("unfold")
      .takeWhile(_.nonEmpty)
      .mapConcat(_.toVector)
      .filterNot { v =>
        v(Columns.DeletedColumnName).bool.get
      }
      .map { map =>
        map(Columns.PersistenceIdColumnName).string.get
      }
      .fold(Set.empty[String]) { case (r, e) => r + e }
      .mapConcat(_.toVector)
      .take(max)
      .withAttributes(logLevels)
  }

  override def eventsByTag(tag: String, offset: Long, maxOffset: Long, max: Long): Source[JournalRow, NotUsed] = {
    val request = ScanRequest()
      .withTableName(Some(tableName))
      .withIndexName(Some("TagsIndex"))
      .withFilterExpression(Some("contains(#tags, :tag)"))
      .withExpressionAttributeNames(
        Some(
          Map("#tags" -> Columns.TagsColumnName)
        )
      )
      .withExpressionAttributeValues(
        Some(
          Map(
            ":tag" -> AttributeValue().withString(Some(tag))
          )
        )
      )
    Source
      .single(request).via(streamClient.scanFlow(parallelism)).map { response =>
        if (response.isSuccessful)
          response.items.getOrElse(Seq.empty)
        else
          throw new Exception
      }
      .takeWhile(_.nonEmpty)
      .mapConcat(_.toVector)
      .map(convertToJournalRow)
      .fold(ArrayBuffer.empty[JournalRow]) { case (r, e) => r append e; r }
      .map(_.sortBy(_.ordering))
      .mapConcat(_.toVector)
      .statefulMapConcat { () =>
        val index = new AtomicLong()
        journalRow =>
          List(journalRow.withOrdering(index.incrementAndGet()))
      }
      .filter { row =>
        row.ordering > offset && row.ordering <= maxOffset
      }
      .take(max)
      .withAttributes(logLevels)
  }

  override def getMessages(persistenceId: String,
                           fromSequenceNr: Long,
                           toSequenceNr: Long,
                           max: Long,
                           deleted: Option[Boolean] = Some(false)): Source[JournalRow, NotUsed] = {
    Source
      .unfold(fromSequenceNr) {
        case nr if nr > toSequenceNr =>
          None
        case nr =>
          Some(nr + 1, nr)
      }
      .grouped(batchSize).log("grouped")
      .map { seqNos =>
        BatchGetItemRequest()
          .withRequestItems(
            Some(
              Map(
                tableName -> KeysAndAttributes()
                  .withKeys(
                    Some(
                      seqNos.map { seqNr =>
                        Map(PersistenceIdColumnName -> AttributeValue().withString(Some(persistenceId)),
                            SequenceNrColumnName    -> AttributeValue().withNumber(Some(seqNr.toString)))
                      }
                    )
                  )
              )
            )
          )
      }
      .via(streamClient.batchGetItemFlow(parallelism))
      .map { batchGetItemResponse =>
        if (batchGetItemResponse.isSuccessful) {
          batchGetItemResponse.responses.getOrElse(Map.empty)(tableName).toVector
        } else
          throw new Exception
      }
      .takeWhile(_.nonEmpty)
      .mapConcat(_.toVector)
      .map(convertToJournalRow)
      .fold(ArrayBuffer.empty[JournalRow]) { case (r, e) => r append e; r }
      .map(_.sortBy(_.ordering))
      .mapConcat(_.toVector)
      .statefulMapConcat { () =>
        val index = new AtomicLong()
        journalRow =>
          List(journalRow.withOrdering(index.incrementAndGet()))
      }
      .filter(_.deleted == false).log("journalRow")
      .take(max)
      .withAttributes(logLevels)

  }

  private def convertToJournalRow(map: Map[String, AttributeValue]): JournalRow = {
    JournalRow(
      persistenceId = map(PersistenceIdColumnName).string.get,
      sequenceNumber = map(SequenceNrColumnName).number.get.toLong,
      deleted = map(DeletedColumnName).bool.get,
      message = {
        map.get(MessageColumnName).flatMap(_.binary).get

      },
      ordering = map(OrderingColumnName).number.get.toLong,
      tags = map.get(TagsColumnName).flatMap(_.string)
    )
  }

  override def journalSequence(offset: Long, limit: Long): Source[Long, NotUsed] = {
    Source
      .single(QueryRequest().withTableName(Some(tableName)))
      .via(streamClient.queryFlow(parallelism))
      .map { result =>
        if (result.isSuccessful)
          result.items.get.map(_(Columns.OrderingColumnName).number.get.toLong)
        else
          throw new Exception
      }
      .takeWhile(_.nonEmpty)
      .mapConcat(_.toVector)
      .drop(offset)
      .take(limit)
      .withAttributes(logLevels)
  }

  override def maxJournalSequence(): Source[Long, NotUsed] = {
    Source.single(Long.MaxValue)
//    val queryRequest =
//      QueryRequest()
//        .withTableName(Some(tableName)).withKeyConditions(Some(Map("" -> Condition().withComparisonOperator(Some(ConditionalOperator.))))).withExpressionAttributeNames(
//          Some(Map("#pid" -> Columns.PersistenceIdColumnName))
//        )
//        .withScanIndexForward(
//          Some(false)
//        ).withLimit(
//          Some(1)
//        )
//    streamClient.underlying
//      .query(queryRequest).flatMap { result =>
//        if (result.isSuccessful)
//          Future.successful(
//            result.items
//              .getOrElse(Seq.empty).map(_(Columns.OrderingColumnName).number.get.toLong).headOption.getOrElse(0L)
//          )
//        else
//          Future.failed(new Exception())
//      }
  }

}
