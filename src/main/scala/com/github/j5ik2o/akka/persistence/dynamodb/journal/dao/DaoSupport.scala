package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.scaladsl.Source
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalColumnsDefConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PersistenceId, SequenceNumber }
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDBStreamClient
import com.github.j5ik2o.reactive.aws.dynamodb.model.{ AttributeValue, QueryRequest }

trait DaoSupport {
  protected val streamClient: DynamoDBStreamClient
  protected val tableName: String
  protected val getJournalRowsIndexName: String
  protected val parallelism: Int
  protected val columnsDefConfig: JournalColumnsDefConfig

  protected val logLevels: Attributes = Attributes.logLevels(onElement = Attributes.LogLevels.Debug,
                                                             onFailure = Attributes.LogLevels.Error,
                                                             onFinish = Attributes.LogLevels.Debug)

  def getMessages(persistenceId: PersistenceId,
                  fromSequenceNr: SequenceNumber,
                  toSequenceNr: SequenceNumber,
                  max: Long,
                  deleted: Option[Boolean] = Some(false)): Source[JournalRow, NotUsed] = {
    def loop(lastKey: Option[Map[String, AttributeValue]]): Source[Map[String, AttributeValue], NotUsed] = {
      val queryRequest = QueryRequest()
        .withTableName(Some(tableName)).withIndexName(Some(getJournalRowsIndexName)).withKeyConditionExpression(
          Some("#pid = :pid and #snr between :min and :max")
        )
        .withFilterExpression(deleted.map { _ =>
          s"#flg = :flg"
        })
        .withExpressionAttributeNames(
          Some(
            Map(
              "#pid" -> columnsDefConfig.persistenceIdColumnName,
              "#snr" -> columnsDefConfig.sequenceNrColumnName
            ) ++ deleted.map(_ => Map("#flg" -> columnsDefConfig.deletedColumnName)).getOrElse(Map.empty)
          )
        ).withExpressionAttributeValues(
          Some(
            Map(
              ":pid" -> AttributeValue().withString(Some(persistenceId.asString)),
              ":min" -> AttributeValue().withNumber(Some(fromSequenceNr.asString)),
              ":max" -> AttributeValue().withNumber(Some(toSequenceNr.asString))
            ) ++ deleted.map(b => Map(":flg" -> AttributeValue().withBool(Some(b)))).getOrElse(Map.empty)
          )
        ).withExclusiveStartKey(lastKey)
      Source
        .single(queryRequest).via(streamClient.queryFlow(parallelism)).takeWhile(_.count.exists(_ > 0)).flatMapConcat {
          response =>
            val last = response.lastEvaluatedKey.getOrElse(Map.empty)
            if (last.isEmpty) {
              Source(response.items.get.toVector)
            } else {
              loop(response.lastEvaluatedKey)
            }
        }
    }

    if (fromSequenceNr > toSequenceNr)
      Source.empty
    else
      loop(None)
        .map(convertToJournalRow)
        .take(max)
        .withAttributes(logLevels)

  }

  protected def convertToJournalRow(map: Map[String, AttributeValue]): JournalRow = {
    JournalRow(
      persistenceId = PersistenceId(map(columnsDefConfig.persistenceIdColumnName).string.get),
      sequenceNumber = SequenceNumber(map(columnsDefConfig.sequenceNrColumnName).number.get.toLong),
      deleted = map(columnsDefConfig.deletedColumnName).bool.get,
      message = map.get(columnsDefConfig.messageColumnName).flatMap(_.binary).get,
      ordering = map(columnsDefConfig.orderingColumnName).number.get.toLong,
      tags = map.get(columnsDefConfig.tagsColumnName).flatMap(_.string)
    )
  }

}
