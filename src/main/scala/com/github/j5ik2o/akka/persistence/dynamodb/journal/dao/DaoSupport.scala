package com.github.j5ik2o.akka.persistence.dynamodb.journal.dao

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.scaladsl.Source
import com.github.j5ik2o.akka.persistence.dynamodb.config.JournalColumnsDefConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ JournalRow, PersistenceId, SequenceNumber }
import com.github.j5ik2o.reactive.aws.dynamodb.implicits._
import com.github.j5ik2o.reactive.aws.dynamodb.akka.DynamoDbAkkaClient
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, QueryRequest }

trait DaoSupport {
  protected val streamClient: DynamoDbAkkaClient
  protected val tableName: String
  protected val getJournalRowsIndexName: String
  protected val parallelism: Int
  protected val columnsDefConfig: JournalColumnsDefConfig

  protected val logLevels: Attributes = Attributes.logLevels(
    onElement = Attributes.LogLevels.Debug,
    onFailure = Attributes.LogLevels.Error,
    onFinish = Attributes.LogLevels.Debug
  )

  def getMessages(
      persistenceId: PersistenceId,
      fromSequenceNr: SequenceNumber,
      toSequenceNr: SequenceNumber,
      max: Long,
      deleted: Option[Boolean] = Some(false)
  ): Source[JournalRow, NotUsed] = {
    def loop(lastKey: Option[Map[String, AttributeValue]]): Source[Map[String, AttributeValue], NotUsed] = {
      val queryRequest = QueryRequest
        .builder()
        .tableName(tableName).indexName(getJournalRowsIndexName).keyConditionExpression(
          "#pid = :pid and #snr between :min and :max"
        )
        .filterExpressionAsScala(deleted.map { _ =>
          s"#flg = :flg"
        })
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
        ).exclusiveStartKeyAsScala(lastKey).build()
      Source
        .single(queryRequest).via(streamClient.queryFlow(parallelism)).takeWhile(_.count.exists(_ > 0)).flatMapConcat {
          response =>
            val last = response.lastEvaluatedKeyAsScala.getOrElse(Map.empty)
            if (last.isEmpty) {
              Source(response.itemsAsScala.get.toVector)
            } else {
              loop(response.lastEvaluatedKeyAsScala)
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
      persistenceId = PersistenceId(map(columnsDefConfig.persistenceIdColumnName).s),
      sequenceNumber = SequenceNumber(map(columnsDefConfig.sequenceNrColumnName).n.toLong),
      deleted = map(columnsDefConfig.deletedColumnName).bool.get,
      message = map.get(columnsDefConfig.messageColumnName).map(_.b.asByteArray()).get,
      ordering = map(columnsDefConfig.orderingColumnName).n.toLong,
      tags = map.get(columnsDefConfig.tagsColumnName).map(_.s)
    )
  }

}
