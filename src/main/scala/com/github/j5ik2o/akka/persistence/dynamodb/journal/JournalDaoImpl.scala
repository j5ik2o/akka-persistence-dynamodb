package com.github.j5ik2o.akka.persistence.dynamodb.journal

import java.nio.ByteBuffer

import akka.NotUsed
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.Serialization
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.stream.{ Attributes, Materializer, OverflowStrategy, QueueOfferResult }
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.FlowPersistentReprSerializer
import com.github.j5ik2o.reactive.dynamodb.akka.DynamoDBStreamClient
import com.github.j5ik2o.reactive.dynamodb.model._
import com.github.j5ik2o.reactive.dynamodb.monix.DynamoDBTaskClientV2
import monix.eval.Task
import monix.execution.Scheduler

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.Try

class JournalDaoImpl(client: DynamoDBTaskClientV2,
                     serialization: Serialization,
                     dynamoDBConfig: PersistencePluginConfig)(
    implicit ec: ExecutionContext,
    mat: Materializer
) extends JournalDaoWithUpdates {

  implicit val scheduler = Scheduler(ec)

  val OrderingColumnName      = "ordering"
  val DeletedColumnName       = "deleted"
  val MessageColumnName       = "message"
  val PersistenceIdColumnName = "persistence-id"
  val SequenceNrColumnName    = "sequence-nr"
  val TagsColumnName          = "tags"

  val tableName        = dynamoDBConfig.tableName
  val tagSeparator     = dynamoDBConfig.tagSeparator
  val bufferSize: Int  = dynamoDBConfig.bufferSize
  val batchSize        = dynamoDBConfig.batchSize
  val parallelism: Int = dynamoDBConfig.parallelism

  private val streamClient = DynamoDBStreamClient(client.underlying)

  private val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, tagSeparator)

  private val writeQueue = Source
    .queue[(Promise[Unit], Seq[JournalRow])](bufferSize, OverflowStrategy.dropNew)
    .batchWeighted[(Seq[Promise[Unit]], Seq[JournalRow])](batchSize, _._2.size, tup => Vector(tup._1) -> tup._2) {
      case ((promises, rows), (newPromise, newRows)) => (promises :+ newPromise) -> (rows ++ newRows)
    }.mapAsync(parallelism) {
      case (promises, rows) =>
        writeJournalRows(rows)
          .map(unit => promises.foreach(_.success(unit)))
          .recover { case t => promises.foreach(_.failure(t)) }
    }.toMat(Sink.ignore)(Keep.left).run()

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Task[immutable.Seq[Try[Unit]]] = {
    val serializedTries = serializer.serialize(messages)
    val rowsToWrite = for {
      serializeTry <- serializedTries
      row          <- serializeTry.getOrElse(Seq.empty)
    } yield row
    def resultWhenWriteComplete =
      if (serializedTries.forall(_.isRight)) Nil else serializedTries.map(_.map(_ => ()))
    queueWriteJournalRows(rowsToWrite).map(_ => resultWhenWriteComplete.map(_.toTry).to)
  }

  private def queueWriteJournalRows(xs: Seq[JournalRow]): Task[Unit] = Task.deferFuture {
    val promise = Promise[Unit]()
    writeQueue.offer(promise -> xs).flatMap {
      case QueueOfferResult.Enqueued =>
        promise.future
      case QueueOfferResult.Failure(t) =>
        Future.failed(new Exception("Failed to write journal row batch", t))
      case QueueOfferResult.Dropped =>
        Future.failed(
          new Exception(
            s"Failed to enqueue journal row batch write, the queue buffer was full ($bufferSize elements) please check the jdbc-journal.bufferSize setting"
          )
        )
      case QueueOfferResult.QueueClosed =>
        Future.failed(new Exception("Failed to enqueue journal row batch write, the queue was closed"))
    }
  }

  override def update(persistenceId: String, sequenceNr: Long, payload: AnyRef): Task[Unit] = {
    val write = PersistentRepr(payload, sequenceNr, persistenceId)
    val serializedRow = serializer.serialize(write) match {
      case Right(t) => t
      case Left(ex) =>
        throw new IllegalArgumentException(
          s"Failed to serialize ${write.getClass} for update of [$persistenceId] @ [$sequenceNr]"
        )
    }
    client
      .updateItem(
        UpdateItemRequest()
          .withTableName(Some(tableName)).withKey(
            Some(
              Map(
                PersistenceIdColumnName -> AttributeValue().withNumber(Some(persistenceId.toString)),
                SequenceNrColumnName    -> AttributeValue().withString(Some(sequenceNr.toString))
              )
            )
          ).withAttributeUpdates(
            Some(
              Map(
                MessageColumnName -> AttributeValueUpdate()
                  .withAction(Some(AttributeAction.PUT)).withValue(
                    Some(AttributeValue().withBinary(Some(serializedRow.message)))
                  )
              )
            )
          )
      ).map { result =>
        if (result.isSuccessful)
          Task.pure(())
        else
          Task.raiseError(new Exception())
      }
  }

  private def getJournalRow(persistenceId: String,
                            toSequenceNr: Long,
                            deleted: Boolean = false): Task[Seq[JournalRow]] = {
    client
      .query(
        QueryRequest()
          .withTableName(Some(tableName))
          .withKeyConditionExpression(Some("#pid = :pid and #snr <= :snr"))
          .withFilterExpression(Some("#d = :flg"))
          .withExpressionAttributeNames(
            Some(Map("#pid" -> PersistenceIdColumnName, "#snr" -> SequenceNrColumnName, "#d" -> DeletedColumnName))
          )
          .withExpressionAttributeValues(
            Some(
              Map(
                ":pid" -> AttributeValue().withString(Some(persistenceId)),
                ":snr" -> AttributeValue().withNumber(Some(toSequenceNr.toString)),
                ":flg" -> AttributeValue().withBool(Some(deleted))
              )
            )
          )
      ).flatMap { result =>
        if (result.isSuccessful)
          Task.pure(result.items.get)
        else
          Task.raiseError(new Exception)
      }.map { convertToJournalRows }
  }

  /**
    * val actions = for {
    * _ <- * JournalTable
    *  .filter(_.persistenceId === persistenceId)
    *  .filter(_.sequenceNumber <= maxSequenceNr)
    *  .filter(_.deleted === false)
    *  .map(_.deleted).update(true)
    *
    * highestMarkedSequenceNr <- JournalTable.filter(_.persistenceId === persistenceId)
    *   .filter(_.deleted === true).sortBy(_.sequenceNumber.desc).map(_.sequenceNumber)
    *
    * _ <- JournalTable.filter(_.persistenceId === persistenceId)
    *   .filter(_.sequenceNumber <= highestMarkedSequenceNr.getOrElse(0L) - 1)
    *   .delete
    *
    * } yield ()
    *
    * @param persistenceId
    * @param maxSequenceNr
    * @return
    */
  override def delete(persistenceId: String, toSequenceNr: Long): Task[Unit] = {

    client
      .deleteItem(
        tableName,
        Map(
          PersistenceIdColumnName -> AttributeValue().withString(Some(persistenceId)),
          SequenceNrColumnName    -> AttributeValue().withNumber(Some(toSequenceNr.toString))
        )
      ).flatMap { result =>
        if (result.isSuccessful) {
          Task.pure(())
        } else {
          Task.raiseError(new Exception("status code = " + result.statusCode))
        }
      }
  }

  override def highestSequenceNr(persistenceId: String, fromSequenceNr: Long): Task[Long] = {
    client
      .query(
        QueryRequest()
          .withTableName(Some(tableName)).withKeyConditionExpression(
            Some("#pid = :id and #snr >= :nr")
          )
          .withExpressionAttributeNames(
            Some(
              Map(
                "#pid" -> PersistenceIdColumnName,
                "#snr" -> SequenceNrColumnName
              )
            )
          )
          .withExpressionAttributeValues(
            Some(
              Map(":id" -> AttributeValue().withString(Some(persistenceId)),
                  ":nr" -> AttributeValue().withNumber(Some(fromSequenceNr.toString)))
            )
          ).withScanIndexForward(Some(false))
          .withLimit(Some(1))
      ).map(_.items.get.map(_(SequenceNrColumnName).number.get.toLong).headOption.getOrElse(0))
  }

  override def messages(persistenceId: String,
                        fromSequenceNr: Long,
                        toSequenceNr: Long,
                        max: Long): Source[PersistentRepr, NotUsed] = {
    Source(fromSequenceNr to toSequenceNr)
      .grouped(parallelism)
      .log("messages:sequenceNr")
      .map { seqNos =>
        BatchGetItemRequest().withRequestItems(
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
      .via(streamClient.batchGetItemFlow())
      .map { result =>
        if (result.isSuccessful)
          result.responses.get
        else
          throw new Exception
      }
      .mapConcat { result =>
        result(tableName)
          .map { map =>
            JournalRow(
              ordering = map(OrderingColumnName).number.get.toLong,
              deleted = map(DeletedColumnName).bool.get,
              persistenceId = map(PersistenceIdColumnName).string.get,
              sequenceNumber = map(SequenceNrColumnName).number.get.toLong,
              message = {
                map.get(MessageColumnName).flatMap(_.binary).get

              },
              tags = map.get(TagsColumnName).flatMap(_.string)
            )
          }.filter(_.deleted == false).sortBy(_.sequenceNumber).toVector
      }.log("messages:journalRow").addAttributes(
        Attributes.logLevels(onElement = Attributes.LogLevels.Info,
                             onFailure = Attributes.LogLevels.Error,
                             onFinish = Attributes.LogLevels.Info)
      )
      .take(max)
      .via(serializer.deserializeFlowWithoutTags)
  }

  private def convertToJournalRows(values: Seq[Map[String, AttributeValue]]): Seq[JournalRow] = {
    values.map { map =>
      JournalRow(
        ordering = map(OrderingColumnName).number.get.toLong,
        deleted = map(DeletedColumnName).bool.get,
        persistenceId = map(PersistenceIdColumnName).string.get,
        sequenceNumber = map(SequenceNrColumnName).number.get.toLong,
        message = {
          map.get(MessageColumnName).flatMap(_.binary).get

        },
        tags = map.get(TagsColumnName).flatMap(_.string)
      )
    }
  }

  private def writeJournalRows(xs: Seq[JournalRow]): Future[Unit] = {
    client
      .batchWriteItem(
        Map(
          tableName -> xs.map { x =>
            WriteRequest().withPutRequest(
              Some(
                PutRequest().withItem(
                  Some(
                    Map(
                      PersistenceIdColumnName -> AttributeValue().withString(Some(x.persistenceId)),
                      SequenceNrColumnName    -> AttributeValue().withNumber(Some(x.sequenceNumber.toString)),
                      OrderingColumnName      -> AttributeValue().withNumber(Some(x.ordering.toString)),
                      DeletedColumnName       -> AttributeValue().withBool(Some(x.deleted)),
                      MessageColumnName       -> AttributeValue().withBinary(Some(x.message)),
                    ) ++ x.tags
                      .map { t =>
                        Map(TagsColumnName -> AttributeValue().withString(Some(t)))
                      }.getOrElse(Map.empty)
                  )
                )
              )
            )
          }
        )
      ).flatMap { result =>
        if (result.isSuccessful)
          Task.pure(())
        else
          Task.raiseError(new Exception())
      }.runToFuture
  }

}
