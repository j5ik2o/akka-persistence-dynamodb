package com.github.j5ik2o.akka.persistence.dynamodb.journal

import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.Serialization
import akka.stream.{ Materializer, OverflowStrategy, QueueOfferResult }
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.{ Done, NotUsed }
import com.github.j5ik2o.akka.persistence.dynamodb.serialization.FlowPersistentReprSerializer
import com.github.j5ik2o.reactive.dynamodb.DynamoDBAsyncClientV2
import com.github.j5ik2o.reactive.dynamodb.model.AttributeValue

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.Try

class JournalDaoImpl(client: DynamoDBAsyncClientV2,
                     tableName: String,
                     serialization: Serialization,
                     tagSeprator: String)(
    implicit ec: ExecutionContext,
    mat: Materializer
) extends JournalDaoWithUpdates {

  val bufferSize: Int  = 0
  val batchSize        = 0
  val parallelism: Int = 0

  private val serializer: FlowPersistentReprSerializer[JournalRow] =
    new ByteArrayJournalSerializer(serialization, tagSeprator)

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

  private def queueWriteJournalRows(xs: Seq[JournalRow]): Future[Unit] = {
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

  private def writeJournalRows(xs: Seq[JournalRow]): Future[Unit] = ???

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    val serializedTries = serializer.serialize(messages)
    val rowsToWrite = for {
      serializeTry <- serializedTries
      row          <- serializeTry.getOrElse(Seq.empty)
    } yield row
    def resultWhenWriteComplete =
      if (serializedTries.forall(_.isRight)) Nil else serializedTries.map(_.map(_ => ()))
    queueWriteJournalRows(rowsToWrite).map(_ => resultWhenWriteComplete.map(_.toTry).to)
  }

  override def update(persistenceId: String, sequenceNr: Long, payload: AnyRef): Future[Done] = ???

  override def delete(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    client
      .deleteItem(
        tableName,
        Map("persistenceId" -> AttributeValue().withString(Some(persistenceId)),
            "toSequenceNr"  -> AttributeValue().withNumber(Some(toSequenceNr.toString)))
      ).flatMap { result =>
        if (result.isSuccessful) {
          Future.successful(())
        } else {
          Future.failed(new Exception("status code = " + result.statusCode))
        }
      }
  }

  override def highestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = ???

  override def messages(persistenceId: String,
                        fromSequenceNr: Long,
                        toSequenceNr: Long,
                        max: Long): Source[Either[Throwable, PersistentRepr], NotUsed] = ???

}
