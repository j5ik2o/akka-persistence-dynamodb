package com.github.j5ik2o.akka.persistence.dynamodb.jmh.untyped

import akka.persistence._
import com.github.j5ik2o.akka.persistence.dynamodb.jmh.untyped.UntypedCounter.{ Increment, IncrementReply }

import java.util.UUID

object UntypedCounter {
  sealed trait Command
  trait Reply
  case class Increment(n: Int) extends Command
  case class IncrementReply() extends Reply
  sealed trait Event
  case class Incremented(n: Int) extends Event
}

class UntypedCounter(id: UUID) extends PersistentActor {
  private var counter: Int           = 0
  override def persistenceId: String = "User-" + id.toString

  override def receiveRecover: Receive = {
    case n: Int => counter += n
    case SnapshotOffer(_: SnapshotMetadata, counter: Int) =>
      this.counter = counter
    case RecoveryCompleted =>
  }

  override def receiveCommand: Receive = {
    case Increment(n) =>
      persist(n) { _ => sender() ! IncrementReply() }
//      if (lastSequenceNr % 100 == 0)
//        saveSnapshot(counter)
    case SaveSnapshotSuccess(_)    =>
    case SaveSnapshotFailure(_, _) =>
  }

}
