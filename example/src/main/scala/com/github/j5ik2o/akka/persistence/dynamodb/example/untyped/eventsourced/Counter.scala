package com.github.j5ik2o.akka.persistence.dynamodb.example.untyped.eventsourced

import akka.actor.{ ActorLogging, ActorRef, Props }
import akka.persistence.{ PersistentActor, RecoveryCompleted, SnapshotOffer, SnapshotSelectionCriteria }
import com.github.j5ik2o.akka.persistence.dynamodb.example.CborSerializable
import com.github.j5ik2o.akka.persistence.dynamodb.example.untyped.eventsourced.CounterProtocol.{
  GetValueReply,
  State,
  ValueAdded
}

import java.util.UUID

object CounterProtocol {
  sealed trait Command extends CborSerializable

  final case object Increment extends Command
  final case class IncrementBy(value: Int) extends Command
  final case class GetValue(replyTo: ActorRef) extends Command
  final case class GetValueReply(n: Int, seqNr: Long)
  final case class DeleteSnapshot(seqNr: Long) extends Command

  sealed trait Event extends CborSerializable

  case class ValueAdded(n: Int) extends Event

  case class State(n: Int) extends CborSerializable
}

object Counter {

  def props(id: UUID): Props = Props(new Counter(id))

}

final class Counter(id: UUID) extends PersistentActor with ActorLogging {
  private var state: State = State(0)

  override def persistenceId: String = s"counter-$id"

  override def receiveRecover: Receive = {
    case SnapshotOffer(_, offeredSnapshot: State) =>
      log.info(s"SnapshotOffer: $offeredSnapshot")
      state = offeredSnapshot
    case ValueAdded(v) =>
      state = state.copy(n = state.n + v)
    case RecoveryCompleted =>
      log.info("recovery completed")
  }

  override def receiveCommand: Receive = {
    case CounterProtocol.Increment =>
      persist(ValueAdded(1)) { event =>
        state = state.copy(n = state.n + 1)
        if (lastSequenceNr % 2 == 0) {
          log.info(s"saveSnapshot($state)")
          saveSnapshot(state)
        }
        log.info(s"seqNr = $lastSequenceNr, event = $event, state = $state")
      }
    case CounterProtocol.IncrementBy(v) =>
      persist(ValueAdded(v)) { _ =>
        state = state.copy(n = state.n + v)
      }
    case CounterProtocol.DeleteSnapshot(seqNr) =>
      deleteSnapshot(seqNr)
    case CounterProtocol.GetValue(replyTo) =>
      replyTo ! GetValueReply(state.n, lastSequenceNr)
  }

}
