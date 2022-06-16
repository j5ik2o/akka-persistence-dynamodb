package com.github.j5ik2o.akka.persistence.dynamodb.example.untyped.eventsourced

import akka.actor.{ ActorLogging, ActorRef, Props }
import akka.persistence.{
  DeleteMessagesSuccess,
  DeleteSnapshotSuccess,
  PersistentActor,
  RecoveryCompleted,
  SaveSnapshotSuccess,
  SnapshotOffer
}
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

  case class SaveSnapshot(replyTo: ActorRef) extends Command
  case class SaveSnapshotReply()

  final case class DeleteMessage(toSeqNr: Long, replyTo: ActorRef) extends Command
  case class DeleteMessageReply()
  final case class DeleteSnapshot(seqNr: Long, replyTo: ActorRef) extends Command
  case class DeleteSnapshotReply()

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
    case SnapshotOffer(metadata, offeredSnapshot: State) =>
      log.info(s"SnapshotOffer($metadata, $offeredSnapshot)")
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
        log.info(s"seqNr = $lastSequenceNr, event = $event, state = $state")
      }
    case CounterProtocol.IncrementBy(v) =>
      persist(ValueAdded(v)) { _ =>
        state = state.copy(n = state.n + v)
      }
    case CounterProtocol.GetValue(replyTo) =>
      replyTo ! GetValueReply(state.n, lastSequenceNr)

    case CounterProtocol.SaveSnapshot(replyTo) =>
      saveSnapshot(state)
      context.become(waitingSaveSnapshot(replyTo))

    case CounterProtocol.DeleteMessage(toSeqNr, replyTo) =>
      deleteMessages(toSeqNr)
      context.become(waitingDeleteMessage(replyTo))

    case CounterProtocol.DeleteSnapshot(toSeqNr, replyTo) =>
      deleteSnapshot(toSeqNr)
      context.become(waitingDeleteSnapshot(replyTo))

  }

  def waitingSaveSnapshot(replyTo: ActorRef): Receive = { case SaveSnapshotSuccess(metadata) =>
    log.info(s"SaveSnapshotSuccess($metadata)")
    replyTo ! CounterProtocol.SaveSnapshotReply()
    context.unbecome()
  }

  def waitingDeleteMessage(replyTo: ActorRef): Receive = { case DeleteMessagesSuccess(toSeqNr) =>
    log.info(s"DeleteMessagesSuccess($toSeqNr)")
    replyTo ! CounterProtocol.DeleteMessageReply()
    context.unbecome()
  }

  def waitingDeleteSnapshot(replyTo: ActorRef): Receive = { case DeleteSnapshotSuccess(metadata) =>
    log.info(s"DeleteSnapshotSuccess($metadata)")
    replyTo ! CounterProtocol.DeleteSnapshotReply()
    context.unbecome()
  }

}
