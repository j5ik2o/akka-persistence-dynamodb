package com.github.j5ik2o.akka.persistence.dynamodb.query

import akka.actor.{ ActorLogging, ActorRef }
import akka.actor.Status.Success
import akka.event.LoggingReceive
import akka.persistence._
import akka.persistence.journal.Tagged

object TestActor {
  final case class DeleteCmd(toSequenceNr: Long = Long.MaxValue) extends Serializable
}

class TestActor(id: Int) extends PersistentActor with ActorLogging {
  import TestActor._
  val pluginName                     = context.system.settings.config.getString("akka.persistence.journal.plugin")
  override def persistenceId: String = "my-" + id
  val label                          = s"$persistenceId - $pluginName"
  log.debug("==> Created test actor: " + persistenceId)
  var state: Int = 1

  def debug(msg: String): Unit = log.debug(s"$msg in state $label")

  def deleteCmd(ref: ActorRef): Receive = LoggingReceive.withLabel(label) {
    case msg @ DeleteMessagesSuccess(toSequenceNr) =>
      debug(s"Deleted up to: $msg")
      ref ! Success(s"deleted-$toSequenceNr")
      context.become(receiveCommand)
    case msg @ DeleteMessagesFailure(t, toSequenceNr) =>
      debug(s"Failed deleting events: $msg")
      ref ! akka.actor.Status.Failure(t)
      context.become(receiveCommand)
  }

  override def receiveCommand: Receive = LoggingReceive.withLabel(label) {
    case DeleteCmd(toSequenceNr) =>
      deleteMessages(toSequenceNr)
      debug(s"Deleting up to: '$toSequenceNr'")
      context.become(deleteCmd(sender()))

    case event @ Tagged(payload: Any, tags) =>
      persist(event.copy(payload = s"$payload-$state")) { _ =>
        increment()
        sender() ! event
      }

    case event =>
      log.info(s">>>>> event = $event")
      persist(s"$event-$state") { _ =>
        increment()
        sender() ! event
      }
  }

  def increment(): Unit = state += 1

  override def receiveRecover: Receive = LoggingReceive.withLabel(label) {
    case event: String     => increment()
    case RecoveryCompleted =>
  }

  override protected def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
    sender() ! akka.actor.Status.Failure(cause)
    context.become(receiveCommand)
    super.onPersistFailure(cause, event, seqNr)
  }

  override protected def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
    sender() ! akka.actor.Status.Failure(cause)
    context.become(receiveCommand)
    super.onPersistRejected(cause, event, seqNr)
  }
}
