/*
 * Copyright 2017 Dennis Vriend
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
package com.github.j5ik2o.akka.persistence.dynamodb.query

import akka.actor.{ ActorLogging, ActorRef }
import akka.actor.Status.Success
import akka.event.LoggingReceive
import akka.persistence._
import akka.persistence.journal.Tagged

object PersistenceTestActor {
  final case class DeleteCmd(toSequenceNr: Long = Long.MaxValue) extends Serializable
}

class PersistenceTestActor(id: Int) extends PersistentActor with ActorLogging {
  import PersistenceTestActor._
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
