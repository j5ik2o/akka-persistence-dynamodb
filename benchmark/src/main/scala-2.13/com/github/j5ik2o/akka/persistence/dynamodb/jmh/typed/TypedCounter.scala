/*
 * Copyright 2020 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.jmh.typed

import java.util.UUID

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorRef, Behavior }
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{ Effect, EventSourcedBehavior }

object TypedCounter {

  sealed trait Command

  trait Reply

  final case class Increment(n: Int, replyTo: ActorRef[IncrementReply]) extends Command

  final case class IncrementReply() extends Reply

  def apply(id: UUID): Behavior[Command] = Behaviors.setup[Command] { _ =>
    EventSourcedBehavior[Command, Int, Int](
      persistenceId = PersistenceId.ofUniqueId("User-" + id.toString),
      emptyState = 0,
      commandHandler = { case (_, Increment(n, replyTo)) =>
        Effect.persist(n).thenReply(replyTo)(_ => IncrementReply())
      },
      eventHandler = { case (state, event) =>
        state + event
      }
    )
  // .withRetention(RetentionCriteria.snapshotEvery(100, 1))
  }
}
