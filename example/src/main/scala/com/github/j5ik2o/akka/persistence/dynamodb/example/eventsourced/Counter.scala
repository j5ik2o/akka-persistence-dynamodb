/*
 * Copyright 2022 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.example.eventsourced

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{ Effect, EventSourcedBehavior }
import com.github.j5ik2o.akka.persistence.dynamodb.example.CounterProtocol.Command
import com.github.j5ik2o.akka.persistence.dynamodb.example.{ CborSerializable, CounterProtocol }

import java.util.UUID

object Counter {

  sealed trait Event extends CborSerializable

  case class ValueAdded(n: Int) extends Event

  case class State(n: Int)

  def apply(id: UUID): Behavior[Command] = Behaviors.setup[Command] { _ =>
    EventSourcedBehavior.apply[Command, Event, State](
      persistenceId = PersistenceId.ofUniqueId(s"counter-${id.toString.replaceAll("-", "")}"),
      emptyState = State(0),
      commandHandler = {
        case (state, CounterProtocol.GetValue(replyTo)) =>
          Effect.reply(replyTo) {
            state.n
          }
        case (_, CounterProtocol.Increment) =>
          Effect.persist(ValueAdded(1))
        case (_, CounterProtocol.IncrementBy(n)) =>
          Effect.persist(ValueAdded(n))
      },
      eventHandler = { case (State(current), ValueAdded(n)) =>
        State(current + n)
      }
    )
  }

}
