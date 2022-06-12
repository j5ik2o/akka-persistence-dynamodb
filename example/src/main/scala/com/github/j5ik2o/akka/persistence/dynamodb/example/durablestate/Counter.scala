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
package com.github.j5ik2o.akka.persistence.dynamodb.example.durablestate

import akka.persistence.typed.PersistenceId
import akka.persistence.typed.state.scaladsl.{ DurableStateBehavior, Effect }
import com.github.j5ik2o.akka.persistence.dynamodb.example.CounterProtocol.Command
import com.github.j5ik2o.akka.persistence.dynamodb.example.{ CborSerializable, CounterProtocol }

import java.util.UUID

object Counter {
  final case class State(value: Int) extends CborSerializable

  private val commandHandler: (State, CounterProtocol.Command) => Effect[State] = (state, command) =>
    command match {
      case CounterProtocol.Increment         => Effect.persist(state.copy(value = state.value + 1))
      case CounterProtocol.IncrementBy(by)   => Effect.persist(state.copy(value = state.value + by))
      case CounterProtocol.GetValue(replyTo) => Effect.reply(replyTo)(state.value)
    }

  def apply(id: UUID): DurableStateBehavior[Command, State] = {
    DurableStateBehavior[Command, State](
      persistenceId = PersistenceId.ofUniqueId(s"counter-${id.toString.replaceAll("-", "")}"),
      emptyState = State(0),
      commandHandler = commandHandler
    )
  }
}
