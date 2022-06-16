package com.github.j5ik2o.akka.persistence.dynamodb.example.typed.durablestate

import akka.persistence.typed.PersistenceId
import akka.persistence.typed.state.scaladsl.{ DurableStateBehavior, Effect }
import com.github.j5ik2o.akka.persistence.dynamodb.example.CborSerializable
import com.github.j5ik2o.akka.persistence.dynamodb.example.typed.CounterProtocol
import com.github.j5ik2o.akka.persistence.dynamodb.example.typed.CounterProtocol.Command

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
