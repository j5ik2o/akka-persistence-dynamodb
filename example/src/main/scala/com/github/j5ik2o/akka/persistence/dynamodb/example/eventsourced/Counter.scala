package com.github.j5ik2o.akka.persistence.dynamodb.example.eventsourced

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.Behavior
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{ Effect, EventSourcedBehavior }
import com.github.j5ik2o.akka.persistence.dynamodb.example.{ CborSerializable, CounterProtocol }
import com.github.j5ik2o.akka.persistence.dynamodb.example.CounterProtocol.Command

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
