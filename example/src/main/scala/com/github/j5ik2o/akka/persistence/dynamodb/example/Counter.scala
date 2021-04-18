package com.github.j5ik2o.akka.persistence.dynamodb.example

import java.util.UUID

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{ Effect, EventSourcedBehavior }

object Counter {

  sealed trait Message
  case class Increment(n: Int) extends Message
  case class State(n: Int)

  def apply(id: UUID): Behavior[Message] = Behaviors.setup[Message] { context =>
    EventSourcedBehavior[Message, Int, State](
      persistenceId = PersistenceId.of("counter", id.toString, "-"),
      emptyState = State(0),
      commandHandler = { case (_, Increment(n)) =>
        Effect.persist(n)
      },
      eventHandler = { case (State(v), n: Int) =>
        State(v + n)
      }
    )
  }

}
