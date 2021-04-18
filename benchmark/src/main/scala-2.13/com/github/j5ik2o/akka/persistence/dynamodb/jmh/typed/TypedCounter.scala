package com.github.j5ik2o.akka.persistence.dynamodb.jmh.typed

import java.util.UUID

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorRef, Behavior }
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{ Effect, EventSourcedBehavior }

object TypedCounter {

  sealed trait Command

  trait Reply

  case class Increment(n: Int, replyTo: ActorRef[IncrementReply]) extends Command

  case class IncrementReply() extends Reply

  def apply(id: UUID): Behavior[Command] = Behaviors.setup[Command] { context =>
    EventSourcedBehavior[Command, Int, Int](
      persistenceId = PersistenceId.ofUniqueId("User-" + id.toString),
      emptyState = 0,
      commandHandler = { case (state, Increment(n, replyTo)) =>
        Effect.persist(n).thenReply(replyTo)(_ => IncrementReply())
      },
      eventHandler = { case (state, event) =>
        state + event
      }
    )
  //.withRetention(RetentionCriteria.snapshotEvery(100, 1))
  }
}
