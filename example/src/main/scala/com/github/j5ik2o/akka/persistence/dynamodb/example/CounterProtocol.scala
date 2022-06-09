package com.github.j5ik2o.akka.persistence.dynamodb.example

import akka.actor.typed.ActorRef

object CounterProtocol {
  sealed trait Command extends CborSerializable

  final case object Increment extends Command
  final case class IncrementBy(value: Int) extends Command
  final case class GetValue(replyTo: ActorRef[Int]) extends Command

}
