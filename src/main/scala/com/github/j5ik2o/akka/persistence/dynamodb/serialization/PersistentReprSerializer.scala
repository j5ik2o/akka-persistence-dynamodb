package com.github.j5ik2o.akka.persistence.dynamodb.serialization

import akka.persistence.journal.Tagged
import akka.persistence.{ AtomicWrite, PersistentRepr }

object EitherSeq {
  def sequence[A](seq: Seq[Either[Throwable, A]]): Either[Throwable, Seq[A]] = {
    def recurse(remaining: Seq[Either[Throwable, A]], processed: Seq[A]): Either[Throwable, Seq[A]] = remaining match {
      case Seq()               => Right(processed)
      case Right(head) +: tail => recurse(remaining = tail, processed :+ head)
      case Left(t) +: _        => Left(t)
    }
    recurse(seq, Vector.empty)
  }
}

trait PersistentReprSerializer[A] {

  def serialize(messages: Seq[AtomicWrite]): Seq[Either[Throwable, Seq[A]]] = {
    messages.map { atomicWrite =>
      val serialized = atomicWrite.payload.map(serialize)
      EitherSeq.sequence(serialized)
    }
  }

  def serialize(persistentRepr: PersistentRepr): Either[Throwable, A] = persistentRepr.payload match {
    case Tagged(payload, tags) =>
      serialize(persistentRepr.withPayload(payload), tags)
    case _ => serialize(persistentRepr, Set.empty[String])
  }

  def serialize(persistentRepr: PersistentRepr, tags: Set[String]): Either[Throwable, A]

  def deserialize(t: A): Either[Throwable, (PersistentRepr, Set[String], Long)]

}
