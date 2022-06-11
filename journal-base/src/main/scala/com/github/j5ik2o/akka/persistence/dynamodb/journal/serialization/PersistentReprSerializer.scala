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
package com.github.j5ik2o.akka.persistence.dynamodb.journal.serialization

import akka.persistence.journal.Tagged
import akka.persistence.{ AtomicWrite, PersistentRepr }

import scala.collection.immutable._
import scala.concurrent.{ ExecutionContext, Future }

trait PersistentReprSerializer[A] {

  def serialize(atomicWrites: Seq[AtomicWrite])(implicit ec: ExecutionContext): Seq[Future[Seq[A]]] = {
    atomicWrites.map { atomicWrite =>
      val serialized = atomicWrite.payload.zipWithIndex.map { case (v, index) =>
        serialize(v, Some(index))
      }
      Future.sequence(serialized)
    }
  }

  def serialize(persistentRepr: PersistentRepr, index: Option[Int])(implicit ec: ExecutionContext): Future[A] =
    persistentRepr.payload match {
      case Tagged(payload, tags) =>
        serialize(persistentRepr.withPayload(payload), tags, index)
      case _ =>
        serialize(persistentRepr, Set.empty[String], index)
    }

  def serialize(persistentRepr: PersistentRepr)(implicit ec: ExecutionContext): Future[A] =
    serialize(persistentRepr, None)

  def serialize(persistentRepr: PersistentRepr, tags: Set[String], index: Option[Int])(implicit
      ec: ExecutionContext
  ): Future[A]

  def deserialize(t: A)(implicit ec: ExecutionContext): Future[(PersistentRepr, Set[String], Long)]

}
