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
package com.github.j5ik2o.akka.persistence.dynamodb.serialization

import akka.persistence.PersistentRepr
import akka.serialization.Serialization
import com.github.j5ik2o.akka.persistence.dynamodb.journal.JournalRow

import scala.util.{ Failure, Success }

class ByteArrayJournalSerializer(serialization: Serialization, separator: String)
    extends FlowPersistentReprSerializer[JournalRow] {

  override def serialize(persistentRepr: PersistentRepr, tags: Set[String]): Either[Throwable, JournalRow] = {
    serialization
      .serialize(persistentRepr)
      .map(
        JournalRow(persistentRepr.persistenceId,
                   persistentRepr.sequenceNr,
                   persistentRepr.deleted,
                   _,
                   Long.MaxValue,
                   encodeTags(tags, separator))
      ) match {
      case Success(value) => Right(value)
      case Failure(ex)    => Left(ex)
    }
  }

  override def deserialize(journalRow: JournalRow): Either[Throwable, (PersistentRepr, Set[String], Long)] = {
    serialization
      .deserialize(journalRow.message, classOf[PersistentRepr])
      .map((_, decodeTags(journalRow.tags, separator), journalRow.ordering)) match {
      case Success(value) => Right(value)
      case Failure(ex)    => Left(ex)
    }
  }
}
