/*
 * Copyright 2020 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.model

final class PersistenceId(private val value: String) {
  require(value.nonEmpty && value.length <= 2048, "Invalid string length")

  override def toString: String = s"PersistenceId($value)"

  def asString: String = value

  override def equals(other: Any): Boolean = other match {
    case that: PersistenceId =>
      value == that.value
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object PersistenceId {
  val Separator = "-"

  def apply(value: String): PersistenceId = new PersistenceId(value)
}
