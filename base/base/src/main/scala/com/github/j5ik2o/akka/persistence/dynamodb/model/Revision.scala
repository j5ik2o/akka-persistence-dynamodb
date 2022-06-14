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
package com.github.j5ik2o.akka.persistence.dynamodb.model

object Revision {
  val MaxValue: Revision = Revision(Long.MaxValue)
  val MinValue: Revision = Revision(0L)
}

case class Revision(value: Long) extends Ordered[Revision] {
  require(value >= 0, "Invalid value")

  override def toString: String = s"Revision($value)"

  override def compare(that: Revision): Int = {
    value compare that.value
  }

  def asString: String = value.toString
}
