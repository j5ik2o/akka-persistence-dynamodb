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
package com.github.j5ik2o.akka.persistence.dynamodb.state

sealed trait GetRawObjectResult[+A]

object GetRawObjectResult {
  final case class Just[A](
      pkey: String,
      skey: String,
      persistenceId: String,
      value: A,
      revision: Long,
      serializerId: Int,
      serializerManifest: Option[String],
      tag: Option[String],
      ordering: Long
  ) extends GetRawObjectResult[A]
  final case object Empty extends GetRawObjectResult[Nothing]
}
