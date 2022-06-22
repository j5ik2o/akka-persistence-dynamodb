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

import akka.serialization._

final case class MyPayload(data: String)

final class MyPayloadSerializer extends Serializer {
  val MyPayloadClass: Class[MyPayload] = classOf[MyPayload]

  def identifier: Int          = 77123
  def includeManifest: Boolean = true

  def toBinary(o: AnyRef): Array[Byte] = o match {
    case MyPayload(data) => s"${data}".getBytes("UTF-8")
    case _               => throw new Exception("Unknown object for serialization")
  }

  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case Some(MyPayloadClass) => MyPayload(s"${new String(bytes, "UTF-8")}")
    case Some(c)              => throw new Exception(s"unexpected manifest ${c}")
    case None                 => throw new Exception("no manifest")
  }
}
