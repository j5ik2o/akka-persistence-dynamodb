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
package com.github.j5ik2o.akka.persistence.dynamodb.jmh.typed

import java.util.UUID
import akka.actor.typed.scaladsl.adapter._
import akka.actor.{ typed, ActorSystem }
import com.github.j5ik2o.akka.persistence.dynamodb.jmh
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ConfigHelper, DynamoDBContainerHelper }
import org.openjdk.jmh.annotations.{ Setup, TearDown }
import com.typesafe.config.Config

trait BenchmarkHelper extends DynamoDBContainerHelper {
  def clientVersion: String
  def clientType: String

  val config: Config =
    ConfigHelper.config(
      None,
      legacyConfigFormat = false,
      legacyJournalMode = false,
      dynamoDBHost,
      dynamoDBPort,
      clientVersion,
      clientType
    )
  var system: ActorSystem                            = _
  var typedRef: typed.ActorRef[TypedCounter.Command] = _

  @Setup
  def setup(): Unit = {
    dynamoDbLocalContainer.start()
    Thread.sleep(1000)
    createTable()
    system = ActorSystem("benchmark-" + UUID.randomUUID().toString, config)

    typedRef = system.spawn(jmh.typed.TypedCounter(UUID.randomUUID()), "typed-counter")
  }

  @TearDown
  def tearDown(): Unit = {
    dynamoDbLocalContainer.stop()
    system.terminate()
  }
}
