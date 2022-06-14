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
package com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl

import akka.actor.{ ActorSystem, ExtendedActorSystem }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.testkit.{ TestKit, TestKitBase }
import com.github.j5ik2o.akka.persistence.dynamodb.state.{ MyPayload, MyPayloadSerializer }
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time._
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }

import scala.concurrent.ExecutionContextExecutor

abstract class StateSpecBase(val config: Config)
    extends TestKitBase
    with AnyFreeSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers
    with ScalaFutures {
  implicit lazy val e: ExecutionContextExecutor = system.dispatcher
  override implicit def system: ExtendedActorSystem =
    ActorSystem("test", customSerializers.withFallback(config)).asInstanceOf[ExtendedActorSystem]

  val customSerializers: Config = ConfigFactory.parseString(s"""
      akka.actor {
        serializers {
          my-payload = "${classOf[MyPayloadSerializer].getName}"
        }
        serialization-bindings {
          "${classOf[MyPayload].getName}" = my-payload
        }
      }
    """)

  lazy val serialization: Serialization = SerializationExtension(system)

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = Span(60, Seconds), interval = Span(100, Millis))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
}
