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
package com.github.j5ik2o.akka.persistence.dynamodb.example.durablestate

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorSystem, Behavior }
import akka.util.Timeout
import com.github.j5ik2o.akka.persistence.dynamodb.example.CounterProtocol
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DynamoDBContainerHelper
import com.typesafe.config.{ Config, ConfigFactory }

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.{ Duration, DurationInt }
import scala.util.Try

object Main extends App with DynamoDBContainerHelper {
  trait Message

  case class AdaptedResponse(id: UUID, n: Try[Int]) extends Message

  def apply(): Behavior[Message] = Behaviors.setup[Message] { ctx =>
    implicit val timeout: Timeout = 3.seconds

    val id         = UUID.randomUUID()
    val counterRef = ctx.spawnAnonymous(Counter(id))

    counterRef ! CounterProtocol.Increment
    counterRef ! CounterProtocol.Increment
    counterRef ! CounterProtocol.Increment

    ctx.ask(counterRef, CounterProtocol.GetValue) { n =>
      AdaptedResponse(id, n)
    }

    def stopped: Behavior[Message] = {
      Behaviors.setup { ctx =>
        val counterRef = ctx.spawnAnonymous(Counter(id))
        counterRef ! CounterProtocol.Increment
        ctx.ask(counterRef, CounterProtocol.GetValue) { n =>
          AdaptedResponse(id, n)
        }

        Behaviors.receiveMessage { case AdaptedResponse(id, n) =>
          ctx.log.info(s"id = $id, count = $n")
          Behaviors.stopped
        }
      }
    }

    Behaviors.receiveMessage { case AdaptedResponse(id, n) =>
      ctx.log.info(s"id = $id, count = $n")
      ctx.stop(counterRef)
      stopped
    }

  }

  dynamoDbLocalContainer.start()
  Thread.sleep(1000)
  createTable()

  override protected lazy val dynamoDBPort = 8000

  val config: Config               = ConfigFactory.load()
  val system: ActorSystem[Message] = ActorSystem(apply(), "main", config)

  Await.result(system.whenTerminated, Duration.Inf)

  dynamoDbLocalContainer.stop()

}
