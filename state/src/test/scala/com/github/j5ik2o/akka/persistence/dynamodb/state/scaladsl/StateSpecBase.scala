package com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl

import akka.actor.{ ActorSystem, ExtendedActorSystem }
import akka.serialization.SerializationExtension
import akka.testkit.{ TestKit, TestKitBase }
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time._
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }
import akka.serialization.Serialization
import com.github.j5ik2o.akka.persistence.dynamodb.state.{ MyPayload, MyPayloadSerializer }

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
