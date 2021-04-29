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
    startDockerContainers()
    Thread.sleep(1000)
    createTable()
    system = ActorSystem("benchmark-" + UUID.randomUUID().toString, config)

    typedRef = system.spawn(jmh.typed.TypedCounter(UUID.randomUUID()), "typed-counter")
  }

  @TearDown
  def tearDown(): Unit = {
    stopDockerContainers()
    system.terminate()
  }
}
