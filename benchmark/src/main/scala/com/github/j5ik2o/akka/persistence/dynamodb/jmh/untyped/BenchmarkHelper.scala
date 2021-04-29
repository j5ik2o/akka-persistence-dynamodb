package com.github.j5ik2o.akka.persistence.dynamodb.jmh.untyped

import java.util.UUID

import akka.actor.{ ActorRef, ActorSystem, Props }
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
  var system: ActorSystem  = _
  var untypedRef: ActorRef = _

  @Setup
  def setup(): Unit = {
    startDockerContainers()
    Thread.sleep(1000)
    createTable()
    system = ActorSystem("benchmark-" + UUID.randomUUID().toString, config)

    val props = Props(new UntypedCounter(UUID.randomUUID()))
    untypedRef = system.actorOf(props, "untyped-counter")
  }

  @TearDown
  def tearDown(): Unit = {
    stopDockerContainers()
    system.terminate()
  }
}
