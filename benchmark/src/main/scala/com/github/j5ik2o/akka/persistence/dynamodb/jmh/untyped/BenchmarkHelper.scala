package com.github.j5ik2o.akka.persistence.dynamodb.jmh.untyped

import akka.actor.{ ActorRef, ActorSystem, Props }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ConfigHelper, DynamoDBContainerHelper }
import com.typesafe.config.Config
import org.openjdk.jmh.annotations.{ Setup, TearDown }

import java.util.UUID

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
    dynamoDbLocalContainer.start()
    Thread.sleep(1000)
    createTable()
    system = ActorSystem("benchmark-" + UUID.randomUUID().toString, config)

    val props = Props(new UntypedCounter(UUID.randomUUID()))
    untypedRef = system.actorOf(props, "untyped-counter")
  }

  @TearDown
  def tearDown(): Unit = {
    dynamoDbLocalContainer.stop()
    system.terminate()
  }
}
