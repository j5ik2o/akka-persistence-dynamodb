package com.github.j5ik2o.akka.persistence.dynamodb.jmh

import java.util.UUID

import akka.actor.{ ActorRef, ActorSystem, Props }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ConfigHelper, DynamoDBContainerHelper }
import org.openjdk.jmh.annotations.{ Setup, TearDown }

trait BenchmarkHelper extends DynamoDBContainerHelper {

  val config =
    ConfigHelper.config(None, legacyConfigFormat = false, legacyJournalMode = false, dynamoDBPort, "v2", "async")
  var system: ActorSystem = _
  var ref: ActorRef       = _

  @Setup
  def setup(): Unit = {
    dynamoDbLocalContainer.start()
    Thread.sleep(1000)
    createTable()
    system = ActorSystem("benchmark-" + UUID.randomUUID().toString, config)
    val props = Props(new Counter(UUID.randomUUID()))
    ref = system.actorOf(props)
  }

  @TearDown
  def tearDown(): Unit = {
    dynamoDbLocalContainer.stop()
    system.terminate()
  }
}
