package com.github.j5ik2o.akka.persistence.dynamodb.jmh

import java.util.UUID

import akka.actor.typed.scaladsl.adapter._
import akka.actor.{ typed, ActorRef, ActorSystem, Props }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ConfigHelper, DynamoDBContainerHelper }
import org.openjdk.jmh.annotations.{ Setup, TearDown }

trait BenchmarkHelper extends DynamoDBContainerHelper {

  val config =
    ConfigHelper.config(None, legacyConfigFormat = false, legacyJournalMode = false, dynamoDBPort, "v2", "sync")
  var system: ActorSystem                            = _
  var untypedRef: ActorRef                           = _
  var typedRef: typed.ActorRef[TypedCounter.Command] = _

  @Setup
  def setup(): Unit = {
    dynamoDbLocalContainer.start()
    Thread.sleep(1000)
    createTable()
    system = ActorSystem("benchmark-" + UUID.randomUUID().toString, config)

    val props = Props(new UntypedCounter(UUID.randomUUID()))
    untypedRef = system.actorOf(props, "untyped-counter")
    typedRef = system.spawn(TypedCounter(UUID.randomUUID()), "typed-counter")
  }

  @TearDown
  def tearDown(): Unit = {
    dynamoDbLocalContainer.stop()
    system.terminate()
  }
}
