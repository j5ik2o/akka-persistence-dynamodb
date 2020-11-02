package com.github.j5ik2o.akka.persistence.dynamodb.jmh

import java.util.UUID

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.persistence.{
  PersistentActor,
  RecoveryCompleted,
  SaveSnapshotFailure,
  SaveSnapshotSuccess,
  SnapshotMetadata,
  SnapshotOffer
}
import com.github.j5ik2o.akka.persistence.dynamodb.jmh.UserPersistentActor.{ Increment, IncrementReply, Incremented }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ConfigHelper, DynamoDBContainerHelper }
import org.openjdk.jmh.annotations.{ Setup, TearDown }

object UserPersistentActor {
  sealed trait Command
  trait Reply
  case class Increment(n: Int) extends Command
  case class IncrementReply()  extends Reply
  sealed trait Event
  case class Incremented(n: Int) extends Event
}

class UserPersistentActor(id: UUID) extends PersistentActor {
  private var counter: Int           = 0
  override def persistenceId: String = "User-" + id.toString

  override def receiveRecover: Receive = {
    case n: Int => counter += n
    case SnapshotOffer(_: SnapshotMetadata, counter: Int) =>
      this.counter = counter
    case RecoveryCompleted =>
  }

  override def receiveCommand: Receive = {
    case Increment(n) =>
      persist(n) { _ => sender() ! IncrementReply() }
      if (lastSequenceNr % 100 == 0)
        saveSnapshot(counter)
    case SaveSnapshotSuccess(_)    =>
    case SaveSnapshotFailure(_, _) =>
  }

}

trait BenchmarkHelper extends DynamoDBContainerHelper {

  val config              = ConfigHelper.config(None, false, false, dynamoDBPort, "v2", "async")
  var system: ActorSystem = _
  var ref: ActorRef       = _

  @Setup
  def setup(): Unit = {
    dynamoDbLocalContainer.start()
    Thread.sleep(1000)
    createTable()
    system = ActorSystem("benchmark-" + UUID.randomUUID().toString, config)
    val props = Props(new UserPersistentActor(UUID.randomUUID()))
    ref = system.actorOf(props)
  }

  @TearDown
  def tearDown(): Unit = {
    dynamoDbLocalContainer.stop()
    system.terminate()
  }
}
