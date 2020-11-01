package com.github.j5ik2o.akka.persistence.dynamodb.jmh

import java.net.URI
import java.util.UUID

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.persistence.{ PersistentActor, RecoveryCompleted, SnapshotMetadata, SnapshotOffer }
import com.github.j5ik2o.akka.persistence.dynamodb.jmh.UserPersistentActor.{ Increment, IncrementReply, Incremented }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ConfigHelper, DynamoDBContainerHelper }
import com.github.j5ik2o.reactive.aws.dynamodb.DynamoDbAsyncClient
import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations.{ Setup, TearDown }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient => JavaDynamoDbAsyncClient }

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
    case Incremented(n) => counter += n
    case SnapshotOffer(_: SnapshotMetadata, counter: Int) =>
      this.counter = counter
    case RecoveryCompleted =>
  }

  override def receiveCommand: Receive = {
    case Increment(n) =>
      persist(Incremented(n)) { _ => sender() ! IncrementReply() }
      saveSnapshot(counter)
  }

}

trait BenchmarkHelper extends DynamoDBContainerHelper {

  var ref: ActorRef = _

  val config = ConfigHelper.config("j5ik2o-application", false, false, dynamoDBPort, "v2", "async")

  @Setup
  def setup(): Unit = {
    dynamoDbLocalContainer.start()
    Thread.sleep(1000)
    createTable()

    implicit val system: ActorSystem = ActorSystem("benchmark", config)
    val props                        = Props(new UserPersistentActor(UUID.randomUUID()))
    ref = system.actorOf(props)
  }

  @TearDown
  def tearDown(): Unit = {
    dynamoDbLocalContainer.stop()
  }
}
