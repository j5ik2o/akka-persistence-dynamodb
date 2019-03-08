package com.github.j5ik2o.akka.persistence.dynamodb

object TestMain extends App {
  import akka.stream.scaladsl._
  import akka.actor._
  import akka.stream.ActorMaterializer
  import scala.concurrent._
  import scala.concurrent.duration._

  implicit val system = ActorSystem()
  implicit val ec     = system.dispatcher
  implicit val mat    = ActorMaterializer()

  def getUserIds: Seq[Int] = 1 to 5

  val future = Source
    .unfold(5) {
      case _ =>
        Some(5, getUserIds)
    }.mapConcat(_.toVector).take(6).runWith(Sink.seq)

  val result = Await.result(future, Duration.Inf)
  println(result)

  system.terminate()
}
