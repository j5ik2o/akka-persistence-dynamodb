import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source

import scala.concurrent.Await
import scala.concurrent.duration.Duration


implicit val system: ActorSystem = ActorSystem("test")
implicit val mat: ActorMaterializer = ActorMaterializer()

val future1 = Source((1 to 10000).toVector)
  .map(_ * 3).async
  .filter(_ % 2 == 0)
  .map(_ + 3)
  .runForeach(println)

Await.result(future1, Duration.Inf)

