import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}


implicit val system: ActorSystem = ActorSystem("test")
implicit val mat: ActorMaterializer = ActorMaterializer()

val future1 = Source((1 to 10).toVector).take(3).runWith(Sink.seq)


