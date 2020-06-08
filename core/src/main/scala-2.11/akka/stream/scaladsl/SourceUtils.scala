package akka.stream.scaladsl

import akka.stream.scaladsl.Source

import scala.concurrent.Future

object SourceUtils {
  def lazySource[T, M](create: () => Source[T, M]): Source[T, Future[M]] = Source.lazily(create)
}
