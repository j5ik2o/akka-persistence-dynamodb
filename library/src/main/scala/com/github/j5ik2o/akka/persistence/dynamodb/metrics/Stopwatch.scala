package com.github.j5ik2o.akka.persistence.dynamodb.metrics

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

class Stopwatch {
  val startedAtNanos: Long = System.nanoTime()

  def elapsed(): Duration = {
    (System.nanoTime() - startedAtNanos).nanos
  }
}

object Stopwatch {
  def start(): Stopwatch = new Stopwatch

  def withTime[A](elapsed: Duration => Unit)(future: Future[A])(implicit ec: ExecutionContext): Future[A] =
    Future.successful(start()).flatMap { sw =>
      future.map { result =>
        elapsed(sw.elapsed())
        result
      }
    }

  def withTime[R](f: Duration => Unit)(block: => R): R = {
    val s = Stopwatch.start()
    try {
      block
    } finally {
      f(s.elapsed())
    }
  }
}
