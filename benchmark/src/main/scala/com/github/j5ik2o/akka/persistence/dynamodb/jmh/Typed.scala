package com.github.j5ik2o.akka.persistence.dynamodb.jmh

import java.util.concurrent.TimeUnit

import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import com.github.j5ik2o.akka.persistence.dynamodb.jmh.TypedCounter.{ Increment, IncrementReply }
import org.openjdk.jmh.annotations.{ Benchmark, BenchmarkMode, Mode, OutputTimeUnit, Scope, State }
import akka.actor.typed.scaladsl.adapter._
import scala.concurrent.Await
import scala.concurrent.duration._

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class Typed extends BenchmarkHelper {

  @Benchmark
  def increment(): Unit = {
    implicit val to          = Timeout(10 seconds)
    implicit val typedSystem = system.toTyped
    val future               = typedRef.ask[IncrementReply](ref => Increment(1, ref))
    try {
      Await.result(future, Duration.Inf)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
    }
  }

}
