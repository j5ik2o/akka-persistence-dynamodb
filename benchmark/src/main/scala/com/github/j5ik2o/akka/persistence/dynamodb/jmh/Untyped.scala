package com.github.j5ik2o.akka.persistence.dynamodb.jmh

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.util.Timeout
import com.github.j5ik2o.akka.persistence.dynamodb.jmh.UntypedCounter.Increment
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration._

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class Untyped extends BenchmarkHelper {

  @Benchmark
  def increment(): Unit = {
    implicit val to = Timeout(10 seconds)
    val future      = untypedRef ? Increment(1)
    try {
      Await.result(future, Duration.Inf)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
    }
  }

}
