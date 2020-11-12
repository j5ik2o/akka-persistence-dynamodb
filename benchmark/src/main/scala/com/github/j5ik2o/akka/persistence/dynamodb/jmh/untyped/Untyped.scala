package com.github.j5ik2o.akka.persistence.dynamodb.jmh.untyped

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import akka.pattern.ask
import com.github.j5ik2o.akka.persistence.dynamodb.jmh.untyped.UntypedCounter.Increment
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration._

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class UntypedV1Async extends BenchmarkHelper {
  override def clientVersion: String = "v1"
  override def clientType: String    = "async"

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

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class UntypedV1Sync extends BenchmarkHelper {
  override def clientVersion: String = "v1"
  override def clientType: String    = "sync"

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

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class UntypedV2Async extends BenchmarkHelper {
  override def clientVersion: String = "v2"
  override def clientType: String    = "async"

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

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class UntypedV2Sync extends BenchmarkHelper {
  override def clientVersion: String = "v2"
  override def clientType: String    = "sync"

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
