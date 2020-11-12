package com.github.j5ik2o.akka.persistence.dynamodb.jmh.typed

import java.util.concurrent.TimeUnit

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.util.Timeout
import com.github.j5ik2o.akka.persistence.dynamodb.jmh.typed.TypedCounter.{ Increment, IncrementReply }
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration._

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class TypedV1Async extends BenchmarkHelper {
  override def clientVersion: String = "v1"
  override def clientType: String    = "async"

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

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class TypedV1Sync extends BenchmarkHelper {
  override def clientVersion: String = "v1"
  override def clientType: String    = "sync"

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

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class TypedV2Async extends BenchmarkHelper {
  override def clientVersion: String = "v2"
  override def clientType: String    = "async"

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

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class TypedV2Sync extends BenchmarkHelper {
  override def clientVersion: String = "v2"
  override def clientType: String    = "sync"

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
