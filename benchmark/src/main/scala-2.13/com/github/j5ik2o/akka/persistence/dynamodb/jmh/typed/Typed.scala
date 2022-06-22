/*
 * Copyright 2020 Junichi Kato
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.j5ik2o.akka.persistence.dynamodb.jmh.typed

import akka.actor.typed.ActorSystem

import java.util.concurrent.TimeUnit
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.util.Timeout
import com.github.j5ik2o.akka.persistence.dynamodb.jmh.typed.TypedCounter.{ Increment, IncrementReply }
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
final class TypedV1Async extends BenchmarkHelper {
  override def clientVersion: String = "v1"
  override def clientType: String    = "async"

  @Benchmark
  def increment(): Unit = {
    implicit val to: Timeout                       = 10.seconds
    implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
    val future                                     = typedRef.ask[IncrementReply](ref => Increment(1, ref))
    try {
      Await.result(future, Duration.Inf)
    } catch {
      case NonFatal(ex) =>
        ex.printStackTrace()
    }
  }

}

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
final class TypedV1Sync extends BenchmarkHelper {
  override def clientVersion: String = "v1"
  override def clientType: String    = "sync"

  @Benchmark
  def increment(): Unit = {
    implicit val to: Timeout                       = 10.seconds
    implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
    val future                                     = typedRef.ask[IncrementReply](ref => Increment(1, ref))
    try {
      Await.result(future, Duration.Inf)
    } catch {
      case NonFatal(ex) =>
        ex.printStackTrace()
    }
  }

}

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
final class TypedV2Async extends BenchmarkHelper {
  override def clientVersion: String = "v2"
  override def clientType: String    = "async"

  @Benchmark
  def increment(): Unit = {
    implicit val to: Timeout                       = 10.seconds
    implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
    val future                                     = typedRef.ask[IncrementReply](ref => Increment(1, ref))
    try {
      Await.result(future, Duration.Inf)
    } catch {
      case NonFatal(ex) =>
        ex.printStackTrace()
    }
  }

}

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
final class TypedV2Sync extends BenchmarkHelper {
  override def clientVersion: String = "v2"
  override def clientType: String    = "sync"

  @Benchmark
  def increment(): Unit = {
    implicit val to: Timeout                       = 10.seconds
    implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
    val future                                     = typedRef.ask[IncrementReply](ref => Increment(1, ref))
    try {
      Await.result(future, Duration.Inf)
    } catch {
      case NonFatal(ex) =>
        ex.printStackTrace()
    }
  }

}
