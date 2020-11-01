package com.github.j5ik2o.akka.persistence.dynamodb.jmh

import java.util.concurrent.TimeUnit

import com.github.j5ik2o.akka.persistence.dynamodb.jmh.UserPersistentActor.Increment
import com.github.j5ik2o.akka.persistence.dynamodb.utils.RandomPortUtil
import org.openjdk.jmh.annotations._

@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@State(Scope.Benchmark)
class Untyped extends BenchmarkHelper {

  @Benchmark
  def increment(): Unit = {
    ref ! Increment(1)
  }

}

object TestMain extends BenchmarkHelper with App {

  override protected lazy val dynamoDBPort = RandomPortUtil.temporaryServerPort()

  setup()

  ref ! Increment(1)

  tearDown()
}
