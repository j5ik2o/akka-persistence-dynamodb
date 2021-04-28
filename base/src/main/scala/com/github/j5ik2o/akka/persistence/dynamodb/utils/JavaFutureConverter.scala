package com.github.j5ik2o.akka.persistence.dynamodb.utils

import java.util.concurrent.{ ExecutionException, Future => JavaFuture }

import scala.concurrent.{ ExecutionContext, Future => ScalaFuture }

object JavaFutureConverter {

  implicit def to[A](jf: JavaFuture[A]): to[A] = new to[A](jf)

  class to[A](jf: JavaFuture[A]) {

    def toScala(implicit ec: ExecutionContext): ScalaFuture[A] = {
      ScalaFuture(jf.get()).recoverWith { case e: ExecutionException =>
        ScalaFuture.failed(e.getCause)
      }
    }
  }

}
