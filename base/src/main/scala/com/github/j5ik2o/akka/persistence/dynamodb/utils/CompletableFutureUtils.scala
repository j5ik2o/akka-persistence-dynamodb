package com.github.j5ik2o.akka.persistence.dynamodb.utils

import java.util.concurrent.{ CompletableFuture, ExecutionException, Executor, Future }
import java.util.function.Supplier

import scala.concurrent._

object CompletableFutureUtils {

  implicit class CompletableFutureOps[T](private val future: Future[T]) extends AnyVal {

    def toCompletableFuture(implicit executor: Executor): CompletableFuture[T] =
      CompletableFutureUtils.toCompletableFuture(future)
  }

  def toCompletableFuture[T](future: Future[T])(implicit executor: Executor): CompletableFuture[T] = {
    if (future.isDone) {
      val cf = new CompletableFuture[T]()
      try {
        cf.complete(future.get())
        cf
      } catch {
        case ex: ExecutionException =>
          cf.completeExceptionally(ex.getCause)
          cf
      }
    } else {
      CompletableFuture.supplyAsync(
        new Supplier[T] {
          override def get(): T = {
            try {
              if (future.isDone)
                future.get()
              else
                blocking {
                  future.get()
                }
            } catch {
              case ex: ExecutionException =>
                throw ex.getCause
              case ex: InterruptedException =>
                Thread.currentThread().interrupt()
                throw ex
            }
          }
        },
        executor
      )
    }
  }

}
