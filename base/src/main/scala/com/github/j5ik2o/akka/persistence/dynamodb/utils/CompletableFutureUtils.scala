package com.github.j5ik2o.akka.persistence.dynamodb.utils

import java.util.concurrent.{ CompletableFuture, ExecutionException, Executor, ForkJoinPool, Future }

import scala.concurrent._

object CompletableFutureUtils {

  implicit class CompletableFutureOps[T](val future: Future[T]) extends AnyVal {

    def toCompletableFuture(implicit executor: Executor): CompletableFuture[T] =
      CompletableFutureUtils.toCompletableFuture(future)
  }

  def toCompletableFuture[T](future: Future[T])(implicit executor: Executor): CompletableFuture[T] = {
    if (future.isDone)
      try {
        CompletableFuture.completedFuture(future.get)
      } catch {
        case ex: ExecutionException =>
          CompletableFuture.failedFuture(ex.getCause)
      }
    else {
      CompletableFuture.supplyAsync(
        { () =>
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
        },
        executor
      )
    }
  }

}
