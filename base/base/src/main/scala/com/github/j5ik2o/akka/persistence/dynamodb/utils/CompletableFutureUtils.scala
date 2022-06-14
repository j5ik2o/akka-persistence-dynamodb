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
