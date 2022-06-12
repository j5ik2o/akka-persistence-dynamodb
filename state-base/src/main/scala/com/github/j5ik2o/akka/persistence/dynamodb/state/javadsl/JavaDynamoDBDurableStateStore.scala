/*
 * Copyright 2022 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.state.javadsl

import akka.Done
import akka.actor.ActorSystem
import akka.annotation.ApiMayChange
import akka.persistence.state.javadsl.GetObjectResult
import com.github.j5ik2o.akka.persistence.dynamodb.state.GetRawObjectResult
import com.github.j5ik2o.akka.persistence.dynamodb.state.scaladsl.ScalaDurableStateUpdateStore

import java.util.Optional
import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext

@ApiMayChange
final class JavaDynamoDBDurableStateStore[A](
    system: ActorSystem,
    pluginExecutor: ExecutionContext,
    underlying: ScalaDurableStateUpdateStore[A]
) extends JavaDurableStateUpdateStore[A] {
  implicit val ec: ExecutionContext = pluginExecutor

  override def getRawObject(persistenceId: String): CompletionStage[GetRawObjectResult[A]] =
    toJava(
      underlying.getRawObject(persistenceId)
    )

  override def getObject(persistenceId: String): CompletionStage[GetObjectResult[A]] =
    toJava(
      underlying
        .getObject(persistenceId).map(x =>
          GetObjectResult(Optional.ofNullable(x.value.getOrElse(null.asInstanceOf[A])), x.revision)
        )
    )

  override def upsertObject(persistenceId: String, revision: Long, value: A, tag: String): CompletionStage[Done] =
    toJava(underlying.upsertObject(persistenceId, revision, value, tag))

  override def deleteObject(persistenceId: String): CompletionStage[Done] =
    toJava(underlying.deleteObject(persistenceId))

}
