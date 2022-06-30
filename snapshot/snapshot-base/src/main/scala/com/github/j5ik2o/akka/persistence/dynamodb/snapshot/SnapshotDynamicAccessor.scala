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
package com.github.j5ik2o.akka.persistence.dynamodb.snapshot

import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.config.SnapshotPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DynamicAccessor

import scala.reflect.ClassTag
import scala.util.Try

object SnapshotDynamicAccessor {

  def apply[A: ClassTag](pluginContext: SnapshotPluginContext): SnapshotDynamicAccessor[A] =
    new SnapshotDynamicAccessor[A](pluginContext)

}

final class SnapshotDynamicAccessor[A: ClassTag](pluginContext: SnapshotPluginContext)
    extends DynamicAccessor[A, SnapshotPluginContext](pluginContext) {
  override def create(className: String): Try[A] = {
    createInstanceFor_None(className, pluginContext.dynamicAccess)
      .recoverWith { case _ =>
        createInstanceFor_DA(className, pluginContext.dynamicAccess)
          .recoverWith { case _ =>
            createInstanceFor_PC[SnapshotPluginConfig](
              className,
              pluginContext.dynamicAccess,
              pluginContext.pluginConfig,
              Vector(classOf[PluginConfig], classOf[SnapshotPluginConfig])
            ).recoverWith { case _ =>
              createInstanceFor_DA_PC[SnapshotPluginConfig](
                className,
                pluginContext.dynamicAccess,
                pluginContext.pluginConfig,
                Vector(classOf[PluginConfig], classOf[SnapshotPluginConfig])
              ).recoverWith { case _ =>
                createInstanceFor_CTX(
                  className,
                  pluginContext.dynamicAccess,
                  pluginContext,
                  Vector(
                    classOf[PluginContext],
                    classOf[SnapshotPluginContext]
                  )
                )
              }
            }
          }
      }
  }
}
