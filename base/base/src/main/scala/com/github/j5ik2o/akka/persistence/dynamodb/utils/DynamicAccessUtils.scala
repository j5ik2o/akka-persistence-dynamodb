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
package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException

import scala.reflect.ClassTag
import scala.util.{ Failure, Success }

object DynamicAccessUtils {
  def createInstanceFor_DA_PC[A](className: String, pluginContext: PluginContext)(implicit
      classTag: ClassTag[A]
  ): A = {
    import pluginContext._
    dynamicAccess
      .createInstanceFor[A](
        className,
        Seq(
          classOf[DynamicAccess] -> dynamicAccess,
          classOf[PluginConfig]  -> pluginConfig
        )
      ).recoverWith { case _ =>
        dynamicAccess
          .createInstanceFor[A](
            className,
            Seq(
              classOf[PluginContext] -> pluginContext
            )
          )
      } match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException(s"Failed to initialize ${classTag.runtimeClass.getName}", Some(ex))
    }
  }

}
