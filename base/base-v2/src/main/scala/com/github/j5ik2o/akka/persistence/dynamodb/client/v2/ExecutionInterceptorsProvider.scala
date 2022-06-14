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
package com.github.j5ik2o.akka.persistence.dynamodb.client.v2

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor

import scala.collection.immutable.Seq
import scala.util.{ Failure, Success }

trait ExecutionInterceptorsProvider {
  def create: Seq[ExecutionInterceptor]
}

object ExecutionInterceptorsProvider {

  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): ExecutionInterceptorsProvider = {
    val className = pluginConfig.clientConfig.v2ClientConfig.executionInterceptorsProviderClassName
    dynamicAccess
      .createInstanceFor[ExecutionInterceptorsProvider](
        className,
        Seq(classOf[DynamicAccess] -> dynamicAccess, classOf[PluginConfig] -> pluginConfig)
      ) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException("Failed to initialize ExecutionInterceptorsProvider", Some(ex))
    }
  }

  final class Default(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends ExecutionInterceptorsProvider {

    override def create: Seq[ExecutionInterceptor] = {
      val classNames = pluginConfig.clientConfig.v2ClientConfig.executionInterceptorClassNames
      classNames.map { className =>
        dynamicAccess
          .createInstanceFor[ExecutionInterceptor](className, Seq(classOf[PluginConfig] -> pluginConfig)) match {
          case Success(value) => value
          case Failure(ex) =>
            throw new PluginException("Failed to initialize ExecutionInterceptor", Some(ex))
        }
      }
    }
  }

}
