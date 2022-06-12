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
package com.github.j5ik2o.akka.persistence.dynamodb.client.v1

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException

import java.security.SecureRandom
import scala.collection.immutable._
import scala.util.{ Failure, Success }

trait SecureRandomProvider {
  def create: SecureRandom
}

object SecureRandomProvider {

  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): SecureRandomProvider = {
    val className = pluginConfig.clientConfig.v1ClientConfig.clientConfiguration.secureRandomProviderClassName
    dynamicAccess
      .createInstanceFor[SecureRandomProvider](
        className,
        Seq(
          classOf[DynamicAccess] -> dynamicAccess,
          classOf[PluginConfig]  -> pluginConfig
        )
      ) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException("Failed to initialize SecureRandomProvider", Some(ex))
    }
  }

  final class Default(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends SecureRandomProvider {
    override def create: SecureRandom = new SecureRandom()
  }
}
