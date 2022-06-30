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

import com.amazonaws.auth.AWSCredentialsProvider
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientVersion
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext

trait AWSCredentialsProviderProvider {
  def create: Option[AWSCredentialsProvider]
}

object AWSCredentialsProviderProvider {
  def create(pluginContext: PluginContext): AWSCredentialsProviderProvider = {
    import pluginContext._
    val className = pluginConfig.clientConfig.clientVersion match {
      case ClientVersion.V1 =>
        pluginConfig.clientConfig.v1ClientConfig.awsCredentialsProviderProviderClassName
      case ClientVersion.V1Dax =>
        pluginConfig.clientConfig.v1DaxClientConfig.awsCredentialsProviderProviderClassName
    }
    pluginContext.newDynamicAccessor[AWSCredentialsProviderProvider]().createThrow(className)
  }

  final class Default(pluginContext: PluginContext) extends AWSCredentialsProviderProvider {
    override def create: Option[AWSCredentialsProvider] = {
      val classNameOpt = pluginContext.pluginConfig.clientConfig.v1ClientConfig.awsCredentialsProviderClassName
      classNameOpt.map { className =>
        pluginContext.newDynamicAccessor[AWSCredentialsProvider]().createThrow(className)
      }
    }
  }
}
