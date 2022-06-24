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

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientVersion
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import com.github.j5ik2o.akka.persistence.dynamodb.utils.DynamicAccessUtils
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

trait AwsCredentialsProviderProvider {
  def create: Option[AwsCredentialsProvider]
}

object AwsCredentialsProviderProvider {
  def create(pluginContext: PluginContext): AwsCredentialsProviderProvider = {
    import pluginContext._
    val className =
      pluginConfig.clientConfig.clientVersion match {
        case ClientVersion.V2 =>
          pluginConfig.clientConfig.v2ClientConfig.awsCredentialsProviderProviderClassName
        case ClientVersion.V2Dax =>
          pluginConfig.clientConfig.v2DaxClientConfig.awsCredentialsProviderProviderClassName
      }
    DynamicAccessUtils.createInstanceFor_CTX_Throw[AwsCredentialsProviderProvider, PluginContext](
      className,
      pluginContext
    )
  }

  final class Default(pluginContext: PluginContext) extends AwsCredentialsProviderProvider {
    override def create: Option[AwsCredentialsProvider] = {
      import pluginContext._
      val classNameOpt = pluginConfig.clientConfig.clientVersion match {
        case ClientVersion.V2 =>
          pluginConfig.clientConfig.v2ClientConfig.awsCredentialsProviderClassName
        case ClientVersion.V2Dax =>
          pluginConfig.clientConfig.v2DaxClientConfig.awsCredentialsProviderClassName
      }
      classNameOpt.map { className =>
        DynamicAccessUtils.createInstanceFor_CTX_Throw[AwsCredentialsProvider, PluginContext](className, pluginContext)
      }
    }
  }
}
