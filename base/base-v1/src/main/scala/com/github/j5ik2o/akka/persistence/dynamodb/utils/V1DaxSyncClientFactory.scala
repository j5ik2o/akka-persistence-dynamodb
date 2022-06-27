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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext

trait V1DaxSyncClientFactory {

  def create: AmazonDynamoDB

}

object V1DaxSyncClientFactory {

  class Default(pluginContext: PluginContext) extends V1DaxSyncClientFactory {
    override def create: AmazonDynamoDB = {
      V1ClientUtils.createV1DaxSyncClient(
        pluginContext,
        pluginContext.pluginConfig.configRootPath
      )
    }
  }

}
