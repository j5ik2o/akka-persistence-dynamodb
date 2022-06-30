/*
 * Copyright 2019 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.journal

import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.text.DecimalFormat

final case class PartitionKey(private val value: String) {
  def asString: String = value
}

trait PartitionKeyResolver {

  def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey

}

trait PartitionKeyResolverProvider {

  def create: PartitionKeyResolver

}

object PartitionKeyResolverProvider {

  def create(pluginContext: JournalPluginContext): PartitionKeyResolverProvider = {
    val className = pluginContext.pluginConfig.partitionKeyResolverProviderClassName
    pluginContext.newDynamicAccessor[PartitionKeyResolverProvider]().createThrow(className)
  }

  final class Default(pluginContext: JournalPluginContext) extends PartitionKeyResolverProvider {

    override def create: PartitionKeyResolver = {
      val className = pluginContext.pluginConfig.partitionKeyResolverClassName
      pluginContext.newDynamicAccessor[PartitionKeyResolver]().createThrow(className)
    }

  }
}

object PartitionKeyResolver {

  final class SequenceNumberBased(pluginContext: JournalPluginContext) extends PartitionKeyResolver {
    import pluginContext._

    // ${persistenceId}-${sequenceNumber % shardCount}
    override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey = {
      val pkey = s"${persistenceId.asString}-${sequenceNumber.value % pluginConfig.shardCount}"
      PartitionKey(pkey)
    }

  }

  final class PersistenceIdBased(pluginContext: JournalPluginContext)
      extends PartitionKeyResolver
      with ToPersistenceIdOps {
    import pluginContext._

    override def separator: String =
      pluginConfig.sourceConfig.valueAs[String]("persistence-id-separator", PersistenceId.Separator)

    // ${persistenceId.prefix}-${md5(persistenceId.reverse) % shardCount}
    override def resolve(persistenceId: PersistenceId, sequenceNumber: SequenceNumber): PartitionKey = {
      val md5          = MessageDigest.getInstance("MD5")
      val df           = new DecimalFormat("0000000000000000000000000000000000000000")
      val bytes        = persistenceId.asString.reverse.getBytes(StandardCharsets.UTF_8)
      val hash         = BigInt(md5.digest(bytes))
      val mod          = (hash.abs % pluginConfig.shardCount) + 1
      val modelNameOpt = persistenceId.prefix
      val pkey = modelNameOpt match {
        case Some(modelName) =>
          "%s-%s".format(modelName, df.format(mod))
        case None => // fallback
          df.format(mod)
      }
      PartitionKey(pkey)
    }

  }

}
