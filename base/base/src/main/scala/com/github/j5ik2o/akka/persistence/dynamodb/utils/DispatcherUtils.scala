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

import akka.NotUsed
import akka.stream.ActorAttributes
import akka.stream.scaladsl.Flow
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.ClientVersion
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext

import scala.concurrent.ExecutionContextExecutorService

object DispatcherUtils extends LoggingSupport {

  def newV1Executor(pluginContext: PluginContext): ExecutionContextExecutorService = {
    import pluginContext._
    DispatcherUtils
      .getV1DispatcherName(pluginConfig).map { dn =>
        val ec = system.dispatchers.lookup(dn)
        ExecutorServiceUtils.fromExecutionContext(ec)
      }.getOrElse(ExecutorServiceUtils.fromExecutionContext(system.dispatcher))
  }

  def newV2Executor(pluginContext: PluginContext): ExecutionContextExecutorService = {
    import pluginContext._
    DispatcherUtils
      .getV2DispatcherName(pluginConfig).map { dn =>
        val ec = system.dispatchers.lookup(dn)
        ExecutorServiceUtils.fromExecutionContext(ec)
      }.getOrElse(ExecutorServiceUtils.fromExecutionContext(system.dispatcher))
  }
  implicit class ApplyV1DispatcherOps[A, B](private val flow: Flow[A, B, NotUsed]) extends AnyVal {

    def withV1Dispatcher(pluginConfig: PluginConfig): Flow[A, B, NotUsed] =
      applyV1Dispatcher(pluginConfig, flow)
  }

  def getV1DispatcherName(pluginConfig: PluginConfig): Option[String] = {
    pluginConfig.clientConfig.clientVersion match {
      case ClientVersion.V1 =>
        pluginConfig.clientConfig.v1ClientConfig.dispatcherName
      case ClientVersion.V1Dax =>
        pluginConfig.clientConfig.v1DaxClientConfig.dispatcherName
      case _ =>
        throw new IllegalArgumentException("Invalid the client version")
    }
  }

  def getV2DispatcherName(pluginConfig: PluginConfig): Option[String] = pluginConfig.clientConfig.clientVersion match {
    case ClientVersion.V2 =>
      pluginConfig.clientConfig.v2ClientConfig.dispatcherName.orElse(
        pluginConfig.clientConfig.v2ClientConfig.syncClientConfig.dispatcherName
      )
    case _ =>
      throw new IllegalArgumentException("Invalid the client version")
  }

  private def applyV1Dispatcher[A, B](pluginConfig: PluginConfig, flow: Flow[A, B, NotUsed]): Flow[A, B, NotUsed] = {
    getV1DispatcherName(pluginConfig).fold(
      flow
    ) { name => flow.withAttributes(ActorAttributes.dispatcher(name)) }
  }

  implicit class ApplyV2DispatcherOps[A, B](private val flow: Flow[A, B, NotUsed]) extends AnyVal {

    def withV2Dispatcher(pluginConfig: PluginConfig): Flow[A, B, NotUsed] =
      applyV2Dispatcher(pluginConfig, flow)
  }

  private def applyV2Dispatcher[A, B](pluginConfig: PluginConfig, flow: Flow[A, B, NotUsed]): Flow[A, B, NotUsed] = {
    getV2DispatcherName(pluginConfig).fold(
      flow
    ) { name => flow.withAttributes(ActorAttributes.dispatcher(name)) }
  }

}
