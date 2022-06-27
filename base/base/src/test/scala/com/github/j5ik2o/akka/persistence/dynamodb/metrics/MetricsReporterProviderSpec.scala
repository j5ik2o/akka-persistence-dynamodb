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
package com.github.j5ik2o.akka.persistence.dynamodb.metrics

import akka.actor.{ ActorSystem, DynamicAccess }
import akka.testkit.TestKit
import com.github.j5ik2o.akka.persistence.dynamodb.PluginTestUtils
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.context.PluginContext
import org.scalatest.freespec.AnyFreeSpecLike

class MR_None() extends MetricsReporter
class MR_PC(pluginConfig: PluginConfig) extends MetricsReporter
class MR_DA(dynamicAccess: DynamicAccess) extends MetricsReporter
class MR_DA_PC(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends MetricsReporter
class MR_CTX(pluginContext: PluginContext) extends MetricsReporter

class MetricsReporterProviderSpec extends TestKit(ActorSystem("MetricsReporterProvider")) with AnyFreeSpecLike {

  "MetricsReporterProvider" - {
    "create None" in {
      val p = MetricsReporterProvider.create(PluginTestUtils.newPluginContext(system, Some(classOf[MR_None]), None))
      val r = p.create
      assert(r.isDefined)
    }
    "create PC" in {
      val p = MetricsReporterProvider.create(PluginTestUtils.newPluginContext(system, Some(classOf[MR_PC]), None))
      val r = p.create
      assert(r.isDefined)
    }
    "create DA" in {
      val p = MetricsReporterProvider.create(PluginTestUtils.newPluginContext(system, Some(classOf[MR_DA]), None))
      val r = p.create
      assert(r.isDefined)
    }
    "create DA PC" in {
      val p = MetricsReporterProvider.create(PluginTestUtils.newPluginContext(system, Some(classOf[MR_DA_PC]), None))
      val r = p.create
      assert(r.isDefined)
    }
    "create CTX" in {
      val p = MetricsReporterProvider.create(PluginTestUtils.newPluginContext(system, Some(classOf[MR_CTX]), None))
      val r = p.create
      assert(r.isDefined)
    }
  }

}
