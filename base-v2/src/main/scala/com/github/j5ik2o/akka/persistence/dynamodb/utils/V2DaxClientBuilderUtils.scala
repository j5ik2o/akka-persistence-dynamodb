package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import software.amazon.dax.{ ClusterDaxAsyncClient, ClusterDaxClient }

object V2DaxClientBuilderUtils {

  def setupAsync(
      dynamicAccess: DynamicAccess,
      pluginConfig: PluginConfig
  ): ClusterDaxAsyncClient.Builder = {
    val builder = ClusterDaxAsyncClient.builder()
    val configuration = V2DaxOverrideConfigurationBuilderUtils
      .setup(dynamicAccess, pluginConfig)
      .build()
    builder.overrideConfiguration(configuration)
    builder
  }

  def setupSync(
      dynamicAccess: DynamicAccess,
      pluginConfig: PluginConfig
  ): ClusterDaxClient.Builder = {
    val builder = ClusterDaxClient.builder()
    val configuration = V2DaxOverrideConfigurationBuilderUtils
      .setup(dynamicAccess, pluginConfig)
      .build()
    builder.overrideConfiguration(configuration)
    builder
  }

}
