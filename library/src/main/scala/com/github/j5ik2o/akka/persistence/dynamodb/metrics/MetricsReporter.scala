package com.github.j5ik2o.akka.persistence.dynamodb.metrics

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig

import scala.concurrent.duration.Duration

import scala.collection.immutable._

trait MetricsReporter {
  def setDynamoDBClientPutItemDuration(duration: Duration): Unit
  def setDynamoDBClientBatchWriteItemDuration(duration: Duration): Unit
  def setDynamoDBClientUpdateItemDuration(duration: Duration): Unit
  def setDynamoDBClientDeleteItemDuration(duration: Duration): Unit
  def setDynamoDBClientQueryDuration(duration: Duration): Unit
  def setDynamoDBClientScanDuration(duration: Duration): Unit
}

object MetricsReporter {

  class None(pluginConfig: PluginConfig) extends MetricsReporter {
    override def setDynamoDBClientPutItemDuration(duration: Duration): Unit        = {}
    override def setDynamoDBClientBatchWriteItemDuration(duration: Duration): Unit = {}
    override def setDynamoDBClientUpdateItemDuration(duration: Duration): Unit     = {}
    override def setDynamoDBClientDeleteItemDuration(duration: Duration): Unit     = {}
    override def setDynamoDBClientQueryDuration(duration: Duration): Unit          = {}
    override def setDynamoDBClientScanDuration(duration: Duration): Unit           = {}
  }

}

trait MetricsReporterProvider {

  def create: Option[MetricsReporter]

}

object MetricsReporterProvider {

  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): MetricsReporterProvider = {
    val className = pluginConfig.metricsReporterProviderClassName
    dynamicAccess
      .createInstanceFor[MetricsReporterProvider](
        className,
        Seq(classOf[DynamicAccess] -> dynamicAccess, classOf[PluginConfig] -> pluginConfig)
      ).getOrElse(throw new ClassNotFoundException(className))
  }

  final class Default(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends MetricsReporterProvider {

    def create: Option[MetricsReporter] = {
      pluginConfig.metricsReporterClassName.map { className =>
        dynamicAccess
          .createInstanceFor[MetricsReporter](
            className,
            Seq(classOf[PluginConfig] -> pluginConfig)
          ).getOrElse(throw new ClassNotFoundException(className))
      }
    }

  }
}
