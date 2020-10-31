package com.github.j5ik2o.akka.persistence.dynamodb.client.v2

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException
import software.amazon.awssdk.metrics.MetricPublisher

import scala.collection.immutable.Seq
import scala.util.{ Failure, Success }

trait MetricPublishersProvider {
  def create: Seq[MetricPublisher]
}

object MetricPublishersProvider {

  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): MetricPublishersProvider = {
    val className = pluginConfig.clientConfig.v2ClientConfig.metricPublishersProviderClassName
    dynamicAccess
      .createInstanceFor[MetricPublishersProvider](
        className,
        Seq(
          classOf[DynamicAccess] -> dynamicAccess,
          classOf[PluginConfig]  -> pluginConfig
        )
      ) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException("Failed to initialize CsmConfigurationProviderProvider", Some(ex))
    }
  }

  final class Default(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends MetricPublishersProvider {

    override def create: Seq[MetricPublisher] = {
      val classNames = pluginConfig.clientConfig.v2ClientConfig.metricPublisherClassNames
      classNames.map { className =>
        dynamicAccess
          .createInstanceFor[MetricPublisher](
            className,
            Seq(
              classOf[DynamicAccess] -> dynamicAccess,
              classOf[PluginConfig]  -> pluginConfig
            )
          ) match {
          case Success(value) => value
          case Failure(ex) =>
            throw new PluginException("Failed to initialize MetricPublisher", Some(ex))
        }
      }
    }
  }

}
