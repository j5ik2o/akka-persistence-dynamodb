package com.github.j5ik2o.akka.persistence.dynamodb.client.v1

import akka.actor.DynamicAccess
import com.amazonaws.auth.AWSCredentialsProvider
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException

import scala.collection.immutable.Seq
import scala.util.{ Failure, Success }

trait AWSCredentialsProviderProvider {
  def create: Option[AWSCredentialsProvider]
}

object AWSCredentialsProviderProvider {
  def create(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): AWSCredentialsProviderProvider = {
    val className = pluginConfig.clientConfig.v1ClientConfig.awsCredentialsProviderProviderClassName
    dynamicAccess
      .createInstanceFor[AWSCredentialsProviderProvider](
        className,
        Seq(
          classOf[DynamicAccess] -> dynamicAccess,
          classOf[PluginConfig]  -> pluginConfig
        )
      ) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException("Failed to initialize AWSCredentialsProviderProvider", Some(ex))
    }
  }

  final class Default(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends AWSCredentialsProviderProvider {
    override def create: Option[AWSCredentialsProvider] = {
      val classNameOpt = pluginConfig.clientConfig.v1ClientConfig.awsCredentialsProviderClassName
      classNameOpt.map { className =>
        dynamicAccess
          .createInstanceFor[AWSCredentialsProvider](
            className,
            Seq(
              classOf[DynamicAccess] -> dynamicAccess,
              classOf[PluginConfig]  -> pluginConfig
            )
          ) match {
          case Success(value) => value
          case Failure(ex) =>
            throw new PluginException("Failed to initialize AWSCredentialsProvider", Some(ex))
        }
      }
    }
  }
}
