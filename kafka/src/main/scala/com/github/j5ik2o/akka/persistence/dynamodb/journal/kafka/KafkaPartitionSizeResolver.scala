package com.github.j5ik2o.akka.persistence.dynamodb.journal.kafka

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException

import scala.collection.immutable._
import scala.util.{ Failure, Success }

trait KafkaPartitionSizeResolver {
  def create: KafkaPartitionSize
}

object KafkaPartitionSizeResolver {

  final class Default(kafkaWriteAdaptorConfig: KafkaWriteAdaptorConfig) extends KafkaPartitionSizeResolver {
    private val partitionSize: Int = kafkaWriteAdaptorConfig.sourceConfig.getInt("partition-size")

    override def create: KafkaPartitionSize = KafkaPartitionSize(partitionSize)
  }

}

trait KafkaPartitionSizeResolverProvider {
  def create: KafkaPartitionSizeResolver
}

object KafkaPartitionSizeResolverProvider {

  def create(
      dynamicAccess: DynamicAccess,
      kafkaWriteAdaptorConfig: KafkaWriteAdaptorConfig
  ): KafkaPartitionSizeResolverProvider = {
    val className = kafkaWriteAdaptorConfig.partitionSizeResolverProviderClassName
    dynamicAccess
      .createInstanceFor[KafkaPartitionSizeResolverProvider](
        className,
        Seq(
          classOf[DynamicAccess]           -> dynamicAccess,
          classOf[KafkaWriteAdaptorConfig] -> kafkaWriteAdaptorConfig
        )
      ) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException("Failed to initialize KafkaPartitionSizeResolverProvider", Some(ex))
    }
  }

  final class Default(
      dynamicAccess: DynamicAccess,
      kafkaWriteAdaptorConfig: KafkaWriteAdaptorConfig
  ) extends KafkaPartitionSizeResolverProvider {

    override def create: KafkaPartitionSizeResolver = {
      val className = kafkaWriteAdaptorConfig.partitionSizeResolverClassName
      val args =
        Seq(classOf[KafkaWriteAdaptorConfig] -> kafkaWriteAdaptorConfig)
      dynamicAccess
        .createInstanceFor[KafkaPartitionSizeResolver](
          className,
          args
        ) match {
        case Success(value) => value
        case Failure(ex) =>
          throw new PluginException("Failed to initialize KafkaPartitionSizeResolver", Some(ex))
      }
    }

  }

}
