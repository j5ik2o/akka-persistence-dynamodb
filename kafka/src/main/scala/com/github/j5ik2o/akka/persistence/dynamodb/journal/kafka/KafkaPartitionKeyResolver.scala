package com.github.j5ik2o.akka.persistence.dynamodb.journal.kafka

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException
import com.github.j5ik2o.akka.persistence.dynamodb.model.{ PersistenceId, SequenceNumber }

import scala.collection.immutable.Seq
import scala.util.{ Failure, Success }

trait KafkaPartitionKeyResolver {

  def create(
      persistenceId: PersistenceId,
      sequenceNumber: SequenceNumber,
      partitionSize: KafkaPartitionSize
  ): KafkaPartitionKey
}

object KafkaPartitionKeyResolver {

  final class Default(kafkaWriteAdaptorConfig: KafkaWriteAdaptorConfig) extends KafkaPartitionKeyResolver {

    override def create(
        persistenceId: PersistenceId,
        sequenceNumber: SequenceNumber,
        partitionSize: KafkaPartitionSize
    ): KafkaPartitionKey = {
      val result = math.abs(persistenceId.##) % partitionSize.value
      KafkaPartitionKey(result)
    }
  }

}

trait KafkaPartitionKeyResolverProvider {
  def create: KafkaPartitionKeyResolver
}

object KafkaPartitionKeyResolverProvider {

  def create(
      dynamicAccess: DynamicAccess,
      kafkaWriteAdaptorConfig: KafkaWriteAdaptorConfig
  ): KafkaPartitionKeyResolverProvider = {
    val className = kafkaWriteAdaptorConfig.partitionKeyResolverProviderClassName
    dynamicAccess
      .createInstanceFor[KafkaPartitionKeyResolverProvider](
        className,
        Seq(
          classOf[DynamicAccess]           -> dynamicAccess,
          classOf[KafkaWriteAdaptorConfig] -> kafkaWriteAdaptorConfig
        )
      ) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException("Failed to initialize KafkaPartitionKeyResolverProvider", Some(ex))
    }
  }

  final class Default(
      dynamicAccess: DynamicAccess,
      kafkaWriteAdaptorConfig: KafkaWriteAdaptorConfig
  ) extends KafkaPartitionKeyResolverProvider {

    override def create: KafkaPartitionKeyResolver = {
      val className = kafkaWriteAdaptorConfig.partitionKeyResolverClassName
      val args =
        Seq(classOf[KafkaWriteAdaptorConfig] -> kafkaWriteAdaptorConfig)
      dynamicAccess
        .createInstanceFor[KafkaPartitionKeyResolver](
          className,
          args
        ) match {
        case Success(value) => value
        case Failure(ex) =>
          throw new PluginException("Failed to initialize KafkaPartitionKeyResolver", Some(ex))
      }
    }

  }

}
