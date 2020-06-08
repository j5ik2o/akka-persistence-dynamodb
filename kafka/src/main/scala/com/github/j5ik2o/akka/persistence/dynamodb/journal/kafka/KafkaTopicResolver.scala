package com.github.j5ik2o.akka.persistence.dynamodb.journal.kafka

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.exception.PluginException
import com.github.j5ik2o.akka.persistence.dynamodb.journal.{ PersistenceId, SequenceNumber, ToPersistenceIdOps }

import scala.collection.immutable.Seq
import scala.util.{ Failure, Success }

trait KafkaTopicResolver {

  def create(persistenceId: PersistenceId): KafkaTopic

}

object KafkaTopicResolver {

  final class Default(kafkaWriteAdaptorConfig: KafkaWriteAdaptorConfig)
      extends KafkaTopicResolver
      with ToPersistenceIdOps {

    override def separator: String = kafkaWriteAdaptorConfig.persistenceIdSeparator

    override def create(
        persistenceId: PersistenceId
    ): KafkaTopic = {
      KafkaTopic(persistenceId.prefix.getOrElse(throw new IllegalArgumentException("invalid persistenceId format")))
    }

  }
}

trait KafkaTopicResolverProvider {

  def create: KafkaTopicResolver

}

object KafkaTopicResolverProvider {

  def create(
      dynamicAccess: DynamicAccess,
      kafkaWriteAdaptorConfig: KafkaWriteAdaptorConfig
  ): KafkaTopicResolverProvider = {
    val className = kafkaWriteAdaptorConfig.topicResolverProviderClassName
    dynamicAccess
      .createInstanceFor[KafkaTopicResolverProvider](
        className,
        Seq(
          classOf[DynamicAccess]           -> dynamicAccess,
          classOf[KafkaWriteAdaptorConfig] -> kafkaWriteAdaptorConfig
        )
      ) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new PluginException("Failed to initialize KafkaTopicResolverProvider", Some(ex))
    }
  }

  final class Default(
      dynamicAccess: DynamicAccess,
      kafkaWriteAdaptorConfig: KafkaWriteAdaptorConfig
  ) extends KafkaTopicResolverProvider {

    override def create: KafkaTopicResolver = {
      val className = kafkaWriteAdaptorConfig.topicResolverClassName
      val args =
        Seq(classOf[KafkaWriteAdaptorConfig] -> kafkaWriteAdaptorConfig)
      dynamicAccess
        .createInstanceFor[KafkaTopicResolver](
          className,
          args
        ) match {
        case Success(value) => value
        case Failure(ex) =>
          throw new PluginException("Failed to initialize KafkaTopicResolver", Some(ex))
      }
    }

  }

}
