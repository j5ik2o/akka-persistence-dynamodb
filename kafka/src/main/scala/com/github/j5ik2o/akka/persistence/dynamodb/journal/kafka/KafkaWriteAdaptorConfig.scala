package com.github.j5ik2o.akka.persistence.dynamodb.journal.kafka

import com.github.j5ik2o.akka.persistence.dynamodb.journal.PersistenceId
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

object KafkaWriteAdaptorConfig {

  def fromConfig(config: Config): KafkaWriteAdaptorConfig = {
    KafkaWriteAdaptorConfig(
      sourceConfig = config,
      persistenceIdSeparator = config.getOrElse[String]("persistence-id-separator", PersistenceId.Separator),
      passThrough = config.getOrElse[Boolean]("pass-through", false),
      partitionKeyResolverType = config
        .getAs[String]("partition-key-resolver-type").map(PartitionKeyResolverType.withName).getOrElse(
          PartitionKeyResolverType.Auto
        ),
      topicResolverProviderClassName = config
        .getOrElse[String]("topic-resolver-provider-class-name", classOf[KafkaTopicResolverProvider.Default].getName),
      topicResolverClassName = config
        .getOrElse[String]("topic-resolver-class-name", classOf[KafkaTopicResolver.Default].getName),
      partitionKeyResolverProviderClassName = config.getOrElse[String](
        "partition-key-resolver-provider-class-name",
        classOf[KafkaPartitionKeyResolverProvider.Default].getName
      ),
      partitionKeyResolverClassName = config
        .getOrElse[String]("partition-key-resolver-class-name", classOf[KafkaPartitionKeyResolver.Default].getName),
      partitionSizeResolverProviderClassName = config.getOrElse[String](
        "partition-size-resolver-provider-class-name",
        classOf[KafkaPartitionSizeResolverProvider.Default].getName
      ),
      partitionSizeResolverClassName = config
        .getOrElse[String]("partition-size-resolver-class-name", classOf[KafkaPartitionSizeResolver.Default].getName)
    )
  }
}

case class KafkaWriteAdaptorConfig(
    sourceConfig: Config,
    persistenceIdSeparator: String,
    passThrough: Boolean,
    partitionKeyResolverType: PartitionKeyResolverType.Value,
    topicResolverProviderClassName: String,
    topicResolverClassName: String,
    partitionKeyResolverProviderClassName: String,
    partitionKeyResolverClassName: String,
    partitionSizeResolverProviderClassName: String,
    partitionSizeResolverClassName: String
)
