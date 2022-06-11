package com.github.j5ik2o.akka.persistence.dynamodb.snapshot.dao

import akka.actor.ActorSystem
import akka.serialization.Serialization
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBAsync }
import com.github.j5ik2o.akka.persistence.dynamodb.metrics.MetricsReporter
import com.github.j5ik2o.akka.persistence.dynamodb.snapshot.config.SnapshotPluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.trace.TraceReporter

trait SnapshotDaoFactory {
  def create(
      system: ActorSystem,
      asyncClient: Option[AmazonDynamoDBAsync],
      syncClient: Option[AmazonDynamoDB],
      serialization: Serialization,
      pluginConfig: SnapshotPluginConfig,
      metricsReporter: Option[MetricsReporter],
      traceReporter: Option[TraceReporter]
  ): SnapshotDao
}
