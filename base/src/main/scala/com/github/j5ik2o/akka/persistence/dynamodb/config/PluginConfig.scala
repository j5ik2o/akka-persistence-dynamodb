package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig

object PluginConfig {

  val v1AsyncClientFactoryClassNameKey    = "v1-async-client-factory-class-name"
  val v1SyncClientFactoryClassNameKey     = "v1-sync-client-factory-class-name"
  val v1DaxAsyncClientFactoryClassNameKey = "v1-dax-async-client-factory-class-name"
  val v1DaxSyncClientFactoryClassNameKey  = "v1-dax-sync-client-factory-class-name"
  val v2AsyncClientFactoryClassNameKey    = "v2-async-client-factory-class-name"
  val v2SyncClientFactoryClassNameKey     = "v2-sync-client-factory-class-name"

  val DefaultV1AsyncClientFactoryClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.utils.V1AsyncClientFactory$Default"
  val DefaultV1SyncClientFactoryClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.utils.V1SyncClientFactory$Default"
  val DefaultV1DaxAsyncClientFactoryClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.utils.V1DaxAsyncClientFactory$Default"
  val DefaultV1DaxSyncClientFactoryClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.utils.V1DaxSyncClientFactory$Default"
  val DefaultV2AsyncClientFactoryClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.utils.V2AsyncClientFactory$Default"
  val DefaultV2SyncClientFactoryClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.utils.V2SyncClientFactory$Default"
}

trait PluginConfig {
  val configRootPath: String
  val v1AsyncClientFactoryClassName: String
  val v1SyncClientFactoryClassName: String
  val v1DaxAsyncClientFactoryClassName: String
  val v1DaxSyncClientFactoryClassName: String
  val v2AsyncClientFactoryClassName: String
  val v2SyncClientFactoryClassName: String
  val tableName: String
  val metricsReporterProviderClassName: String
  val metricsReporterClassName: Option[String]
  val traceReporterProviderClassName: String
  val traceReporterClassName: Option[String]
  val clientConfig: DynamoDBClientConfig
}
