package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig

object PluginConfig {

  val plugInLifecycleHandlerFactoryClassNameKey = "plug-in-lifecycle-handler-factory-class-name"
  val plugInLifecycleHandlerClassNameKey        = "plug-in-lifecycle-handler-class-name"
  val v1AsyncClientFactoryClassNameKey          = "v1-async-client-factory-class-name"
  val v1SyncClientFactoryClassNameKey           = "v1-sync-client-factory-class-name"
  val v1DaxAsyncClientFactoryClassNameKey       = "v1-dax-async-client-factory-class-name"
  val v1DaxSyncClientFactoryClassNameKey        = "v1-dax-sync-client-factory-class-name"
  val v2AsyncClientFactoryClassNameKey          = "v2-async-client-factory-class-name"
  val v2SyncClientFactoryClassNameKey           = "v2-sync-client-factory-class-name"
  val v2DaxAsyncClientFactoryClassNameKey       = "v2-dax-async-client-factory-class-name"
  val v2DaxSyncClientFactoryClassNameKey        = "v2-dax-sync-client-factory-class-name"

  val v1JournalRowWriteDriverFactoryClassNameKey    = "v1-journal-row-write-driver-factory-class-name"
  val v1DaxJournalRowWriteDriverFactoryClassNameKey = "v1-dax-journal-row-write-driver-factory-class-name"
  val v2JournalRowWriteDriverFactoryClassNameKey    = "v2-journal-row-write-driver-factory-class-name"
  val v2DaxJournalRowWriteDriverFactoryClassNameKey = "v2-dax-journal-row-write-driver-factory-class-name"

  val PlugInLifecycleHandlerFactoryClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.utils.PlugInLifecycleHandlerFactory"
  val PlugInLifecycleHandlerClassName  = "com.github.j5ik2o.akka.persistence.dynamodb.utils.PlugInLifecycleHandler"
  val V1AsyncClientFactoryClassName    = "com.github.j5ik2o.akka.persistence.dynamodb.utils.V1AsyncClientFactory"
  val V1SyncClientFactoryClassName     = "com.github.j5ik2o.akka.persistence.dynamodb.utils.V1SyncClientFactory"
  val V1DaxAsyncClientFactoryClassName = "com.github.j5ik2o.akka.persistence.dynamodb.utils.V1DaxAsyncClientFactory"
  val V1DaxSyncClientFactoryClassName  = "com.github.j5ik2o.akka.persistence.dynamodb.utils.V1DaxSyncClientFactory"
  val V2AsyncClientFactoryClassName    = "com.github.j5ik2o.akka.persistence.dynamodb.utils.V2AsyncClientFactory"
  val V2SyncClientFactoryClassName     = "com.github.j5ik2o.akka.persistence.dynamodb.utils.V2SyncClientFactory"
  val V2DaxAsyncClientFactoryClassName = "com.github.j5ik2o.akka.persistence.dynamodb.utils.V2DaxAsyncClientFactory"
  val V2DaxSyncClientFactoryClassName  = "com.github.j5ik2o.akka.persistence.dynamodb.utils.V2DaxSyncClientFactory"

  val JournalRowWriteDriverFactoryClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.journal.JournalRowWriteDriverFactory"
}

trait PluginConfig {
  val configRootPath: String
  val plugInLifecycleHandlerFactoryClassName: String
  val plugInLifecycleHandlerClassName: String
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
