# Customize

## Plugin dispatcher

Specify the dispatcher to be used inside the plugin. The default is "akka.actor.default-dispatcher".

### Journal

```
j5ik2o.dynamo-db-journal {
  plugin-dispatcher = "akka.actor.default-dispatcher"
}
```

### Snapshot

```
j5ik2o.dynamo-db-snapshot {
  plugin-dispatcher = "akka.actor.default-dispatcher"
}
```

### State

```
j5ik2o.dynamo-db-state {
  plugin-dispatcher = "akka.actor.default-dispatcher"
}
```

## Table names

Specify the table name. The default is the followings.

### Journal

```
j5ik2o.dynamo-db-journal {
  table-name = "Journal"
}
```

### Snapshot

```
j5ik2o.dynamo-db-snapshot {
  table-name = "Snapshot"
}
```

#### State

```
j5ik2o.dynamo-db-state {
  table-name = "State"
}
```

## Table column names

### Journal

```
j5ik2o.dynamo-db-journal {
  columns-def {
    partition-key-column-name = "pkey"
    sort-key-column-name = "skey"
    
    persistence-id-column-name = "persistence-id"
    sequence-nr-column-name = "sequence-nr"
    
    deleted-column-name = "deleted"
    message-column-name = "message"
    ordering-column-name = "ordering"
    tags-column-name = "tags"
  }
}
```

### Snapshot

```
j5ik2o.dynamo-db-snapshot {
  columns-def {
    partition-key-column-name = "pkey"
    sort-key-column-name = "skey"
    
    persistence-id-column-name = "persistence-id"
    sequence-nr-column-name = "sequence-nr"
    
    snapshot-column-name = "snapshot"
    created-column-name = "created"
  }
}
```

### State

```
j5ik2o.dynamo-db-state {
  columns-def {
    partition-key-column-name = "pkey"
    sort-key-column-name = "skey"
    
    persistence-id-column-name = "persistence-id"
    sequence-nr-column-name = "sequence-nr"
    
    deleted-column-name = "deleted"
    payload-column-name = "payload"
    serializer-id-column-name = "serializer-id"
    serializer-manifest-column-name = "serializer-manifest"
    ordering-column-name = "ordering" 
    tags-column-name = "tags"
  }
}
```

## Index names

### Journal

```
j5ik2o.dynamo-db-journal {
  get-journal-rows-index-name = "GetJournalRowsIndex"
}
```

### Snapshot

```
j5ik2o.dynamo-db-snapshot {
  get-snapshot-rows-index-name = "GetSnapshotRowsIndex"
}
```

### State

The state plugin has not the secondary index.

## Write-sharding

### Journal

```
j5ik2o.dynamo-db-journal {
  shard-count = 64
  partition-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver$PersistenceIdBased"
  sort-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.journal.SortKeyResolver$PersistenceIdWithSeqNr" 
}
```

#### Shard Count

`shard-count` is the logical number of the shards. the default value is `64`.

#### Partition Key Resolver Class Name

`partition-key-resolver-class-name` specifies the implementation class that generates `pkey` from `PersistenceId` and `Sequence Number`.
The following two implementations are available for built-in use. 

- `com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver.SequenceNumberBased` (Default)
  - `pkey = ${persistenceId}-${sequenceNumber % shardCount}`
  - The same `PersistenceId` will be assigned to a different shard if the `Sequence Number` is different. This is a write-specific sharding.
- `com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver.PersistenceIdBased`
  - `pkey = ${persistenceId.prefix}-${md5(persistenceId.reverse) % shardCount}`
  - e.g. `counter-875e6ce0425e4d2b8203f3b44b9b531a`, `persistenceId.prefix` is `counter`.
  - If you choose this option, the same shard will be assigned if the `PersistenceId` is the same, so be sure to select this option if you are using DynamoDB Streams or KDS for DynamoDB.

#### Sort Key Resolver Class Name
 
`sort-key-resolver-class-name` specifies the implementation class that generates `skey` from `PersistenceId` and `Sequence Number`.
The following two implementations are available for built-in use. 

- `com.github.j5ik2o.akka.persistence.dynamodb.journal.SortKeyResolver$SeqNr`
  - `skey = $sequenceNumber`
  - An implementation in which `pkey` is the `Sequence Number`.
- `com.github.j5ik2o.akka.persistence.dynamodb.journal.SortKeyResolver$PersistenceIdWithSeqNr`
  - `skey = ${persistenceId.body}-${sequenceNumber}`
  - e.g. `875e6ce0425e4d2b8203f3b44b9b531a`, `persistenceId.body` is `875e6ce0425e4d2b8203f3b44b9b531a`.
  - Use `persistenceId.body` as the prefix since `shard-count` may cause multiple `persistenceId`s events to be stored in the same shard.


#### Configure your own implementation

If you need, set up your own implementation.

```
j5ik2o.dynamo-db-journal {
  partition-key-resolver-class-name = "your class name(fqcn)"
  sort-key-resolver-class-name = "your class name(fqcn)"
}
```

### Snapshot

```
j5ik2o.dynamo-db-snapshot {
  shard-count = 64
  partition-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.snapshot.PartitionKeyResolver$PersistenceIdBased"
  sort-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.snapshot.SortKeyResolver$PersistenceIdWithSeqNr" 
}
```

#### Shard Count

`shard-count` is the logical number of shards. default value is `64`.

#### Partition Key Resolver Class Name

`partition-key-resolver-class-name` specifies the implementation class that generates `pkey` from `PersistenceId` and `Sequence Number`.
The following two implementations are available for built-in use. 

- `com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver.SequenceNumberBased` (Default)
  - `pkey =${persistenceId}-${sequenceNumber % shardCount}`
  - The same `PersistenceId` will be assigned to a different shard if the `Sequence Number` is different. This is a write-specific sharding.
- `com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver.PersistenceIdBased`
  - `pkey = ${persistenceId.prefix}-${md5(persistenceId.reverse) % shardCount}`
  - e.g. `counter-875e6ce0425e4d2b8203f3b44b9b531a`, `persistenceId.prefix` is `counter`.
  - If you choose this option, the same shard will be assigned if the `PersistenceId` is the same, so be sure to select this option if you are using DynamoDB Streams or KDS for DynamoDB.

#### Sort Key Resolver Class Name

`sort-key-resolver-class-name` specifies the implementation class that generates `skey` from `PersistenceId` and `Sequence Number`.
The following two implementations are available for built-in use.

- `com.github.j5ik2o.akka.persistence.dynamodb.journal.SortKeyResolver$SeqNr`
  - `skey = $sequenceNumber`
  - An implementation in which `pkey` is the `Sequence Number`.
- `com.github.j5ik2o.akka.persistence.dynamodb.journal.SortKeyResolver$PersistenceIdWithSeqNr`
  - `skey = ${persistenceId.body}-${sequenceNumber}`
  - e.g. `875e6ce0425e4d2b8203f3b44b9b531a`, `persistenceId.body` is `875e6ce0425e4d2b8203f3b44b9b531a`.
  - Use `persistenceId.body` as the prefix since `shard-count` may cause multiple `persistenceId`s events to be stored in the same shard.

#### Configure your own implementation
    
If you need, set up your own implementation.

```
j5ik2o.dynamo-db-snapshot {
  partition-key-resolver-class-name = "your class name(fqcn)"
  sort-key-resolver-class-name = "your class name(fqcn)"
}
```

```{admonition} Persistent data images

| persistenceId                            | sequence-nr | pkey(SequenceNumberBased)                  | skey(SeqNr)         |
| :--------------------------------------- | ----------: | :----------------------------------------- | :------------------ |
| counter-875e6ce0425e4d2b8203f3b44b9b531a |           1 | counter-875e6ce0425e4d2b8203f3b44b9b531a-1 | 0000000000000000001 |
| counter-875e6ce0425e4d2b8203f3b44b9b531a |           2 | counter-875e6ce0425e4d2b8203f3b44b9b531a-2 | 0000000000000000002 |
| counter-875e6ce0425e4d2b8203f3b44b9b531a |           3 | counter-875e6ce0425e4d2b8203f3b44b9b531a-3 | 0000000000000000003 |
| counter-875e6ce0425e4d2b8203f3b44b9b531a |           4 | counter-875e6ce0425e4d2b8203f3b44b9b531a-4 | 0000000000000000004 |
| counter-875e6ce0425e4d2b8203f3b44b9b531a |           5 | counter-875e6ce0425e4d2b8203f3b44b9b531a-5 | 0000000000000000005 |

#### 

| persistenceId                            | sequence-nr | pkey(PersistenceIdBased)                         | skey(PersistenceIdWithSeqNr)                         |
| :--------------------------------------- | ----------: | :----------------------------------------------- | :--------------------------------------------------- |
| counter-875e6ce0425e4d2b8203f3b44b9b531a |           1 | counter-0000000000000000000000000000000000000803 | 875e6ce0425e4d2b8203f3b44b9b531a-0000000000000000001 |
| counter-875e6ce0425e4d2b8203f3b44b9b531a |           2 | counter-0000000000000000000000000000000000000803 | 875e6ce0425e4d2b8203f3b44b9b531a-0000000000000000002 |
| counter-875e6ce0425e4d2b8203f3b44b9b531a |           3 | counter-0000000000000000000000000000000000000803 | 875e6ce0425e4d2b8203f3b44b9b531a-0000000000000000003 |
| counter-875e6ce0425e4d2b8203f3b44b9b531a |           4 | counter-0000000000000000000000000000000000000803 | 875e6ce0425e4d2b8203f3b44b9b531a-0000000000000000004 |
| counter-875e6ce0425e4d2b8203f3b44b9b531a |           5 | counter-0000000000000000000000000000000000000803 | 875e6ce0425e4d2b8203f3b44b9b531a-0000000000000000005 |
| counter-a8d46579bc2f4caf8c3b8dc2db984227 |           1 | counter-0000000000000000000000000000000000000803 | a8d46579bc2f4caf8c3b8dc2db984227-0000000000000000001 |
| counter-a8d46579bc2f4caf8c3b8dc2db984227 |           2 | counter-0000000000000000000000000000000000000803 | a8d46579bc2f4caf8c3b8dc2db984227-0000000000000000002 |
| counter-a8d46579bc2f4caf8c3b8dc2db984227 |           3 | counter-0000000000000000000000000000000000000803 | a8d46579bc2f4caf8c3b8dc2db984227-0000000000000000003 |
| counter-a8d46579bc2f4caf8c3b8dc2db984227 |           4 | counter-0000000000000000000000000000000000000803 | a8d46579bc2f4caf8c3b8dc2db984227-0000000000000000004 |
| counter-a8d46579bc2f4caf8c3b8dc2db984227 |           5 | counter-0000000000000000000000000000000000000803 | a8d46579bc2f4caf8c3b8dc2db984227-0000000000000000005 |

```

### State

The state plugin has not the sort-key and SortKeyResolver.

```
j5ik2o.dynamo-db-state {
  shard-count = 64
  partition-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.state.PartitionKeyResolver$PersistenceIdBased"
}
```

#### Shard Count

`shard-count` is the logical number of shards. default value is `64`.

#### Partition Key Resolver Class Name

There are one standard implementations as follows.

- `com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver.PersistenceIdBased`
  - This is the same implementation as Journal or Snapshot.

#### Configure your own implementation

If you need, set up your own implementation.

```
j5ik2o.dynamo-db-state {
  partition-key-resolver-class-name = "your class name(fqcn)"
}
```

## AWS Client

### AWS Client Factory

The following factories are used to create AWS Clients inside the plugins.

| key                                                           | version | async/sync | default value                                                                     |
|:--------------------------------------------------------------|:-------:|:----------:|:----------------------------------------------------------------------------------|
| j5ik2o.dynamo-db-?????.v2-async-client-factory-class-name     |   v2    |   async    | com.github.j5ik2o.akka.persistence.dynamodb.utils.V2AsyncClientFactory$Default    |
| j5ik2o.dynamo-db-?????.v2-sync-client-factory-class-name      |   v2    |    sync    | com.github.j5ik2o.akka.persistence.dynamodb.utils.V2SyncClientFactory$Default     |
| j5ik2o.dynamo-db-?????.v2-dax-async-client-factory-class-name | v2-dax  |   async    | com.github.j5ik2o.akka.persistence.dynamodb.utils.V2DaxAsyncClientFactory$Default |
| j5ik2o.dynamo-db-?????.v2-dax-sync-client-factory-class-name  | v2-dax  |    sync    | com.github.j5ik2o.akka.persistence.dynamodb.utils.V2DaxSyncClientFactory$Default  |
| j5ik2o.dynamo-db-?????.v1-async-client-factory-class-name     |   v1    |   async    | com.github.j5ik2o.akka.persistence.dynamodb.utils.V1AsyncClientFactory$Default    |
| j5ik2o.dynamo-db-?????.v1-sync-client-factory-class-name      |   v1    |    sync    | com.github.j5ik2o.akka.persistence.dynamodb.utils.V1SyncClientFactory$Default     |
| j5ik2o.dynamo-db-?????.v1-dax-async-client-factory-class-name | v1-dax  |   async    | com.github.j5ik2o.akka.persistence.dynamodb.utils.V1DaxAsyncClientFactory$Default |
| j5ik2o.dynamo-db-?????.v1-dax-sync-client-factory-class-name  | v1-dax  |    sync    | com.github.j5ik2o.akka.persistence.dynamodb.utils.V1DaxSyncClientFactory$Default  |

The AWS Clients generation process is automatic if you specify the set values, but if you wish to apply your own generation process, please do the following.

This plugin is useful for specifying the configuration items to the client that are not supported by this plugin, or for dynamically specifying configuration items within the program.

```scala V2 AWS Client Factory
package example

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import com.github.j5ik2o.akka.persistence.dynamodb.utils.V2AsyncClientFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

class MyV2AsyncClientFactory extends V2AsyncClientFactory {
  override def create(
      dynamicAccess: DynamicAccess,
      pluginConfig: PluginConfig
  ): DynamoDbAsyncClient = {
    // Default Implementation is the following.
    // V2ClientUtils.createV2AsyncClient(dynamicAccess, pluginConfig)
    
    // e.g. Custom AWS Client Initialization
    V2ClientBuilderUtils
      .setupAsync(
        dynamicAccess,
        pluginConfig
      )
      .defaultsMode(DefaultsMode.AUTO) // This is not a configuration item in the plugin.
      .build()
  }
}
```

```
j5ik2o.dynamo-db-journal.v2-async-client-factory-class-name = "example.MyV2AsyncClientFactory"
```

### AWS Client Config

The configurations for AWS Clients is specified in the following sections(`j5ik2o.dynamodb-db-?????.dynamo-db-client`).

```
j5ik2o.dynamo-db-journal {
  dynamo-db-client {
    access-key-id = "x"
    secret-access-key = "x"
    endpoint = "http://localhost:8000/"
  }
}
```

- `access-key-id` is Access Key ID
- `secret-access-key` is Secret Access Key
- `endpoint` is an endpoint to DynamoDB

### AWS CredentialsProvider

You can specify your own `AwsCredentialsProvider`.

`aws-credentials-provider-provider-class-name` can specify the provider that generate `AwsCredentialsProvider`.
`aws-credentials-provider-class-name` can specify the implementation class name of `AwsCredentialsProvider`.

```
j5ik2o.dynamo-db-????? {
  dynamo-db-client {
    v2 {
      aws-credentials-provider-provider-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.client.v2.AwsCredentialsProviderProvider$Default"
      aws-credentials-provider-class-name = "software.amazon.awssdk.auth.credentials.AwsCredentialsProvider"
    }
  }
}
```

#### Custom AWSCredentialsProviderProvider by using `aws-credentials-provider-provider-class-name`

```scala
package example

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.client.v2.AwsCredentialsProviderProvider
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import software.amazon.awssdk.auth.credentials.{ AwsCredentialsProvider, WebIdentityTokenFileCredentialsProvider }

final class MyAwsCredentialsProviderProvider(@unused dynamicAccess: DynamicAccess, @unused pluginConfig: PluginConfig)
  extends AwsCredentialsProviderProvider {

  override def create: Option[AwsCredentialsProvider] = {
    if (sys.env.contains("AWS_ROLE_ARN"))
      Some(WebIdentityTokenFileCredentialsProvider.create())
    else
      None
  }

}
```

```
j5ik2o.dynamo-db-????? {
  v2 {
    aws-credentials-provider-provider-class-name = "exampe.MyAwsCredentialsProviderProvider"
  }
}
```

#### Custom AWSCredentialsProvider by using `aws-credentials-provider-class-name`

```scala
package example

import akka.actor.DynamicAccess
import software.amazon.awssdk.auth.credentials.{ AwsCredentialsProvider, AwsCredentials }
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig

class MyAwsCredentialsProvider(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig) extends AwsCredentialsProvider {
  override def resolveCredentials(): AwsCredentials = {
    // Custom AWS Credentials
  }
}
```

Specify your custom implementation class to `aws-credentials-provider-class-name`.

```
j5ik2o.dynamo-db-????? {
  v2 {
    aws-credentials-provider-class-name = "exampe.MyAwsCredentialsProvider"
  }
}
```

### MetricPublisher 

Using the MetricPublisher in the V2 sdk, metrics at the SDK level can be sent to Datadog and Newrelic via Kamon.

```scala
package example

import akka.actor.DynamicAccess
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import kamon.Kamon
import kamon.metric.{ Counter, Histogram, MeasurementUnit }
import software.amazon.awssdk.core.metrics.CoreMetric
import software.amazon.awssdk.metrics.{ MetricCollection, MetricPublisher }

import scala.annotation.unused
import scala.jdk.StreamConverters.StreamHasToScala

class MyMetricPublisher(
                           @unused dynamicAccess: DynamicAccess,
                           pluginConfig: PluginConfig
                         ) extends MetricPublisher {

  override def publish(metricCollection: MetricCollection): Unit = {
    val metricsMap = metricCollection.stream().toScala(Vector).map { mr => (mr.metric().name(), mr) }.toMap
    metricsMap(CoreMetric.OPERATION_NAME.name()).value() match {
      case "PutItem" =>
        if (metricsMap(CoreMetric.API_CALL_SUCCESSFUL.name()).value().asInstanceOf[java.lang.Boolean]) {
          val apiCallDuration = metricsMap(CoreMetric.API_CALL_DURATION.name()).value().asInstanceOf[java.time.Duration]
          val credentialsFetchDuration =
            metricsMap(CoreMetric.CREDENTIALS_FETCH_DURATION.name()).value().asInstanceOf[java.time.Duration]
          val marshallingDuration =
            metricsMap(CoreMetric.MARSHALLING_DURATION.name()).value().asInstanceOf[java.time.Duration]
          val retryCount = metricsMap(CoreMetric.RETRY_COUNT.name()).value().asInstanceOf[java.lang.Integer]
          putItemApiCallDurationHistogram.record(apiCallDuration.toMillis)
          putItemCredentialsFetchDurationHistogram.record(credentialsFetchDuration.toMillis)
          putItemMarshallingDurationHistogram.record(marshallingDuration.toMillis)
          putItemRetryCounter.increment(retryCount.toLong)
        } else {
          putItemErrorCounter.increment()
        }
      case _ =>
    }

  }

  override def close(): Unit = {}

  private val prefix                            = "app"
  private val apiCallDurationHistogram          = histogram(s"$prefix.aws.api-call-duration")
  private val credentialsFetchDurationHistogram = histogram(s"$prefix.aws.credentials-fetch-duration")
  private val marshallingDurationHistogram      = histogram(s"$prefix.aws.marshalling-duration")
  private val retryCounter                      = counter(s"$prefix.aws.retry-count")
  private val errorCounter                      = counter(s"$prefix.error-count")

  private val putItemApiCallDurationHistogram = apiCallDurationHistogram.withTag("Operation", "PutItem")
  private val putItemCredentialsFetchDurationHistogram = credentialsFetchDurationHistogram
    .withTag("Operation", "PutItem")
  private val putItemMarshallingDurationHistogram = marshallingDurationHistogram
    .withTag("Operation", "PutItem")
  private val putItemRetryCounter = retryCounter
    .withTag("Operation", "PutItem")
  private val putItemErrorCounter = errorCounter
    .withTag("Operation", "PutItem")

  private def histogram(metricName: String): Histogram =
    Kamon
      .histogram(metricName, MeasurementUnit.time.milliseconds)
      .withTag("TableName", pluginConfig.tableName)
      .withTag("sdk", s"java-${pluginConfig.clientConfig.clientVersion}")

  private def counter(metricName: String): Counter =
    Kamon
      .counter(metricName)
      .withTag("TableName", pluginConfig.tableName)
      .withTag("sdk", s"java-${pluginConfig.clientConfig.clientVersion}")
}
```

Specify your custom implementation classes to `metric-publisher-class-names`.

```
j5ik2o.dynamo-db-????? {
  v2 {
    metric-publisher-class-names = ["exampe.MyMetricPublisher"]
  }
}
```

