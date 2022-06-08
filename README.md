# akka-persistence-dynamodb

[![CI](https://github.com/j5ik2o/akka-persistence-dynamodb/workflows/CI/badge.svg)](https://github.com/j5ik2o/akka-persistence-dynamodb/actions?query=workflow%3ACI)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.j5ik2o/akka-persistence-dynamodb-journal_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.j5ik2o/akka-persistence-dynamodb-journal_2.13)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fj5ik2o%2Fakka-persistence-dynamodb.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fj5ik2o%2Fakka-persistence-dynamodb?ref=badge_shield)

akka-persistence-dynamodb writes journal and snapshot entries to DynamoDB. It implements the full akka-persistence-query API and is therefore very useful for implementing DDD-style application models using Akka and Scala for creating reactive applications.

## Genealogy of plugins

This plugin is derived from [akka/akka-persistence-jdbc](https://github.com/akka/akka-persistence-jdbc), not [akka/akka-persistence-dynamodb](https://github.com/akka/akka-persistence-dynamodb).

It was impossible to change [akka/akka-persistence-dynamodb](https://github.com/akka/akka-persistence-dynamodb) to support aws-sdk v2. Because it is a complex structure. As a simpler solution, I decided to derive from [akka/akka-persistence-jdbc](https://github.com/akka/akka-persistence-jdbc).

## Supported versions:

- Java: `1.8+`
- Scala: `2.12.x` or `2.13.x` 
- Akka: `2.5.x`(Scala 2.11 only), `2.6.x`(Scala 2.12, 2.13)
- AWS-SDK: `1.x`, `DAX`, `2.x`

## Features

| Product | **j5ik2o/akka-persistence-dynamodb** | **[akka/akka-persistence-dynamodb](https://github.com/akka/akka-persistence-dynamodb)** | **[akka/akka-persistence-jdbc](https://github.com/dnvriend/akka-persistence-jdbc)** |
|:-------:|:----:|:----:|:----:|
|DynamoDB support|✓|✓|-|
|Write Sharding|✓|✓|-|
|Non-blocking I/O|✓|-|-|
|back pressure|✓|-|-|
|AWS Client Version| v1,dax,v2 | v1 | - |
|Journal Plugin|✓|✓|✓|
|Snapshot Plugin|✓|✓|✓|
|State Plugin|✓|-|✓|
|Query Plugin|✓|-|✓|


### DynamoDB Support

- [aws-sdk-java-v2](https://github.com/aws/aws-sdk-java-v2).
- [aws-sdk-java-v1](https://github.com/aws/aws-sdk-java).
- [Amazon DynamoDB Accelerator(DAX) Client](https://aws.amazon.com/jp/dynamodb/dax/)

### Write Sharding

This plugin supports a simple sharding to avoid the throttle of write on DynamoDB.

- Primary Index(for Events writing, default is pattern-1)
  - pattern-1 is `sequenceNumber` based write sharding (com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver.SequenceNumberBased)
    - Partition Key = ${persistenceId}-${sequenceNumber % shardCount}
    - Sort Key = ${sequenceNumber}
  - pattern-2 is `persistenceId` based write sharding (com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver.PersistenceIdBased)
    - Partition Key = ${persistenceId.prefix}-${md5(persistenceId.reverse) % shardCount}
      - `persistenceId.prefix` is the first part of the `persistenceId` is delimiter-separated.
    - Sort Key = ${persistenceId.body}-${sequenceNumber}
      - `persistenceId.body` is the last part of the `persistenceId` is delimiter-separated.
    
If you want to load events chronologically in DynamoDB Streams, choose pattern-2.

```hocon
j5ik2o.dynamo-db-journal {
  partition-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver$PersistenceIdBased"
}
```

- GSI: GetJournalRows(for Actor replaying)
  - PartitionKey = ${persistenceId}
  - Sort Key = ${sequenceNumber}

By the way, `akka/akka-persistence-dynamodb` maybe has a heavy maintenance cost because it provides complicated sharding.

### AWS SDK

- The AWS SDK is supported for both V1 and V2(non-blocking I/O). Support to DAX enabled by using V1
- The other logic implemented by akka-actor, akka-stream also non-blocking and async.

## Installation

Add the following to your sbt build (2.12.x, 2.13.x):

```scala
val version = "..."

libraryDependencies += Seq(
  "com.github.j5ik2o" %% "akka-persistence-dynamodb-journal" % version,
  "com.github.j5ik2o" %% "akka-persistence-dynamodb-snapshot" % version
)
```

## akka-persistence journal plugin

Just this, if you like the default settings.

```hocon
akka.persistence.journal.plugin = "j5ik2o.dynamo-db-journal"
```

If overwrite the default values.

```hocon
akka.persistence.journal.plugin = "j5ik2o.dynamo-db-journal"

j5ik2o.dynamo-db-journal {
  class = "com.github.j5ik2o.akka.persistence.dynamodb.journal.DynamoDBJournal"
  plugin-dispatcher = "akka.actor.default-dispatcher"
 
  # Enable the following line if you want to read the deprecated legacy format configuration file.
  # Once you have verified that it works, please migrate to the new format configuration file.
  # legacy-config-layout = true 

  table-name = "Journal"
  get-journal-rows-index-name = "GetJournalRows"
  persistence-id-separator = "-"
  tag-separator = ","
  shard-count = 2
  partition-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver$Default"
  sort-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.journal.SortKeyResolver$Default" 
  queue-enable = true
  queue-buffer-size = 1024
  queue-overflow-strategy = "Fail"
  queue-parallelism = 32
  write-parallelism = 32
  soft-delete = true
  query-batch-size = 1024
  replay-batch-size = 512
  consistent-read = false
  
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

  dynamo-db-client {
    # access-key-id = ???
    # secret-access-key = ???
    # endpoint = ???
    # region = ???
    client-version = "v2"
    client-type = "async"
    v2 {
      # dispatcher-name = ""
      async {
        max-concurrency = 50
        max-pending-connection-acquires = 10000
        read-timeout = 30s
        write-timeout = 30s
        connection-timeout = 2s
        connection-acquisition-timeout = 3s
        connection-time-to-live = 0s
        max-idle-connection-timeout = 60s
        use-connection-reaper = true
        threads-of-event-loop-group = 32
        use-http2 = false
        http2-max-streams = 4294967295
        http2-initial-window-size = 1048576
      }
      sync {
        socket-timeout = 50s
        connection-timeout = 2s
        connection-acquisition-timeout = 10s
        max-connections = 50
        connection-time-to-live = 0s
        max-idle-connection-timeout = 60s
        use-connection-reaper = true
      }
      # retry-mode = ???
      # api-call-timeout = ???
      # api-call-attempt-timeout = ???
    }
    v1 {
      # dispatcher-name = ""
      connection-timeout = 10000 ms
      # max-error-retry = ???
      # retry-policy-class-name = ???
      max-connections = 50
      throttle-retries = true
      # local-address = ???
      # protocol = ???
      socket-timeout = 50000 ms
      request-timeout = 0s
      client-execution-timeout = 0s
      # user-agent-prefix = ???
      # user-agent-suffix = ???
      use-reaper = true
      use-gzip = false
      # socket-send-buffer-size-hint = ???
      # socket-receive-buffer-size-hint = ???
      # signer-override = ???
      response-metadata-cache-size = 50
      # dns-resolver-class-name = ???
      use-expect-contine = true
      cache-response-metadata = true
      # connection-ttl = ???
      connection-max-idle = 60000 ms
      validate-after-inactivity = 5000
      tcp-keep-alive = false
      max-consecutive-retries-before-throttling = 100
      # disable-host-prefix-injection = ???
      # retry-mode = ???
    }
    v1-dax {
      # dispatcher-name = ""
      connection-timeout = 1000 ms
      request-timeout = 60000 ms
      health-check-timeout = 1000 ms
      health-check-interval = 5000 ms
      idle-connection-timeout = 3000 ms
      min-idle-connection-size = 1
      write-retries = 2
      max-pending-connections-per-host = 10
      read-retries = 2
      thread-keep-alive = 10000 ms
      cluster-update-interval = 4000 ms
      cluster-update-threshold = 125 ms
      max-retry-delay = 7000 ms
      unhealthy-consecutive-error-count = 5
    } 
    batch-get-item-limit = 100
    batch-write-item-limit = 25
  }

}
```

### Important changes

The sort-key(skey) column has been added to the journal table. In the default implementation, this column is set to sequenceNumber, but different implementations do not necessarily store sequence-number. You can flexibly design pkey and skey for write sharding.
If you want to use a legacy column layout, you can configure the following. To create a table, please refer to `tools/legacy-journal-table.json`.
**In legacy column layout, Never use `PartitionKeyResolver.PersistenceIdBased` because it is incompatible.**

```hocon
j5ik2o.dynamo-db-journal {
  columns-def.sort-key-column-name = "sequence-nr" # override default 'skey'
}

j5ik2o.dynamo-db-read-journal {
  columns-def.sort-key-column-name = "sequence-nr" # override default 'skey'
}
```

## akka-persistence snapshot plugin

Just this, if you like the default settings.

```hocon
akka.persistence.snapshot-store.plugin = "j5ik2o.dynamo-db-snapshot"
```

If overwrite the default values.

```hocon
akka.persistence.snapshot-store.plugin = "j5ik2o.dynamo-db-snapshot"

j5ik2o.dynamo-db-snapshot {
  table-name = "Snapshot"

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

  dynamo-db-client {
    # access-key-id = ???
    # secret-access-key = ???
    # endpoint = ???
    # region = ???
    client-version = "v2"
    client-type = "async"
    v2 {
      # dispatcher-name = ""
      async {
        max-concurrency = 50
        max-pending-connection-acquires = 10000
        read-timeout = 30s
        write-timeout = 30s
        connection-timeout = 2s
        connection-acquisition-timeout = 3s
        connection-time-to-live = 0s
        max-idle-connection-timeout = 60s
        use-connection-reaper = true
        threads-of-event-loop-group = 32
        use-http2 = false
        http2-max-streams = 4294967295
        http2-initial-window-size = 1048576
      }
      sync {
        socket-timeout = 50s
        connection-timeout = 2s
        connection-acquisition-timeout = 10s
        max-connections = 50
        connection-time-to-live = 0s
        max-idle-connection-timeout = 60s
        use-connection-reaper = true
      }
      # retry-mode = ???
      # api-call-timeout = ???
      # api-call-attempt-timeout = ???
    }
    v1 {
      # dispatcher-name = ""
      connection-timeout = 10000 ms
      # max-error-retry = ???
      # retry-policy-class-name = ???
      max-connections = 50
      throttle-retries = true
      # local-address = ???
      # protocol = ???
      socket-timeout = 50000 ms
      request-timeout = 0s
      client-execution-timeout = 0s
      # user-agent-prefix = ???
      # user-agent-suffix = ???
      use-reaper = true
      use-gzip = false
      # socket-send-buffer-size-hint = ???
      # socket-receive-buffer-size-hint = ???
      # signer-override = ???
      response-metadata-cache-size = 50
      # dns-resolver-class-name = ???
      use-expect-contine = true
      cache-response-metadata = true
      # connection-ttl = ???
      connection-max-idle = 60000 ms
      validate-after-inactivity = 5000
      tcp-keep-alive = false
      max-consecutive-retries-before-throttling = 100
      # disable-host-prefix-injection = ???
      # retry-mode = ???
    }
    v1-dax {
      # dispatcher-name = ""
      connection-timeout = 1000 ms
      request-timeout = 60000 ms
      health-check-timeout = 1000 ms
      health-check-interval = 5000 ms
      idle-connection-timeout = 3000 ms
      min-idle-connection-size = 1
      write-retries = 2
      max-pending-connections-per-host = 10
      read-retries = 2
      thread-keep-alive = 10000 ms
      cluster-update-interval = 4000 ms
      cluster-update-threshold = 125 ms
      max-retry-delay = 7000 ms
      unhealthy-consecutive-error-count = 5
    } 
    batch-get-item-limit = 100
    batch-write-item-limit = 25
  }
}
```

## akka-persistence state plugin

Just this, if you like the default settings.

```hocon
akka.persistence.state.plugin = "j5ik2o.dynamo-db-state"
```

If overwrite the default values.

```hocon
akka.persistence.state.plugin = "j5ik2o.dynamo-db-state"

j5ik2o.dynamo-db-state {
  table-name = "State"
  
  persistence-id-separator = "-"
  tag-separator = ","
  shard-count = 64
  
  partition-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.state.PartitionKeyResolver$Default"
  table-name-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.state.TableNameResolver$Default"

  dynamo-db-client {
    # access-key-id = ???
    # secret-access-key = ???
    # endpoint = ???
    # region = ???
    client-version = "v2"
    client-type = "async"
    v2 {
      # dispatcher-name = ""
      async {
        max-concurrency = 50
        max-pending-connection-acquires = 10000
        read-timeout = 30s
        write-timeout = 30s
        connection-timeout = 2s
        connection-acquisition-timeout = 3s
        connection-time-to-live = 0s
        max-idle-connection-timeout = 60s
        use-connection-reaper = true
        threads-of-event-loop-group = 32
        use-http2 = false
        http2-max-streams = 4294967295
        http2-initial-window-size = 1048576
      }
      sync {
        socket-timeout = 50s
        connection-timeout = 2s
        connection-acquisition-timeout = 10s
        max-connections = 50
        connection-time-to-live = 0s
        max-idle-connection-timeout = 60s
        use-connection-reaper = true
      }
      # retry-mode = ???
      # api-call-timeout = ???
      # api-call-attempt-timeout = ???
    }
    v1 {
      # dispatcher-name = ""
      connection-timeout = 10000 ms
      # max-error-retry = ???
      # retry-policy-class-name = ???
      max-connections = 50
      throttle-retries = true
      # local-address = ???
      # protocol = ???
      socket-timeout = 50000 ms
      request-timeout = 0s
      client-execution-timeout = 0s
      # user-agent-prefix = ???
      # user-agent-suffix = ???
      use-reaper = true
      use-gzip = false
      # socket-send-buffer-size-hint = ???
      # socket-receive-buffer-size-hint = ???
      # signer-override = ???
      response-metadata-cache-size = 50
      # dns-resolver-class-name = ???
      use-expect-contine = true
      cache-response-metadata = true
      # connection-ttl = ???
      connection-max-idle = 60000 ms
      validate-after-inactivity = 5000
      tcp-keep-alive = false
      max-consecutive-retries-before-throttling = 100
      # disable-host-prefix-injection = ???
      # retry-mode = ???
    }
    v1-dax {
      # dispatcher-name = ""
      connection-timeout = 1000 ms
      request-timeout = 60000 ms
      health-check-timeout = 1000 ms
      health-check-interval = 5000 ms
      idle-connection-timeout = 3000 ms
      min-idle-connection-size = 1
      write-retries = 2
      max-pending-connections-per-host = 10
      read-retries = 2
      thread-keep-alive = 10000 ms
      cluster-update-interval = 4000 ms
      cluster-update-threshold = 125 ms
      max-retry-delay = 7000 ms
      unhealthy-consecutive-error-count = 5
    } 
    batch-get-item-limit = 100
    batch-write-item-limit = 25
  }
}
```

## akka-persistence query plugin

Just this, if you like the default settings.

```hocon
j5ik2o.dynamo-db-read-journal {
  class = "com.github.j5ik2o.akka.persistence.dynamodb.query.DynamoDBReadJournalProvider"
  write-plugin = "j5ik2o.dynamo-db-journal"
}
```

If overwrite the default values.

```hocon
j5ik2o.dynamo-db-read-journal {
  class = "com.github.j5ik2o.akka.persistence.dynamodb.query.DynamoDBReadJournalProvider"
  plugin-dispatcher = "akka.actor.default-dispatcher"
  write-plugin = "j5ik2o.dynamo-db-journal"
  table-name = "Journal"
  tags-index-name = "TagsIndex"
  get-journal-rows-index-name = "GetJournalRows"
  tag-separator = ","
  shard-count = 2
  refresh-interval = 0.5 s
  query-batch-size = 1024
  consistent-read = false
  
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

  dynamo-db-client {
    # access-key-id = ???
    # secret-access-key = ???
    # endpoint = ???
    # region = ???
    client-version = "v2"
    client-type = "async"
    v2 {
      # dispatcher-name = ""
      async {
        max-concurrency = 50
        max-pending-connection-acquires = 10000
        read-timeout = 30s
        write-timeout = 30s
        connection-timeout = 2s
        connection-acquisition-timeout = 3s
        connection-time-to-live = 0s
        max-idle-connection-timeout = 60s
        use-connection-reaper = true
        threads-of-event-loop-group = 32
        use-http2 = false
        http2-max-streams = 4294967295
        http2-initial-window-size = 1048576
      }
      sync {
        socket-timeout = 50s
        connection-timeout = 2s
        connection-acquisition-timeout = 10s
        max-connections = 50
        connection-time-to-live = 0s
        max-idle-connection-timeout = 60s
        use-connection-reaper = true
      }
      # retry-mode = ???
      # api-call-timeout = ???
      # api-call-attempt-timeout = ???
    }
    v1 {
      # dispatcher-name = ""
      connection-timeout = 10000 ms
      # max-error-retry = ???
      # retry-policy-class-name = ???
      max-connections = 50
      throttle-retries = true
      # local-address = ???
      # protocol = ???
      socket-timeout = 50000 ms
      request-timeout = 0s
      client-execution-timeout = 0s
      # user-agent-prefix = ???
      # user-agent-suffix = ???
      use-reaper = true
      use-gzip = false
      # socket-send-buffer-size-hint = ???
      # socket-receive-buffer-size-hint = ???
      # signer-override = ???
      response-metadata-cache-size = 50
      # dns-resolver-class-name = ???
      use-expect-contine = true
      cache-response-metadata = true
      # connection-ttl = ???
      connection-max-idle = 60000 ms
      validate-after-inactivity = 5000
      tcp-keep-alive = false
      max-consecutive-retries-before-throttling = 100
      # disable-host-prefix-injection = ???
      # retry-mode = ???
    }
    v1-dax {
      # dispatcher-name = ""
      connection-timeout = 1000 ms
      request-timeout = 60000 ms
      health-check-timeout = 1000 ms
      health-check-interval = 5000 ms
      idle-connection-timeout = 3000 ms
      min-idle-connection-size = 1
      write-retries = 2
      max-pending-connections-per-host = 10
      read-retries = 2
      thread-keep-alive = 10000 ms
      cluster-update-interval = 4000 ms
      cluster-update-threshold = 125 ms
      max-retry-delay = 7000 ms
      unhealthy-consecutive-error-count = 5
    } 
    batch-get-item-limit = 100
    batch-write-item-limit = 25
  }

}
```

```scala
val readJournal : ReadJournal
  with CurrentPersistenceIdsQuery
  with PersistenceIdsQuery
  with CurrentEventsByPersistenceIdQuery
  with EventsByPersistenceIdQuery
  with CurrentEventsByTagQuery
  with EventsByTagQuery = PersistenceQuery(system).readJournalFor(DynamoDBReadJournal.Identifier)
```

## DynamoDB setup

Assuming the default values are used (adjust as necessary if not):

| type | name | partition key | sort key | comments |
|:----:|:----:|:--------------|:---------|:---------|
|table | Journal | `pkey` (String) | `skey` (String) | Provision capacity as necessary for your application. |
|index | GetJournalRowsIndex (GSI) | `persistence-id` (String) | `sequence-nr` (Number) | Index for replaying actors. |
|index | TagsIndex (GSI) | `tags` (String) | - | Index for queries using tags. |
|table | Snapshot | `persistence-id` (String) | `sequence-nr` (Number) | No indices necessary. |


As the access to the DynamoDB instance is via the AWS Java SDK, use the methods for the SDK, which are documented at [docs.aws.amazon.com](https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/credentials.html)

## License

Apache License Version 2.0

This product was made by duplicating or referring to the code of the following products, so Dennis Vriend's license is included in the product code and test code.

- [akka/akka-persistence-jdbc](https://github.com/dnvriend/akka-persistence-jdbc)
- [dnvriend/akka-persistence-query-test](https://github.com/dnvriend/akka-persistence-query-test)

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fj5ik2o%2Fakka-persistence-dynamodb.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fj5ik2o%2Fakka-persistence-dynamodb?ref=badge_large)
