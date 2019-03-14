# akka-persistence-dynamodb

[![CircleCI](https://circleci.com/gh/j5ik2o/akka-persistence-dynamodb/tree/master.svg?style=shield&circle-token=9f6f53d09f0fb87ee8ea81246e69683d668291cd)](https://circleci.com/gh/j5ik2o/akka-persistence-dynamodb/tree/master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.j5ik2o/akka-persistence-dynamodb_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.j5ik2o/akka-persistence-dynamodb_2.12)
[![Scaladoc](http://javadoc-badge.appspot.com/com.github.j5ik2o/akka-persistence-dynamodb_2.12.svg?label=scaladoc)](http://javadoc-badge.appspot.com/com.github.j5ik2o/akka-persistence-dynamodb_2.12/com/github/j5ik2o/akka/persistence/dynamodb/index.html?javadocio=true)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This plugin is derived from [dnvriend/akka-persistence-jdbc](https://github.com/dnvriend/akka-persistence-jdbc), not [akka/akka-persistence-dynamodb](https://github.com/akka/akka-persistence-dynamodb).

| Product | **j5ik2o/akka-persistence-dynamodb** | **[akka/akka-persistence-dynamodb](https://github.com/akka/akka-persistence-dynamodb)** | **[dnvriend/akka-persistence-jdbc](https://github.com/dnvriend/akka-persistence-jdbc)** |
|:-------:|:----:|:----:|:----:|
|DynamoDB support|✓|✓|-|
|I/O Sharding|✓|✓|-|
|Non-blocking I/O|✓|-|-|
|Journal Plugin|✓|✓|✓|
|Snapshot Plugin|✓|✓|✓|
|Query Plugin|✓|-|✓|

## Features

### DynamoDB Support

Supports non-blocking based [aws-sdk-java-v2](https://github.com/aws/aws-sdk-java-v2). 

### I/O Sharding

This plugin does a simple sharding to avoid the throttle of write on DynamoDB.

- Primary Index
  - Partition Key = ${PersistenceId}-${SequenceNumber % ShardCount}
  - Sort Key = ${SequenceNumber}

- GSI: GetJournalRows
  - PartitionKey = ${PersistenceId}
  - Sort Key = ${SequenceNumber}

By the way, akka/akka-persistence-dynamodb maybe has a heavy maintenance cost because it provides complicated sharding.

### Non-blocking

Implemented by akka-actor, akka-stream.

## Installation

Add the following to your sbt build (Scala 2.11.x, 2.12.x):

```scala
resolvers += "Sonatype OSS Release Repository" at "https://oss.sonatype.org/content/repositories/releases/"

val version = "..."

libraryDependencies += Seq(
  "com.github.j5ik2o" %% "akka-persistence-dynamodb" % version
)
```

## Depends on

- akka 2.5.x
- aws-sdk v2

## akka-persistence journal plugin

Just this, if you like the default settings.

```hocon
akka.persistence.journal.plugin = "dynamo-db-journal"
```

If overwrite the default values.

```hocon
akka.persistence.journal.plugin = "dynamo-db-journal"

dynamo-db-journal {
  class = "com.github.j5ik2o.akka.persistence.dynamodb.journal.DynamoDBJournal"
  plugin-dispatcher = "akka.actor.default-dispatcher"
  table-name = "Journal"
  get-journal-rows-index-name = "GetJournalRows"
  tag-separator = ","
  buffer-size = 1024
  parallelism = 32
  refresh-interval = 0.5 s
  soft-delete = true
  shard-count = 64
  columns-def {
    partition-key-column-name = "pkey"
    persistence-id-column-name = "persistence-id"
    sequence-nr-column-name = "sequence-nr"
    deleted-column-name = "deleted"
    message-column-name = "message"
    ordering-column-name = "ordering"
    tags-column-name = "tags"
  }
  dynamodb-client {
    max-concurrency = 128
    max-pending-connection-acquires = ?
    read-timeout = 3 s
    write-timeout = 3 s
    connection-timeout = 3 s
    connection-acquisition-timeout = 3 s
    connection-time-to-live = 3 s
    max-idle-connection-timeout = 3 s
    use-connection-reaper = true
    threads-of-event-loop-group = 32
    user-http2 = true
    max-http2-streams = 32
    batch-get-item-limit = 100
    batch-write-item-limit = 25
  }
  
}
```

## akka-persistence snapshot plugin

Just this, if you like the default settings.

```hocon
akka.persistence.snapshot-store.plugin = "dynamo-db-snapshot"
```

If overwrite the default values.

```hocon
akka.persistence.snapshot-store.plugin = "dynamo-db-snapshot"

dynamo-db-snapshot {
  table-name = "Snapshot"
  parallelism = 32
  refresh-interval = 0.5 s
  shard-count = 64
  columns-def {
    partition-key-column-name = "pkey"
    persistence-id-column-name = "persistence-id"
    sequence-nr-column-name = "sequence-nr"
    deleted-column-name = "deleted"
    message-column-name = "message"
    ordering-column-name = "ordering"
    tags-column-name = "tags"
  }
  dynamodb-client {
    max-concurrency = 128
    max-pending-connection-acquires = ?
    read-timeout = 3 s
    write-timeout = 3 s
    connection-timeout = 3 s
    connection-acquisition-timeout = 3 s
    connection-time-to-live = 3 s
    max-idle-connection-timeout = 3 s
    use-connection-reaper = true
    threads-of-event-loop-group = 32
    user-http2 = true
    max-http2-streams = 32
    batch-get-item-limit = 100
    batch-write-item-limit = 25
  }
}
```

## akka-persistence query plugin

Just this, if you like the default settings.

```hocon
dynamo-db-read-journal {
  class = "com.github.j5ik2o.akka.persistence.dynamodb.query.DynamoDBReadJournalProvider"
  write-plugin = "dynamo-db-journal"
}
```

If overwrite the default values.

```hocon
dynamo-db-read-journal {
  class = "com.github.j5ik2o.akka.persistence.dynamodb.query.DynamoDBReadJournalProvider"
  plugin-dispatcher = "akka.actor.default-dispatcher"
  write-plugin = "dynamo-db-journal"
  table-name = "Journal"
  get-journal-rows-index-name = "GetJournalRows"
  tag-separator = ","
  buffer-size = 1024
  parallelism = 32
  refresh-interval = 0.5 s
  soft-delete = true
  shard-count = 64
  columns-def {
    partition-key-column-name = "pkey"
    persistence-id-column-name = "persistence-id"
    sequence-nr-column-name = "sequence-nr"
    deleted-column-name = "deleted"
    message-column-name = "message"
    ordering-column-name = "ordering"
    tags-column-name = "tags"
  }
  dynamodb-client {
    max-concurrency = 128
    max-pending-connection-acquires = ?
    read-timeout = 3 s
    write-timeout = 3 s
    connection-timeout = 3 s
    connection-acquisition-timeout = 3 s
    connection-time-to-live = 3 s
    max-idle-connection-timeout = 3 s
    use-connection-reaper = true
    threads-of-event-loop-group = 32
    user-http2 = true
    max-http2-streams = 32
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

## License

Apache License

This product was made by duplicating or referring to the code of the following products, so Dennis Vriend's license is included in the product code and test code.

- [dnvriend/akka-persistence-jdbc](https://github.com/dnvriend/akka-persistence-jdbc)
- [dnvriend/akka-persistence-query-test](https://github.com/dnvriend/akka-persistence-query-test)


