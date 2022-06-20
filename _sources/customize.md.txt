# Customize

## Modify Plugin dispatcher

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

## Modify the table name

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

## Modify the table column names

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

## Modify the index name

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

## Modify Write sharding

### Journal

```
j5ik2o.dynamo-db-journal {
  shard-count = 64
  partition-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver$Default"
  sort-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.journal.SortKeyResolver$Default" 
}
```

shard-count is the logical number of shards.

There are two standard implementations as follows. You may also set up your own implementation.

- `com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver.SequenceNumberBased` (Default)
  - The same `PersistenceId` will be assigned to a different shard if the `Sequence Number` is different. This is a write-specific sharding.
- `com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver.PersistenceIdBased`
  - If you choose this option, the same shard will be assigned if the `PersistenceId` is the same, so be sure to select this option if you are using DynamoDB Stream or KDS for DynamoDB.

### Snapshot

```
j5ik2o.dynamo-db-snapshot {
  shard-count = 64
  partition-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.snapshot.PartitionKeyResolver$Default"
  sort-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.snapshot.SortKeyResolver$Default" 
}
```

`shard-count` is the logical number of shards.

There are two standard implementations as follows. You may also set up your own implementation.

- `com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver.SequenceNumberBased` (Default)
  - The same `PersistenceId` will be assigned to a different shard if the `Sequence Number` is different. This is a write-specific sharding.
- `com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver.PersistenceIdBased`
  - If you choose this option, the same shard will be assigned if the `PersistenceId` is the same, so be sure to select this option if you are using DynamoDB Stream or KDS for DynamoDB.

```{admonition} Data images

#### SequenceNumberBased

| persistenceId                            | sequence-nr | pkey                                       | skey                                                 |
|:-----------------------------------------|------------:|:-------------------------------------------|:-----------------------------------------------------|
| counter-875e6ce0425e4d2b8203f3b44b9b531a |           1 | counter-875e6ce0425e4d2b8203f3b44b9b531a-1 | 875e6ce0425e4d2b8203f3b44b9b531a-0000000000000000001 |
| counter-875e6ce0425e4d2b8203f3b44b9b531a |           2 | counter-875e6ce0425e4d2b8203f3b44b9b531a-2 | 875e6ce0425e4d2b8203f3b44b9b531a-0000000000000000002 |
| counter-875e6ce0425e4d2b8203f3b44b9b531a |           3 | counter-875e6ce0425e4d2b8203f3b44b9b531a-3 | 875e6ce0425e4d2b8203f3b44b9b531a-0000000000000000003 |
| counter-875e6ce0425e4d2b8203f3b44b9b531a |           4 | counter-875e6ce0425e4d2b8203f3b44b9b531a-4 | 875e6ce0425e4d2b8203f3b44b9b531a-0000000000000000004 |
| counter-875e6ce0425e4d2b8203f3b44b9b531a |           5 | counter-875e6ce0425e4d2b8203f3b44b9b531a-5 | 875e6ce0425e4d2b8203f3b44b9b531a-0000000000000000005 |

#### PersistenceIdBased

| persistenceId                            | sequence-nr | pkey                                             | skey                                                 |
|:-----------------------------------------|------------:|:-------------------------------------------------|:-----------------------------------------------------|
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
  partition-key-resolver-class-name = "com.github.j5ik2o.akka.persistence.dynamodb.state.PartitionKeyResolver$Default"
}
```

shard-count is the logical number of shards.

There are one standard implementations as follows. You may also set up your own implementation.

- `com.github.j5ik2o.akka.persistence.dynamodb.journal.PartitionKeyResolver.PersistenceIdBased`
  - This is the same implementation as Journal or Snapshot.
