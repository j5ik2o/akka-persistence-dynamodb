# Getting started

## Installation

Add the following to your sbt build (2.12.x, 2.13.x):

```{note}
The plugin uses the AWS SDK to access DynamoDB. The plugin has separate modules for each AWS SDK, so please decide on the version of the module you want to use. We recommend the module for AWS SDK v2.
```

- if you use event sourced

```scala
val version = "..."
val awsClientVersion = "v2" // or "v1"

libraryDependencies += Seq(
  "com.github.j5ik2o" %% s"akka-persistence-dynamodb-journal-$awsClientVersion" % version,
  "com.github.j5ik2o" %% s"akka-persistence-dynamodb-snapshot-$awsClientVersion" % version
)
```

```{figure} ./images/tag-list.png
:height: 250px
:name: tag-list

Choose from the tag list in the Github repository
```

The Journal and Snapshot plugins are independent and can be used separately. For example, you can use DynamoDB for Journal and another plugin for Snapshot. And vice versa.

- if you use durable state

```scala
val version = "..."
val awsClientVersion = "v2" // or "v1"

libraryDependencies += Seq(
  "com.github.j5ik2o" %% s"akka-persistence-dynamodb-state-$awsClientVersion" % version
)
```

```{note}
Since version 1.7, the plugin have been split by AWS SDK version. There are no changes to the configuration file specifications. Currently used configuration files can be used as they are.
```

## Configurations

### Journal Plugin

#### Create Table

Create the table using the JSON file under `tools` directory.

```shell
$ aws dynamodb create-table --cli-input-json file://./tools/journal-table.json
```

#### Preparing for application.conf

Please add the following settings to your `application.conf`.

```
akka.persistence.journal.plugin = "j5ik2o.dynamo-db-journal"
```

If you use the v1 module, the following configuration is required.

```
j5ik2o.dynamo-db-journal {
  dynamo-db-client.client-version = "v1"
}
```

If you want to use DynamoDB Local, do the following

```
j5ik2o.dynamo-db-journal {
  dynamo-db-client {
    access-key-id = "x"
    secret-access-key = "x"
    endpoint = "http://localhost:8000/"
  }
}
```

```{warning}
Do not use the above method in the production environment. Be sure to use IAM Role without specifying the above parameters.
```

### Snapshot Store Plugin

#### Create Table

Create the table using the JSON file under `tools` directory.

```shell
$ aws dynamodb create-table --cli-input-json file://./tools/snapshot-table.json
```

```{note}
The snapshot table format has changed since 1.9. If you want to use the old table format, set `j5ik2o.dynamo-db-snapshot.legacy-table-format` to true.
```

#### Preparing for application.conf

Please add the following settings to your `application.conf`.

```
akka.persistence.snapshot-store.plugin = "j5ik2o.dynamo-db-snapshot"
```

If you use the v1 module, the following configuration is required.

```
j5ik2o.dynamo-db-snapshot {
  dynamo-db-client.client-version = "v1"
}
```

If you want to use DynamoDB Local, do the following

```
j5ik2o.dynamo-db-snapshot {
  dynamo-db-client {
    access-key-id = "x"
    secret-access-key = "x"
    endpoint = "http://localhost:8000/"
  }
}
```

```{warning}
Do not use the above method in the production environment. Be sure to use IAM Role without specifying the above parameters.
```

### State Store Plugin

#### Create Table

Create the table using the JSON file under `tools` directory.

```shell
$ aws dynamodb create-table --cli-input-json file://./tools/state-table.json
```

#### Preparing for application.conf

Please add the following settings to your `application.conf`.

```
akka.persistence.state.plugin = "j5ik2o.dynamo-db-state"
```

If you use the v1 module, the following configuration is required.

```
j5ik2o.dynamo-db-state {
  dynamo-db-client.client-version = "v1"
}
```

If you want to use DynamoDB Local, do the following

```
j5ik2o.dynamo-db-state {
  dynamo-db-client {
    access-key-id = "x"
    secret-access-key = "x"
    endpoint = "http://localhost:8000/"
  }
}
```

```{warning}
Do not use the above method in the production environment. Be sure to use IAM Role without specifying the above parameters.
```

## Persistent Actor Implementation

See below for an example implementation of a persistent actor by using akka typed. Note that akka classic is not recommended.

- [Event Sourced](https://github.com/j5ik2o/akka-persistence-dynamodb/tree/main/example/src/main/scala/com/github/j5ik2o/akka/persistence/dynamodb/example/typed/eventsourced)
- [Durable State](https://github.com/j5ik2o/akka-persistence-dynamodb/tree/main/example/src/main/scala/com/github/j5ik2o/akka/persistence/dynamodb/example/typed/durablestate)

Please also see [the official documentation](https://doc.akka.io/docs/akka/current/typed/persistence.html).

## Configurations for the plugin

Please refer to the following for the setting items of each plugin.

- [Journal Plugin - reference.conf](https://github.com/j5ik2o/akka-persistence-dynamodb/blob/main/journal/journal-base/src/main/resources/reference.conf)
- [Snapshot Plugin - reference.conf](https://github.com/j5ik2o/akka-persistence-dynamodb/blob/main/snapshot/snapshot-base/src/main/resources/reference.conf)  
- [State Plugin - reference.conf](https://github.com/j5ik2o/akka-persistence-dynamodb/blob/main/state/state-base/src/main/resources/reference.conf)
  
Common configuration items are defined in `reference.conf` of the base project.
 
[Common - reference.conf](https://github.com/j5ik2o/akka-persistence-dynamodb/blob/main/base/base/src/main/resources/reference.conf)
