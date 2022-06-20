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

### Snapshot Store Plugin

### State Store Plugin
