# Overview

## Features

|      Product       | **[j5ik2o/akka-persistence-dynamodb](https://github.com/j5ik2o/akka-persistence-dynamodb)** | **[akka/akka-persistence-dynamodb](https://github.com/akka/akka-persistence-dynamodb)** |
|:------------------:|:-------------------------------------------------------------------------------------------:|:---------------------------------------------------------------------------------------:|
|  DynamoDB support  |                                              ✓                                              |                                            ✓                                            |
|   Write Sharding   |                                              ✓                                              |                                            ✓                                            |
|  Non-blocking I/O  |                                              ✓                                              |                                            -                                            |
|   back pressure    |                                              ✓                                              |                                            -                                            |
| AWS Client Version |                                     v1,v1-dax,v2,v2-dax                                     |                                           v1                                            |
|   Journal Plugin   |                                              ✓                                              |                                            ✓                                            |
|  Snapshot Plugin   |                                              ✓                                              |                                            ✓                                            |
|    State Plugin    |                                              ✓                                              |                                            -                                            |
|    Query Plugin    |                                              -                                              |                                            -                                            |

```{note}
Since version 1.7.x, the query plugin was discontinued. You can't get throughput by simply doing a Scan like the query plugin. A better way is to use [DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html) or [KDS for DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/kds.html).
```

### Programming Language Support

Java 1.8+ or Scala 2.12.x or 2.13.x

### Akka Support

[Akka](https://akka.io/) 2.6+

The following modules are required to use the plugins.

- akka typed
  - [akka-actor-typed](https://doc.akka.io/docs/akka/current/typed/index.html)
  - [akka-persistence-typed](https://doc.akka.io/docs/akka/current/typed/index-persistence.html)
- akka classic
  - [akka-actor](https://doc.akka.io/docs/akka/current/actors.html)
  - [akka-persistence](https://doc.akka.io/docs/akka/current/persistence.html)

### AWS DynamoDB Client Support

The plugins use the AWS SDK for communication with `AWS DynamoDB`.

- [aws-sdk-java-v1](https://github.com/aws/aws-sdk-java)
- [aws-sdk-java-v2](https://github.com/aws/aws-sdk-java-v2)
- [Amazon DynamoDB Accelerator(DAX) Client](https://aws.amazon.com/jp/dynamodb/dax/)

