package com.github.j5ik2o.akka.persistence.dynamodb.journal

import scala.concurrent.duration.Duration

case class DynamoDBConfig(tableName: String = "",
                          tagSeparator: String = "",
                          maxConcurrency: Option[Int] = None,
                          maxPendingConnectionAcquires: Option[Int] = None,
                          readTimeout: Option[Duration] = None,
                          writeTimeout: Option[Duration] = None,
                          connectionTimeout: Option[Duration] = None,
                          connectionAcquisitionTimeout: Option[Duration] = None,
                          connectionTimeToLive: Option[Duration] = None,
                          maxIdleConnectionTimeout: Option[Duration] = None,
                          useConnectionReaper: Option[Boolean] = None,
                          userHttp2: Option[Boolean] = None,
                          maxHttp2Streams: Option[Int] = None)
