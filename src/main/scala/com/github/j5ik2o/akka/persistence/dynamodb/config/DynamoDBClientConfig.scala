package com.github.j5ik2o.akka.persistence.dynamodb.config

import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.typesafe.config.Config

import scala.concurrent.duration._

object DynamoDBClientConfig {

  def fromConfig(rootConfig: Config): DynamoDBClientConfig = {
    val result = DynamoDBClientConfig(
      accessKeyId = rootConfig.asString("access-key-id"),
      secretAccessKey = rootConfig.asString("secret-access-key"),
      endpoint = rootConfig.asString("endpoint"),
      maxConcurrency = rootConfig.asInt("max-concurrency"),
      maxPendingConnectionAcquires = rootConfig.asInt("max-pending-connection-acquires"),
      readTimeout = rootConfig.asFiniteDuration("read-timeout"),
      writeTimeout = rootConfig.asFiniteDuration("write-timeout"),
      connectionTimeout = rootConfig.asFiniteDuration("connection-timeout"),
      connectionAcquisitionTimeout = rootConfig.asFiniteDuration("connection-acquisition-timeout"),
      connectionTimeToLive = rootConfig.asFiniteDuration("connection-time-to-live"),
      maxIdleConnectionTimeout = rootConfig.asFiniteDuration("max-idle-connection-timeout"),
      useConnectionReaper = rootConfig.asBoolean("use-connection-reaper"),
      threadsOfEventLoopGroup = rootConfig.asInt("threads-of-event-loop-group"),
      userHttp2 = rootConfig.asBoolean("user-http2"),
      maxHttp2Streams = rootConfig.asInt("max-http2-streams")
    )
    result
  }

}

case class DynamoDBClientConfig(accessKeyId: Option[String],
                                secretAccessKey: Option[String],
                                endpoint: Option[String],
                                maxConcurrency: Option[Int] = None,
                                maxPendingConnectionAcquires: Option[Int] = None,
                                readTimeout: Option[FiniteDuration] = None,
                                writeTimeout: Option[FiniteDuration] = None,
                                connectionTimeout: Option[FiniteDuration] = None,
                                connectionAcquisitionTimeout: Option[FiniteDuration] = None,
                                connectionTimeToLive: Option[FiniteDuration] = None,
                                maxIdleConnectionTimeout: Option[FiniteDuration] = None,
                                useConnectionReaper: Option[Boolean] = None,
                                threadsOfEventLoopGroup: Option[Int] = None,
                                userHttp2: Option[Boolean] = None,
                                maxHttp2Streams: Option[Int] = None)
