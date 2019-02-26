package com.github.j5ik2o.akka.persistence.dynamodb.journal

import com.typesafe.config.Config

import scala.concurrent.duration._
import scala.util.Try

object PersistencePluginConfig {

  def fromConfig(rootConfig: Config): Option[PersistencePluginConfig] = {
    val root = "akka-persistence-dynamodb"
    Try(rootConfig.getConfig(root)).map { config =>
      new PersistencePluginConfig(
        tableName = config.getString("table-name"),
        tagSeparator = config.getString("tag-separator"),
        bufferSize = config.getInt("buffer-size"),
        batchSize = config.getInt("batch-size"),
        parallelism = config.getInt("parallelism"),
        clientConfig = DynamoDBClientConfig.fromConfig(config.getConfig("client-config"))
      )
    }.toOption

  }

}

case class PersistencePluginConfig(tableName: String = "Journal",
                                   tagSeparator: String = ",",
                                   bufferSize: Int = 1024,
                                   batchSize: Int = 64,
                                   parallelism: Int = 64,
                                   clientConfig: Option[DynamoDBClientConfig] = Some(DynamoDBClientConfig()))

object DynamoDBClientConfig {

  def fromConfig(rootConfig: Config): Option[DynamoDBClientConfig] = {
    val root = "akka-persistence-dynamodb.dynamodb-client"
    Try(rootConfig.getConfig(root)).map { config =>
      new DynamoDBClientConfig(
        accessKeyId = Try(config.getString("access-key-id")).toOption,
        secretAccessKey = Try(config.getString("secret-access-key")).toOption,
        endpoint = Try(config.getString("endpoint")).toOption,
        maxConcurrency = Try(config.getInt("max-concurrency")).toOption,
        maxPendingConnectionAcquires = Try(config.getInt("max-pending-connection-acquires")).toOption,
        readTimeout = Try(config.getDuration("read-timeout")).map(_.toMillis millis).toOption,
        writeTimeout = Try(config.getDuration("write-timeout")).map(_.toMillis millis).toOption,
        connectionTimeout = Try(config.getDuration("connection-timeout")).map(_.toMillis millis).toOption,
        connectionAcquisitionTimeout =
          Try(config.getDuration("connection-acquisition-timeout")).map(_.toMillis millis).toOption,
        connectionTimeToLive = Try(config.getDuration("connection-time-to-live")).map(_.toMillis millis).toOption,
        maxIdleConnectionTimeout =
          Try(config.getDuration("max-idle-connection-timeout")).map(_.toMillis millis).toOption,
        useConnectionReaper = Try(config.getBoolean("use-connection-reaper")).toOption,
        userHttp2 = Try(config.getBoolean("user-http2")).toOption,
        maxHttp2Streams = Try(config.getInt("max-http2-streams")).toOption
      )
    }.toOption
  }

}

case class DynamoDBClientConfig(accessKeyId: Option[String] = Some("x"),
                                secretAccessKey: Option[String] = Some("x"),
                                endpoint: Option[String] = Some("http://127.0.0.1:8000/"),
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
