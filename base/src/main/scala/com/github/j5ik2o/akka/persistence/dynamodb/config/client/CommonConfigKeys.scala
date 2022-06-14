package com.github.j5ik2o.akka.persistence.dynamodb.config.client

import scala.concurrent.duration.{ Duration, DurationInt, FiniteDuration }

object CommonConfigKeys {

  val dispatcherNameKey    = "dispatcher-name"
  val requestTimeoutKey    = "request-timeout"
  val connectionTimeoutKey = "connection-timeout"
  val retryModeKey         = "retry-mode"
  val connectionTtlKey     = "connection-ttl"
  val maxConnectionsKey    = "max-connections"

  val socketTimeoutKey = "socket-timeout"
  val writeRetriesKey  = "write-retries"
  val readRetriesKey   = "read-retries"

  val headersKey = "headers"

  val metricsReporterClassNameKey         = "metrics-reporter-class-name"
  val metricsReporterProviderClassNameKey = "metrics-reporter-provider-class-name"
  val traceReporterClassNameKey           = "trace-reporter-class-name"
  val traceReporterProviderClassNameKey   = "trace-reporter-provider-class-name"
}

object CommonConfigDefaultValues {
  val DefaultConnectionTtl: FiniteDuration     = Duration.Zero
  val DefaultRequestTimeout: FiniteDuration    = 60000.milliseconds
  val DefaultConnectionTimeout: FiniteDuration = 2000.milliseconds
  val DefaultSocketTimeout: FiniteDuration     = 50000.milliseconds
  val DefaultMaxConnections: Int               = 50
  val DefaultWriteRetries: Int                 = 2
  val DefaultReadRetries: Int                  = 2
}
