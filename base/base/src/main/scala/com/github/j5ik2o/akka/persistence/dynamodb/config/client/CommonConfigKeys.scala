package com.github.j5ik2o.akka.persistence.dynamodb.config.client

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
