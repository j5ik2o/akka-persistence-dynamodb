package com.github.j5ik2o.akka.persistence.dynamodb.config

import java.net.InetAddress

import akka.actor.DynamicAccess
import com.amazonaws.retry.{ RetryMode, RetryPolicy }
import com.amazonaws.{ ClientConfiguration, DnsResolver, Protocol }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config

import scala.collection.immutable._
import scala.concurrent.duration.{ FiniteDuration, _ }

object DynamoDBClientV1Config extends LoggingSupport {

  def from(config: Config): DynamoDBClientV1Config = {
    val result = DynamoDBClientV1Config(
      dispatcherName = config.asString("dispatcher-name"),
      connectionTimeout = config
        .asFiniteDuration("connection-timeout", ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT milliseconds),
      maxConnections = config.asInt("max-connections", ClientConfiguration.DEFAULT_MAX_CONNECTIONS),
      maxErrorRetry = config.asInt("max-error-retry"),
      retryPolicyClassName = config.asString("retry-policy-class-name"),
      throttleRetries = config.asBoolean("throttle-retries", ClientConfiguration.DEFAULT_THROTTLE_RETRIES),
      localAddress = config.asString("local-address"),
      protocol = config.asString("protocol").map(s => Protocol.valueOf(s)),
      socketTimeout =
        config.asFiniteDuration("socket-timeout", ClientConfiguration.DEFAULT_SOCKET_TIMEOUT milliseconds),
      requestTimeout =
        config.asFiniteDuration("request-timeout", ClientConfiguration.DEFAULT_REQUEST_TIMEOUT milliseconds),
      clientExecutionTimeout = config.asFiniteDuration(
        "client-execution-timeout",
        ClientConfiguration.DEFAULT_CLIENT_EXECUTION_TIMEOUT milliseconds
      ),
      userAgentPrefix = config.asString("user-agent-prefix"),
      userAgentSuffix = config.asString("user-agent-suffix"),
      useReaper = config.asBoolean("use-reaper", ClientConfiguration.DEFAULT_USE_REAPER),
      useGzip = config.asBoolean("use-gzip", ClientConfiguration.DEFAULT_USE_GZIP),
      socketBufferSizeHint = {
        (config.asInt("socket-send-buffer-size-hint"), config.asInt("socket-receive-buffer-size-hint")) match {
          case (Some(s), Some(r)) =>
            Some(SocketSendBufferSizeHint(s, r))
          case _ =>
            None
        }
      },
      signerOverride = config.asString("signer-override"),
      responseMetadataCacheSize =
        config.asInt("response-metadata-cache-size", ClientConfiguration.DEFAULT_RESPONSE_METADATA_CACHE_SIZE),
      dnsResolverClassName = config.asString("dns-resolver-class-name"),
      useExpectContinue = config.asBoolean("use-expect-contine", ClientConfiguration.DEFAULT_USE_EXPECT_CONTINUE),
      cacheResponseMetadata = config.asBoolean("", ClientConfiguration.DEFAULT_CACHE_RESPONSE_METADATA),
      connectionTtl = config.asDuration("connection-ttl"),
      connectionMaxIdle = config
        .asFiniteDuration(
          "connection-max-idle",
          ClientConfiguration.DEFAULT_CONNECTION_MAX_IDLE_MILLIS milliseconds
        ),
      validateAfterInactivity = config.asFiniteDuration(
        "validate-after-inactivity",
        ClientConfiguration.DEFAULT_VALIDATE_AFTER_INACTIVITY_MILLIS milliseconds
      ),
      tcpKeepAlive = config.asBoolean("tcp-keep-alive", ClientConfiguration.DEFAULT_TCP_KEEP_ALIVE),
      //  headers = config.as[Map[String, String]]("headers", Map.empty),
      maxConsecutiveRetriesBeforeThrottling = config.asInt(
        "max-consecutive-retries-before-throttling",
        ClientConfiguration.DEFAULT_MAX_CONSECUTIVE_RETRIES_BEFORE_THROTTLING
      ),
      disableHostPrefixInjection = config.asBoolean("disable-host-prefix-injection"),
      retryMode = config.asString("retry-mode").map(s => RetryMode.valueOf(s))
    )
    logger.debug("config = {}", result)
    result
  }
}

case class SocketSendBufferSizeHint(send: Int, receive: Int)

case class DynamoDBClientV1Config(
    dispatcherName: Option[String],
    connectionTimeout: FiniteDuration,
    maxConnections: Int,
    maxErrorRetry: Option[Int],
    retryPolicyClassName: Option[String],
    throttleRetries: Boolean,
    localAddress: Option[String],
    protocol: Option[Protocol],
    socketTimeout: FiniteDuration,
    requestTimeout: FiniteDuration,
    clientExecutionTimeout: FiniteDuration,
    userAgentPrefix: Option[String],
    userAgentSuffix: Option[String],
    useReaper: Boolean,
    useGzip: Boolean,
    socketBufferSizeHint: Option[SocketSendBufferSizeHint],
    signerOverride: Option[String],
    responseMetadataCacheSize: Int,
    dnsResolverClassName: Option[String],
    useExpectContinue: Boolean,
    cacheResponseMetadata: Boolean,
    connectionTtl: Option[Duration],
    connectionMaxIdle: FiniteDuration,
    validateAfterInactivity: FiniteDuration,
    tcpKeepAlive: Boolean,
    // headers: Map[String, String],
    maxConsecutiveRetriesBeforeThrottling: Int,
    disableHostPrefixInjection: Option[Boolean],
    retryMode: Option[RetryMode]
) {

  def toAWS(dynamicAccess: DynamicAccess): ClientConfiguration = {
    val result = new ClientConfiguration()
      .withConnectionTimeout(connectionTimeout.toMillis.toInt)
      .withMaxConnections(maxConnections)
    maxErrorRetry.foreach { v => result.setMaxErrorRetry(v) }
    retryPolicyClassName.foreach { v =>
      val retryPolicy = dynamicAccess.createInstanceFor[RetryPolicy](v, Seq.empty).get
      result
        .setRetryPolicy(retryPolicy)
    }
    result
      .withThrottledRetries(throttleRetries)
    localAddress.foreach { v => result.setLocalAddress(InetAddress.getByName(v)) }
    protocol.foreach { v => result.setProtocol(v) }
    result
      .withSocketTimeout(socketTimeout.toMillis.toInt)
      .withRequestTimeout(requestTimeout.toMillis.toInt)
      .withClientExecutionTimeout(clientExecutionTimeout.toMillis.toInt)
    userAgentPrefix.foreach { v => result.setUserAgentPrefix(v) }
    userAgentSuffix.foreach { v => result.setUserAgentSuffix(v) }
    result
      .withReaper(useReaper)
      .withGzip(useGzip)
    socketBufferSizeHint.foreach { v => result.setSocketBufferSizeHints(v.send, v.receive) }
    signerOverride.foreach { v => result.setSignerOverride(v) }
    result.withResponseMetadataCacheSize(responseMetadataCacheSize)
    dnsResolverClassName.foreach { v =>
      val dnsResolver = dynamicAccess.createInstanceFor[DnsResolver](v, Seq.empty).get
      result.setDnsResolver(dnsResolver)
    }
    result
      .withUseExpectContinue(useExpectContinue)
      .withCacheResponseMetadata(cacheResponseMetadata)
    connectionTtl.foreach { v => result.setConnectionTTL(v.toMillis) }
    result
      .withConnectionMaxIdleMillis(connectionMaxIdle.toMillis)
      .withValidateAfterInactivityMillis(validateAfterInactivity.toMillis.toInt)
      .withTcpKeepAlive(tcpKeepAlive)
      .withMaxConsecutiveRetriesBeforeThrottling(maxConsecutiveRetriesBeforeThrottling)
    disableHostPrefixInjection.foreach { v => result.setDisableHostPrefixInjection(v) }
    retryMode.foreach { v => result.setRetryMode(v) }
    result
  }

}
