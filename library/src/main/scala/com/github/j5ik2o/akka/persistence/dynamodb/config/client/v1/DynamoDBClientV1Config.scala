package com.github.j5ik2o.akka.persistence.dynamodb.config.client.v1

import com.amazonaws.retry.RetryMode
import com.amazonaws.{ ClientConfiguration, Protocol }
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v1.V1RetryPolicyProvider
import com.github.j5ik2o.akka.persistence.dynamodb.utils.LoggingSupport
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.collection.immutable._
import scala.concurrent.duration.{ FiniteDuration, _ }

object DynamoDBClientV1Config extends LoggingSupport {

  val dispatcherNameKey               = "dispatcher-name"
  val connectionTimeoutKey            = "connection-timeout"
  val maxConnectionsKey               = "max-connections"
  val maxErrorRetryKey                = "max-error-retry"
  val retryModeKey                    = "retry-mode"
  val retryPolicyProviderClassNameKey = "retry-policy-provider-class-name"
  val throttleRetriesKey              = "throttle-retries"
  val localAddressKey                 = "local-address"
  val protocolKey                     = "protocol"
  val socketTimeoutKey                = "socket-timeout"
  val requestTimeoutKey               = "request-timeout"
  val clientExecutionTimeoutKey       = "client-execution-timeout"
  val userAgentPrefixKey              = "user-agent-prefix"
  val userAgentSuffixKey              = "user-agent-suffix"
  val useReaper                       = "use-reaper"
  val useGzip                         = "use-gzip"

  def fromConfig(config: Config): DynamoDBClientV1Config = {
    val result = DynamoDBClientV1Config(
      dispatcherName = config.getAs[String](dispatcherNameKey),
      connectionTimeout = config
        .getOrElse[FiniteDuration](connectionTimeoutKey, ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT milliseconds),
      maxConnections = config.getOrElse[Int](maxConnectionsKey, ClientConfiguration.DEFAULT_MAX_CONNECTIONS),
      maxErrorRetry = config.getAs[Int](maxErrorRetryKey),
      retryMode = config.getAs[String](retryModeKey).map(s => RetryMode.valueOf(s)),
      retryPolicyProviderClassName = config
        .getAs[String](retryPolicyProviderClassNameKey).orElse(Some(classOf[V1RetryPolicyProvider.Default].getName)),
      throttleRetries = config.getOrElse[Boolean](throttleRetriesKey, ClientConfiguration.DEFAULT_THROTTLE_RETRIES),
      localAddress = config.getAs[String](localAddressKey),
      protocol = config.getAs[String](protocolKey).map(s => Protocol.valueOf(s)),
      socketTimeout =
        config.getOrElse[FiniteDuration](socketTimeoutKey, ClientConfiguration.DEFAULT_SOCKET_TIMEOUT milliseconds),
      requestTimeout =
        config.getOrElse[FiniteDuration](requestTimeoutKey, ClientConfiguration.DEFAULT_REQUEST_TIMEOUT milliseconds),
      clientExecutionTimeout = config.getOrElse[FiniteDuration](
        clientExecutionTimeoutKey,
        ClientConfiguration.DEFAULT_CLIENT_EXECUTION_TIMEOUT milliseconds
      ),
      userAgentPrefix = config.getAs[String](userAgentPrefixKey),
      userAgentSuffix = config.getAs[String](userAgentSuffixKey),
      useReaper = config.getOrElse[Boolean](useReaper, ClientConfiguration.DEFAULT_USE_REAPER),
      useGzip = config.getOrElse[Boolean]("use-gzip", ClientConfiguration.DEFAULT_USE_GZIP),
      socketBufferSizeHint = {
        (config.getAs[Int]("socket-send-buffer-size-hint"), config.getAs[Int]("socket-receive-buffer-size-hint")) match {
          case (Some(s), Some(r)) =>
            Some(SocketSendBufferSizeHint(s, r))
          case _ =>
            None
        }
      },
      signerOverride = config.getAs[String]("signer-override"),
      responseMetadataCacheSize =
        config.getOrElse[Int]("response-metadata-cache-size", ClientConfiguration.DEFAULT_RESPONSE_METADATA_CACHE_SIZE),
      dnsResolverClassName = config.getAs[String]("dns-resolver-class-name"),
      secureRandomProviderClassName = config.getAs[String]("secure-random-provider-class-name"),
      useExpectContinue =
        config.getOrElse[Boolean]("use-expect-contine", ClientConfiguration.DEFAULT_USE_EXPECT_CONTINUE),
      cacheResponseMetadata =
        config.getOrElse[Boolean]("cache-response-metadata", ClientConfiguration.DEFAULT_CACHE_RESPONSE_METADATA),
      connectionTtl = config.getAs[Duration]("connection-ttl"),
      connectionMaxIdle = config
        .getOrElse[FiniteDuration](
          "connection-max-idle",
          ClientConfiguration.DEFAULT_CONNECTION_MAX_IDLE_MILLIS milliseconds
        ),
      validateAfterInactivity = config.getOrElse[FiniteDuration](
        "validate-after-inactivity",
        ClientConfiguration.DEFAULT_VALIDATE_AFTER_INACTIVITY_MILLIS milliseconds
      ),
      tcpKeepAlive = config.getOrElse[Boolean]("tcp-keep-alive", ClientConfiguration.DEFAULT_TCP_KEEP_ALIVE),
      headers = config.getOrElse[Map[String, String]]("headers", Map.empty),
      maxConsecutiveRetriesBeforeThrottling = config.getOrElse[Int](
        "max-consecutive-retries-before-throttling",
        ClientConfiguration.DEFAULT_MAX_CONSECUTIVE_RETRIES_BEFORE_THROTTLING
      ),
      disableHostPrefixInjection = config.getAs[Boolean]("disable-host-prefix-injection")
    )
    logger.debug("config = {}", result)
    result
  }
}

case class DynamoDBClientV1Config(
    dispatcherName: Option[String],
    connectionTimeout: FiniteDuration,
    maxConnections: Int,
    maxErrorRetry: Option[Int],
    retryMode: Option[RetryMode],
    retryPolicyProviderClassName: Option[String],
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
    secureRandomProviderClassName: Option[String],
    useExpectContinue: Boolean,
    cacheResponseMetadata: Boolean,
    connectionTtl: Option[Duration],
    connectionMaxIdle: FiniteDuration,
    validateAfterInactivity: FiniteDuration,
    tcpKeepAlive: Boolean,
    headers: Map[String, String],
    maxConsecutiveRetriesBeforeThrottling: Int,
    disableHostPrefixInjection: Option[Boolean]
)
