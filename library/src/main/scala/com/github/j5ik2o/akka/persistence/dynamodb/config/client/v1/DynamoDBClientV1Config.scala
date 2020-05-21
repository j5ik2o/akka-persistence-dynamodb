/*
 * Copyright 2020 Junichi Kato
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.j5ik2o.akka.persistence.dynamodb.config.client.v1

import com.amazonaws.handlers.RequestHandler2
import com.amazonaws.metrics.RequestMetricCollector
import com.amazonaws.monitoring.MonitoringListener
import com.amazonaws.retry.RetryMode
import com.amazonaws.{ DnsResolver, Protocol, ClientConfiguration => AWSClientConfiguration }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v1._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.{ ClassCheckUtils, LoggingSupport }
import com.typesafe.config.{ Config, ConfigFactory }
import net.ceedubs.ficus.Ficus._

import scala.collection.immutable._
import scala.concurrent.duration.{ FiniteDuration, _ }

object DynamoDBClientV1Config extends LoggingSupport {

  val dispatcherNameKey                          = "dispatcher-name"
  val clientConfigurationKey                     = "client-configuration"
  val requestMetricCollectorProviderClassNameKey = "request-metric-collector-provider-class-name"
  val requestMetricCollectorClassNameKey         = "request-metric-collector-class-name"
  val monitoringListenerProviderClassNameKey     = "monitoring-listener-provider-class-name"
  val monitoringListenerClassNameKey             = "monitoring-listener-class-name"
  val requestHandlersProviderClassNameKey        = "request-handlers-provider-class-name"
  val requestHandlerClassNamesKey                = "request-handler-class-names"

  val DefaultRequestMetricCollectorProviderClassName: String = classOf[RequestMetricCollectorProvider.Default].getName
  val DefaultMonitoringListenerProviderClassName: String     = classOf[MonitoringListenerProvider.Default].getName

  def fromConfig(config: Config): DynamoDBClientV1Config = {
    logger.debug("config = {}", config)
    val result = DynamoDBClientV1Config(
      sourceConfig = config,
      dispatcherName = config.getAs[String](dispatcherNameKey),
      clientConfiguration =
        ClientConfiguration.fromConfig(config.getOrElse[Config](clientConfigurationKey, ConfigFactory.empty())),
      requestMetricCollectorProviderClassName = {
        val className =
          config.getOrElse(requestMetricCollectorProviderClassNameKey, DefaultRequestMetricCollectorProviderClassName)
        ClassCheckUtils.requireClass(classOf[RequestMetricCollectorProvider], className)
      },
      requestMetricCollectorClassName = {
        val className = config.getAs[String](requestMetricCollectorClassNameKey)
        ClassCheckUtils.requireClass(classOf[RequestMetricCollector], className)
      },
      monitoringListenerProviderClassName = {
        val className = config
          .getOrElse(monitoringListenerProviderClassNameKey, DefaultMonitoringListenerProviderClassName)
        ClassCheckUtils.requireClass(classOf[MonitoringListenerProvider], className)
      },
      monitoringListenerClassName = {
        val className = config.getAs[String](monitoringListenerClassNameKey)
        ClassCheckUtils.requireClass(classOf[MonitoringListener], className)
      },
      requestHandlersProviderClassName = {
        val className = config
          .getOrElse[String](requestHandlersProviderClassNameKey, classOf[RequestHandlersProvider.Default].getName)
        ClassCheckUtils.requireClass(classOf[RequestHandlersProvider], className)
      },
      requestHandlerClassNames = {
        val classNames = config.getOrElse[Seq[String]](requestHandlerClassNamesKey, Seq.empty)
        classNames.map { className => ClassCheckUtils.requireClass(classOf[RequestHandler2], className) }
      }
    )
    logger.debug("result = {}", result)
    result
  }
}

object ClientConfiguration {
  val connectionTimeoutKey                     = "connection-timeout"
  val maxConnectionsKey                        = "max-connections"
  val maxErrorRetryKey                         = "max-error-retry"
  val retryModeKey                             = "retry-mode"
  val retryPolicyProviderClassNameKey          = "retry-policy-provider-class-name"
  val throttleRetriesKey                       = "throttle-retries"
  val localAddressKey                          = "local-address"
  val protocolKey                              = "protocol"
  val socketTimeoutKey                         = "socket-timeout"
  val requestTimeoutKey                        = "request-timeout"
  val clientExecutionTimeoutKey                = "client-execution-timeout"
  val userAgentPrefixKey                       = "user-agent-prefix"
  val userAgentSuffixKey                       = "user-agent-suffix"
  val useReaper                                = "use-reaper"
  val useGzip                                  = "use-gzip"
  val socketSendBufferSizeHintKey              = "socket-send-buffer-size-hint"
  val socketReceiveBufferSizeHint              = "socket-receive-buffer-size-hint"
  val signerOverrideKey                        = "signer-override"
  val responseMetadataCacheSizeKey             = "response-metadata-cache-size"
  val dnsResolverProviderClassNameKey          = "dns-resolver-provider-class-name"
  val dnsResolverClassNameKey                  = "dns-resolver-class-name"
  val secureRandomProviderClassNameKey         = "secure-random-provider-class-name"
  val useSecureRandomKey                       = "secure-random-default"
  val useExpectContinueKey                     = "use-expect-continue"
  val cacheResponseMetadataKey                 = "cache-response-metadata"
  val connectionTtlKey                         = "connection-ttl"
  val connectionMaxIdleKey                     = "connection-max-idle"
  val validateAfterInactivityKey               = "validate-after-inactivity"
  val tcpKeepAliveKey                          = "tcp-keep-alive"
  val headersKey                               = "headers"
  val maxConsecutiveRetriesBeforeThrottlingKey = "max-consecutive-retries-before-throttling"
  val disableHostPrefixInjectionKey            = "disable-host-prefix-injection"
  val proxyProtocolKey                         = "proxy-protocol"
  val proxyHostKey                             = "proxy-host"
  val proxyPortKey                             = "proxy-port"
  val disableSocketProxyKey                    = "disable-socket-proxy"
  val proxyUsernameKey                         = "proxy-username"
  val proxyPasswordKey                         = "proxy-password"
  val proxyDomainKey                           = "proxy-domain"
  val proxyWorkstationKey                      = "proxy-workstation"
  val nonProxyHostsKey                         = "non-proxy-hosts"
  val proxyAuthenticationMethodsKey            = "proxy-authentication-methods"

  val DefaultConnectionTimeout: FiniteDuration      = AWSClientConfiguration.DEFAULT_CONNECTION_TIMEOUT milliseconds
  val DefaultMaxConnections: Int                    = AWSClientConfiguration.DEFAULT_MAX_CONNECTIONS
  val DefaultV1RetryPolicyProviderClassName: String = classOf[RetryPolicyProvider.Default].getName
  val DefaultThrottleRetries: Boolean               = AWSClientConfiguration.DEFAULT_THROTTLE_RETRIES
  val DefaultSocketTimeout: FiniteDuration          = AWSClientConfiguration.DEFAULT_SOCKET_TIMEOUT milliseconds
  val DefaultRequestTimeout: FiniteDuration         = AWSClientConfiguration.DEFAULT_REQUEST_TIMEOUT milliseconds

  val DefaultClientExecutionTimeout: FiniteDuration =
    AWSClientConfiguration.DEFAULT_CLIENT_EXECUTION_TIMEOUT milliseconds
  val DefaultUseReaper: Boolean                    = AWSClientConfiguration.DEFAULT_USE_REAPER
  val DefaultUseGZIP: Boolean                      = AWSClientConfiguration.DEFAULT_USE_GZIP
  val DefaultResponseMetadataCacheSize: Int        = AWSClientConfiguration.DEFAULT_RESPONSE_METADATA_CACHE_SIZE
  val DefaultSecureRandomProviderClassName: String = classOf[SecureRandomProvider.Default].getName
  val DefaultUseSecureRandom: Boolean              = false
  val DefaultUseExpectContinue: Boolean            = AWSClientConfiguration.DEFAULT_USE_EXPECT_CONTINUE
  val DefaultCacheResponseMetadata: Boolean        = AWSClientConfiguration.DEFAULT_CACHE_RESPONSE_METADATA
  val DefaultConnectionMaxIdle: FiniteDuration     = AWSClientConfiguration.DEFAULT_CONNECTION_MAX_IDLE_MILLIS milliseconds

  val DefaultValidateAfterInactivity: FiniteDuration =
    AWSClientConfiguration.DEFAULT_VALIDATE_AFTER_INACTIVITY_MILLIS milliseconds
  val DefaultTcpKeepAlive: Boolean = AWSClientConfiguration.DEFAULT_TCP_KEEP_ALIVE

  val DefaultMaxConsecutiveRetiesBeforeThrottling: Int =
    AWSClientConfiguration.DEFAULT_MAX_CONSECUTIVE_RETRIES_BEFORE_THROTTLING
  val DnsResolverProviderClassName: String = classOf[DnsResolverProviderImpl].getName

  def fromConfig(config: Config) = {
    ClientConfiguration(
      connectionTimeout = config
        .getOrElse[FiniteDuration](connectionTimeoutKey, DefaultConnectionTimeout),
      maxConnections = config.getOrElse[Int](maxConnectionsKey, DefaultMaxConnections),
      maxErrorRetry = config.getAs[Int](maxErrorRetryKey),
      retryMode = config.getAs[String](retryModeKey).map(s => RetryMode.valueOf(s)),
      retryPolicyProviderClassName = {
        val className = config
          .getAs[String](retryPolicyProviderClassNameKey).orElse(Some(DefaultV1RetryPolicyProviderClassName))
        ClassCheckUtils.requireClass(classOf[RetryPolicyProvider], className)
      },
      throttleRetries = config.getOrElse[Boolean](throttleRetriesKey, DefaultThrottleRetries),
      localAddress = config.getAs[String](localAddressKey),
      protocol = config.getAs[String](protocolKey).map(s => Protocol.valueOf(s)),
      socketTimeout = config.getOrElse[FiniteDuration](socketTimeoutKey, DefaultSocketTimeout),
      requestTimeout = config.getOrElse[FiniteDuration](requestTimeoutKey, DefaultRequestTimeout),
      clientExecutionTimeout =
        config.getOrElse[FiniteDuration](clientExecutionTimeoutKey, DefaultClientExecutionTimeout),
      userAgentPrefix = config.getAs[String](userAgentPrefixKey),
      userAgentSuffix = config.getAs[String](userAgentSuffixKey),
      useReaper = config.getOrElse[Boolean](useReaper, DefaultUseReaper),
      useGzip = config.getOrElse[Boolean](useGzip, DefaultUseGZIP),
      socketBufferSizeHint = {
        (config.getAs[Int](socketSendBufferSizeHintKey), config.getAs[Int](socketReceiveBufferSizeHint)) match {
          case (Some(s), Some(r)) => Some(SocketSendBufferSizeHint(s, r))
          case _                  => None
        }
      },
      signerOverride = config.getAs[String](signerOverrideKey),
      responseMetadataCacheSize = config.getOrElse[Int](responseMetadataCacheSizeKey, DefaultResponseMetadataCacheSize),
      dnsResolverProviderClassName = {
        val className =
          config.getOrElse[String](dnsResolverProviderClassNameKey, DnsResolverProviderClassName)
        ClassCheckUtils.requireClass(classOf[DnsResolverProvider], className)
      },
      dnsResolverClassName = {
        val className = config.getAs[String](dnsResolverClassNameKey)
        ClassCheckUtils.requireClass(classOf[DnsResolver], className)
      },
      secureRandomProviderClassName = {
        val className =
          config.getOrElse[String](secureRandomProviderClassNameKey, DefaultSecureRandomProviderClassName)
        ClassCheckUtils.requireClass(classOf[SecureRandomProvider], className)
      },
      useSecureRandom = config.getOrElse(useSecureRandomKey, DefaultUseSecureRandom),
      useExpectContinue = config.getOrElse[Boolean](useExpectContinueKey, DefaultUseExpectContinue),
      cacheResponseMetadata = config.getOrElse[Boolean](cacheResponseMetadataKey, DefaultCacheResponseMetadata),
      connectionTtl = config.getAs[Duration](connectionTtlKey),
      connectionMaxIdle = config
        .getOrElse[FiniteDuration](connectionMaxIdleKey, DefaultConnectionMaxIdle),
      validateAfterInactivity =
        config.getOrElse[FiniteDuration](validateAfterInactivityKey, DefaultValidateAfterInactivity),
      tcpKeepAlive = config.getOrElse[Boolean](tcpKeepAliveKey, DefaultTcpKeepAlive),
      headers = config.getOrElse[Map[String, String]](headersKey, Map.empty),
      maxConsecutiveRetriesBeforeThrottling =
        config.getOrElse[Int](maxConsecutiveRetriesBeforeThrottlingKey, DefaultMaxConsecutiveRetiesBeforeThrottling),
      disableHostPrefixInjection = config.getAs[Boolean](disableHostPrefixInjectionKey),
      proxyProtocol = config.getAs[String](proxyProtocolKey),
      proxyHost = config.getAs[String](proxyHostKey),
      proxyPort = config.getAs[Int](proxyPortKey),
      disableSocketProxy = config.getAs[Boolean](disableSocketProxyKey),
      proxyUsername = config.getAs[String](proxyUsernameKey),
      proxyPassword = config.getAs[String](proxyPasswordKey),
      proxyDomain = config.getAs[String](proxyDomainKey),
      proxyWorkstation = config.getAs[String](proxyWorkstationKey),
      nonProxyHosts = config.getAs[String](nonProxyHostsKey),
      proxyAuthenticationMethods = config.getOrElse[Seq[String]](proxyAuthenticationMethodsKey, Seq.empty)
    )
  }
}

case class ClientConfiguration(
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
    dnsResolverProviderClassName: String,
    dnsResolverClassName: Option[String],
    secureRandomProviderClassName: String,
    useSecureRandom: Boolean,
    useExpectContinue: Boolean,
    cacheResponseMetadata: Boolean,
    connectionTtl: Option[Duration],
    connectionMaxIdle: FiniteDuration,
    validateAfterInactivity: FiniteDuration,
    tcpKeepAlive: Boolean,
    headers: Map[String, String],
    maxConsecutiveRetriesBeforeThrottling: Int,
    disableHostPrefixInjection: Option[Boolean],
    proxyProtocol: Option[String],
    proxyHost: Option[String],
    proxyPort: Option[Int],
    disableSocketProxy: Option[Boolean],
    proxyUsername: Option[String],
    proxyPassword: Option[String],
    proxyDomain: Option[String],
    proxyWorkstation: Option[String],
    nonProxyHosts: Option[String],
    proxyAuthenticationMethods: Seq[String]
)

case class DynamoDBClientV1Config(
    sourceConfig: Config,
    dispatcherName: Option[String],
    clientConfiguration: ClientConfiguration,
    requestMetricCollectorProviderClassName: String,
    requestMetricCollectorClassName: Option[String],
    monitoringListenerProviderClassName: String,
    monitoringListenerClassName: Option[String],
    requestHandlersProviderClassName: String,
    requestHandlerClassNames: Seq[String]
)
