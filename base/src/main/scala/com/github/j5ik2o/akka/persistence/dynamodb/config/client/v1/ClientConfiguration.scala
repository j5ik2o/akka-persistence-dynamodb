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

import com.amazonaws.retry.RetryMode
import com.amazonaws.{ ClientConfiguration => AWSClientConfiguration, DnsResolver, Protocol }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v1._
import com.github.j5ik2o.akka.persistence.dynamodb.config.ConfigSupport._
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ClassCheckUtils
import com.typesafe.config.Config

import scala.collection.immutable._
import scala.concurrent.duration.{ FiniteDuration, _ }

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
  val useSecureRandomKey                       = "use-secure-random"
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

  val DefaultConnectionTimeout: FiniteDuration      = AWSClientConfiguration.DEFAULT_CONNECTION_TIMEOUT.milliseconds
  val DefaultMaxConnections: Int                    = AWSClientConfiguration.DEFAULT_MAX_CONNECTIONS
  val DefaultV1RetryPolicyProviderClassName: String = classOf[RetryPolicyProvider.Default].getName
  val DefaultThrottleRetries: Boolean               = AWSClientConfiguration.DEFAULT_THROTTLE_RETRIES
  val DefaultSocketTimeout: FiniteDuration          = AWSClientConfiguration.DEFAULT_SOCKET_TIMEOUT.milliseconds
  val DefaultRequestTimeout: FiniteDuration         = AWSClientConfiguration.DEFAULT_REQUEST_TIMEOUT.milliseconds

  val DefaultClientExecutionTimeout: FiniteDuration =
    AWSClientConfiguration.DEFAULT_CLIENT_EXECUTION_TIMEOUT.milliseconds
  val DefaultUseReaper: Boolean                    = AWSClientConfiguration.DEFAULT_USE_REAPER
  val DefaultUseGZIP: Boolean                      = AWSClientConfiguration.DEFAULT_USE_GZIP
  val DefaultResponseMetadataCacheSize: Int        = AWSClientConfiguration.DEFAULT_RESPONSE_METADATA_CACHE_SIZE
  val DefaultSecureRandomProviderClassName: String = classOf[SecureRandomProvider.Default].getName
  val DefaultUseSecureRandom: Boolean              = false
  val DefaultUseExpectContinue: Boolean            = AWSClientConfiguration.DEFAULT_USE_EXPECT_CONTINUE
  val DefaultCacheResponseMetadata: Boolean        = AWSClientConfiguration.DEFAULT_CACHE_RESPONSE_METADATA
  val DefaultConnectionMaxIdle: FiniteDuration = AWSClientConfiguration.DEFAULT_CONNECTION_MAX_IDLE_MILLIS.milliseconds

  val DefaultValidateAfterInactivity: FiniteDuration =
    AWSClientConfiguration.DEFAULT_VALIDATE_AFTER_INACTIVITY_MILLIS.milliseconds
  val DefaultTcpKeepAlive: Boolean = AWSClientConfiguration.DEFAULT_TCP_KEEP_ALIVE

  val DefaultMaxConsecutiveRetiesBeforeThrottling: Int =
    AWSClientConfiguration.DEFAULT_MAX_CONSECUTIVE_RETRIES_BEFORE_THROTTLING
  val DnsResolverProviderClassName: String = classOf[DnsResolverProvider.Default].getName

  def fromConfig(config: Config): ClientConfiguration = {
    ClientConfiguration(
      connectionTimeout = config
        .valueAs[FiniteDuration](connectionTimeoutKey, DefaultConnectionTimeout),
      maxConnections = config.valueAs[Int](maxConnectionsKey, DefaultMaxConnections),
      maxErrorRetry = config.valueOptAs[Int](maxErrorRetryKey),
      retryMode = config.valueOptAs[String](retryModeKey).map(s => RetryMode.valueOf(s)),
      retryPolicyProviderClassName = {
        val className = config
          .valueOptAs[String](retryPolicyProviderClassNameKey).orElse(Some(DefaultV1RetryPolicyProviderClassName))
        ClassCheckUtils.requireClass(classOf[RetryPolicyProvider], className)
      },
      throttleRetries = config.valueAs[Boolean](throttleRetriesKey, DefaultThrottleRetries),
      localAddress = config.valueOptAs[String](localAddressKey),
      protocol = config.valueOptAs[String](protocolKey).map(s => Protocol.valueOf(s)),
      socketTimeout = config.valueAs[FiniteDuration](socketTimeoutKey, DefaultSocketTimeout),
      requestTimeout = config.valueAs[FiniteDuration](requestTimeoutKey, DefaultRequestTimeout),
      clientExecutionTimeout = config.valueAs[FiniteDuration](clientExecutionTimeoutKey, DefaultClientExecutionTimeout),
      userAgentPrefix = config.valueOptAs[String](userAgentPrefixKey),
      userAgentSuffix = config.valueOptAs[String](userAgentSuffixKey),
      useReaper = config.valueAs[Boolean](useReaper, DefaultUseReaper),
      useGzip = config.valueAs[Boolean](useGzip, DefaultUseGZIP),
      socketBufferSizeHint = {
        (
          config.valueOptAs[Int](socketSendBufferSizeHintKey),
          config.valueOptAs[Int](socketReceiveBufferSizeHint)
        ) match {
          case (Some(s), Some(r)) => Some(SocketSendBufferSizeHint(s, r))
          case _                  => None
        }
      },
      signerOverride = config.valueOptAs[String](signerOverrideKey),
      responseMetadataCacheSize = config.valueAs[Int](responseMetadataCacheSizeKey, DefaultResponseMetadataCacheSize),
      dnsResolverProviderClassName = {
        val className =
          config.valueAs[String](dnsResolverProviderClassNameKey, DnsResolverProviderClassName)
        ClassCheckUtils.requireClass(classOf[DnsResolverProvider], className)
      },
      dnsResolverClassName = {
        val className = config.valueOptAs[String](dnsResolverClassNameKey)
        ClassCheckUtils.requireClass(classOf[DnsResolver], className)
      },
      secureRandomProviderClassName = {
        val className =
          config.valueAs[String](secureRandomProviderClassNameKey, DefaultSecureRandomProviderClassName)
        ClassCheckUtils.requireClass(classOf[SecureRandomProvider], className)
      },
      useSecureRandom = config.valueAs(useSecureRandomKey, DefaultUseSecureRandom),
      useExpectContinue = config.valueAs[Boolean](useExpectContinueKey, DefaultUseExpectContinue),
      cacheResponseMetadata = config.valueAs[Boolean](cacheResponseMetadataKey, DefaultCacheResponseMetadata),
      connectionTtl = config.valueOptAs[Duration](connectionTtlKey),
      connectionMaxIdle = config
        .valueAs[FiniteDuration](connectionMaxIdleKey, DefaultConnectionMaxIdle),
      validateAfterInactivity =
        config.valueAs[FiniteDuration](validateAfterInactivityKey, DefaultValidateAfterInactivity),
      tcpKeepAlive = config.valueAs[Boolean](tcpKeepAliveKey, DefaultTcpKeepAlive),
      headers = config.valueAs[Map[String, String]](headersKey, Map.empty),
      maxConsecutiveRetriesBeforeThrottling =
        config.valueAs[Int](maxConsecutiveRetriesBeforeThrottlingKey, DefaultMaxConsecutiveRetiesBeforeThrottling),
      disableHostPrefixInjection = config.valueOptAs[Boolean](disableHostPrefixInjectionKey),
      proxyProtocol = config.valueOptAs[String](proxyProtocolKey),
      proxyHost = config.valueOptAs[String](proxyHostKey),
      proxyPort = config.valueOptAs[Int](proxyPortKey),
      disableSocketProxy = config.valueOptAs[Boolean](disableSocketProxyKey),
      proxyUsername = config.valueOptAs[String](proxyUsernameKey),
      proxyPassword = config.valueOptAs[String](proxyPasswordKey),
      proxyDomain = config.valueOptAs[String](proxyDomainKey),
      proxyWorkstation = config.valueOptAs[String](proxyWorkstationKey),
      nonProxyHosts = config.valueOptAs[String](nonProxyHostsKey),
      proxyAuthenticationMethods = config.valueAs[Seq[String]](proxyAuthenticationMethodsKey, Seq.empty)
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
