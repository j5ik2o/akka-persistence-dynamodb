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

import com.github.j5ik2o.akka.persistence.dynamodb.config.client.{ CommonConfigKeys, RetryMode }
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ClassCheckUtils
import net.ceedubs.ficus.Ficus._
import com.typesafe.config.Config

import scala.collection.immutable._
import scala.concurrent.duration._

object ClientConfiguration {
  val maxErrorRetryKey                 = "max-error-retry"
  val retryPolicyProviderClassNameKey  = "retry-policy-provider-class-name"
  val useThrottleRetriesKey            = "use-throttle-retries"
  val localAddressKey                  = "local-address"
  val protocolKey                      = "protocol"
  val clientExecutionTimeoutKey        = "client-execution-timeout"
  val userAgentPrefixKey               = "user-agent-prefix"
  val userAgentSuffixKey               = "user-agent-suffix"
  val useReaper                        = "use-reaper"
  val useGzip                          = "use-gzip"
  val socketSendBufferSizeHintKey      = "socket-send-buffer-size-hint"
  val socketReceiveBufferSizeHint      = "socket-receive-buffer-size-hint"
  val signerOverrideKey                = "signer-override"
  val responseMetadataCacheSizeKey     = "response-metadata-cache-size"
  val dnsResolverProviderClassNameKey  = "dns-resolver-provider-class-name"
  val dnsResolverClassNameKey          = "dns-resolver-class-name"
  val secureRandomProviderClassNameKey = "secure-random-provider-class-name"
  val useSecureRandomKey               = "use-secure-random"
  val useExpectContinueKey             = "use-expect-continue"
  val cacheResponseMetadataKey         = "cache-response-metadata"
  val connectionMaxIdleKey             = "connection-max-idle"
  val validateAfterInactivityKey       = "validate-after-inactivity"
  val useTcpKeepAliveKey               = "use-tcp-keep-alive"
  val maxConsecutiveRetriesBeforeThrottlingKey =
    "max-consecutive-retries-before-throttling"
  val disableHostPrefixInjectionKey = "disable-host-prefix-injection"
  val proxyProtocolKey              = "proxy-protocol"
  val proxyHostKey                  = "proxy-host"
  val proxyPortKey                  = "proxy-port"
  val disableSocketProxyKey         = "disable-socket-proxy"
  val proxyUsernameKey              = "proxy-username"
  val proxyPasswordKey              = "proxy-password"
  val proxyDomainKey                = "proxy-domain"
  val proxyWorkstationKey           = "proxy-workstation"
  val nonProxyHostsKey              = "non-proxy-hosts"
  val proxyAuthenticationMethodsKey = "proxy-authentication-methods"

  val RetryPolicyProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.RetryPolicyProvider"
  val DnsResolverProviderClassName: String =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.DnsResolverProvider"
  val DnsResolverClassName = "com.amazonaws.DnsResolver"
  val SecureRandomProviderClassName =
    "com.github.j5ik2o.akka.persistence.dynamodb.client.v1.SecureRandomProvider"

  def fromConfig(
      config: Config,
      classNameValidation: Boolean
  ): ClientConfiguration = {
    ClientConfiguration(
      connectionTimeout = config.as[FiniteDuration](CommonConfigKeys.connectionTimeoutKey),
      maxConnections = config.as[Int](CommonConfigKeys.maxConnectionsKey),
      maxErrorRetry = config.getAs[Int](maxErrorRetryKey),
      retryMode = config
        .getAs[String](CommonConfigKeys.retryModeKey)
        .map(s => RetryMode.withName(s.toUpperCase)),
      retryPolicyProviderClassName = {
        val className = config.as[String](retryPolicyProviderClassNameKey)
        ClassCheckUtils.requireClassByName(RetryPolicyProviderClassName, className, classNameValidation)
      },
      useThrottleRetries = config.as[Boolean](useThrottleRetriesKey),
      localAddress = config.getAs[String](localAddressKey),
      protocol = config
        .getAs[String](protocolKey)
        .map(s => Protocol.withName(s.toUpperCase)),
      socketTimeout = config.as[FiniteDuration](CommonConfigKeys.socketTimeoutKey),
      requestTimeout = config.as[FiniteDuration](CommonConfigKeys.requestTimeoutKey),
      clientExecutionTimeout = config.as[FiniteDuration](clientExecutionTimeoutKey),
      userAgentPrefix = config.getAs[String](userAgentPrefixKey),
      userAgentSuffix = config.getAs[String](userAgentSuffixKey),
      useReaper = config.as[Boolean](useReaper),
      useGzip = config.as[Boolean](useGzip),
      socketBufferSizeHint = {
        (
          config.getAs[Int](socketSendBufferSizeHintKey),
          config.getAs[Int](socketReceiveBufferSizeHint)
        ) match {
          case (Some(s), Some(r)) => Some(SocketSendBufferSizeHint(s, r))
          case _                  => None
        }
      },
      signerOverride = config.getAs[String](signerOverrideKey),
      responseMetadataCacheSize = config.as[Int](responseMetadataCacheSizeKey),
      dnsResolverProviderClassName = {
        val className =
          config.as[String](dnsResolverProviderClassNameKey)
        ClassCheckUtils
          .requireClassByName(
            DnsResolverProviderClassName,
            className,
            classNameValidation
          )
      },
      dnsResolverClassName = {
        val className = config.getAs[String](dnsResolverClassNameKey)
        ClassCheckUtils.requireClassByName(
          DnsResolverClassName,
          className,
          classNameValidation
        )
      },
      secureRandomProviderClassName = {
        val className =
          config.as[String](secureRandomProviderClassNameKey)
        ClassCheckUtils
          .requireClassByName(
            SecureRandomProviderClassName,
            className,
            classNameValidation
          )
      },
      useSecureRandom = config.as[Boolean](useSecureRandomKey),
      useExpectContinue = config.as[Boolean](useExpectContinueKey),
      cacheResponseMetadata = config.as[Boolean](
        cacheResponseMetadataKey
      ),
      connectionTtl = config.getAs[Duration](CommonConfigKeys.connectionTtlKey),
      connectionMaxIdle = config.as[FiniteDuration](connectionMaxIdleKey),
      validateAfterInactivity = config.as[FiniteDuration](validateAfterInactivityKey),
      useTcpKeepAlive = config.as[Boolean](useTcpKeepAliveKey),
      headers = config
        .getAs[Map[String, String]](CommonConfigKeys.headersKey).getOrElse(Map.empty),
      maxConsecutiveRetriesBeforeThrottling = config.as[Int](
        maxConsecutiveRetriesBeforeThrottlingKey
      ),
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
      proxyAuthenticationMethods = config.getAs[Seq[String]](proxyAuthenticationMethodsKey).getOrElse(Seq.empty)
    )
  }
}

final case class ClientConfiguration(
    connectionTimeout: FiniteDuration,
    maxConnections: Int,
    maxErrorRetry: Option[Int],
    retryMode: Option[RetryMode.Value],
    retryPolicyProviderClassName: String,
    useThrottleRetries: Boolean,
    localAddress: Option[String],
    protocol: Option[Protocol.Value],
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
    useTcpKeepAlive: Boolean,
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
