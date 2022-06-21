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
import com.github.j5ik2o.akka.persistence.dynamodb.utils.ConfigOps._
import com.typesafe.config.Config

import scala.collection.immutable._
import scala.concurrent.duration._

object ClientConfiguration {
  val maxErrorRetryKey                 = "max-error-retry"
  val retryPolicyProviderClassNameKey  = "retry-policy-provider-class-name"
  val throttleRetriesKey               = "throttle-retries"
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
  val tcpKeepAliveKey                  = "tcp-keep-alive"
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
      connectionTimeout = config.value[FiniteDuration](CommonConfigKeys.connectionTimeoutKey),
      maxConnections = config.value[Int](CommonConfigKeys.maxConnectionsKey),
      maxErrorRetry = config.valueOptAs[Int](maxErrorRetryKey),
      retryMode = config
        .valueOptAs[String](CommonConfigKeys.retryModeKey)
        .map(s => RetryMode.withName(s.toUpperCase)),
      retryPolicyProviderClassName = {
        val className = config.value[String](retryPolicyProviderClassNameKey)
        ClassCheckUtils.requireClassByName(RetryPolicyProviderClassName, className, classNameValidation)
      },
      throttleRetries = config.value[Boolean](throttleRetriesKey),
      localAddress = config.valueOptAs[String](localAddressKey),
      protocol = config
        .valueOptAs[String](protocolKey)
        .map(s => Protocol.withName(s.toUpperCase)),
      socketTimeout = config.value[FiniteDuration](CommonConfigKeys.socketTimeoutKey),
      requestTimeout = config.value[FiniteDuration](CommonConfigKeys.requestTimeoutKey),
      clientExecutionTimeout = config.value[FiniteDuration](clientExecutionTimeoutKey),
      userAgentPrefix = config.valueOptAs[String](userAgentPrefixKey),
      userAgentSuffix = config.valueOptAs[String](userAgentSuffixKey),
      useReaper = config.value[Boolean](useReaper),
      useGzip = config.value[Boolean](useGzip),
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
      responseMetadataCacheSize = config.value[Int](responseMetadataCacheSizeKey),
      dnsResolverProviderClassName = {
        val className =
          config.value[String](dnsResolverProviderClassNameKey)
        ClassCheckUtils
          .requireClassByName(
            DnsResolverProviderClassName,
            className,
            classNameValidation
          )
      },
      dnsResolverClassName = {
        val className = config.valueOptAs[String](dnsResolverClassNameKey)
        ClassCheckUtils.requireClassByName(
          DnsResolverClassName,
          className,
          classNameValidation
        )
      },
      secureRandomProviderClassName = {
        val className =
          config.value[String](secureRandomProviderClassNameKey)
        ClassCheckUtils
          .requireClassByName(
            SecureRandomProviderClassName,
            className,
            classNameValidation
          )
      },
      useSecureRandom = config.value[Boolean](useSecureRandomKey),
      useExpectContinue = config.value[Boolean](useExpectContinueKey),
      cacheResponseMetadata = config.value[Boolean](
        cacheResponseMetadataKey
      ),
      connectionTtl = config.valueOptAs[Duration](CommonConfigKeys.connectionTtlKey),
      connectionMaxIdle = config.value[FiniteDuration](connectionMaxIdleKey),
      validateAfterInactivity = config.value[FiniteDuration](validateAfterInactivityKey),
      tcpKeepAlive = config.value[Boolean](tcpKeepAliveKey),
      headers = config
        .valueAs[Map[String, String]](CommonConfigKeys.headersKey, Map.empty),
      maxConsecutiveRetriesBeforeThrottling = config.value[Int](
        maxConsecutiveRetriesBeforeThrottlingKey
      ),
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

final case class ClientConfiguration(
    connectionTimeout: FiniteDuration,
    maxConnections: Int,
    maxErrorRetry: Option[Int],
    retryMode: Option[RetryMode.Value],
    retryPolicyProviderClassName: String,
    throttleRetries: Boolean,
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
