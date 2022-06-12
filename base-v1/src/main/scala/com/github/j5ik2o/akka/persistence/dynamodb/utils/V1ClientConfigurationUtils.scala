/*
 * Copyright 2022 Junichi Kato
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
package com.github.j5ik2o.akka.persistence.dynamodb.utils

import akka.actor.DynamicAccess
import com.amazonaws.retry.RetryMode
import com.amazonaws.{ ClientConfiguration, Protocol, ProxyAuthenticationMethod }
import com.github.j5ik2o.akka.persistence.dynamodb.client.v1.{
  DnsResolverProvider,
  RetryPolicyProvider,
  SecureRandomProvider
}
import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig

import java.net.InetAddress
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

private[utils] object V1ClientConfigurationUtils {

  def setup(dynamicAccess: DynamicAccess, pluginConfig: PluginConfig): ClientConfiguration = {
    val result = new ClientConfiguration()
    import pluginConfig.clientConfig.v1ClientConfig.clientConfiguration._
    protocol.foreach {
      case com.github.j5ik2o.akka.persistence.dynamodb.config.client.v1.Protocol.HTTP =>
        result.setProtocol(Protocol.HTTP)
      case com.github.j5ik2o.akka.persistence.dynamodb.config.client.v1.Protocol.HTTPS =>
        result.setProtocol(Protocol.HTTPS)
    }
    result.setMaxConnections(maxConnections)
    userAgentPrefix.foreach { v => result.setUserAgentPrefix(v) }
    userAgentSuffix.foreach { v => result.setUserAgentSuffix(v) }
    localAddress.foreach { v => result.setLocalAddress(InetAddress.getByName(v)) }
    proxyProtocol.foreach { v => result.setProtocol(Protocol.valueOf(v)) }
    proxyHost.foreach { v => result.setProxyHost(v) }
    proxyPort.foreach { v => result.setProxyPort(v) }
    disableSocketProxy.foreach { v => result.setDisableSocketProxy(v) }
    proxyUsername.foreach { v => result.setProxyUsername(v) }
    proxyPassword.foreach { v => result.setProxyPassword(v) }
    proxyDomain.foreach { v => result.setProxyDomain(v) }
    proxyWorkstation.foreach { v => result.setProxyWorkstation(v) }
    nonProxyHosts.foreach { v => result.setNonProxyHosts(v) }
    if (proxyAuthenticationMethods.nonEmpty) {
      val seq = proxyAuthenticationMethods.map(ProxyAuthenticationMethod.valueOf)
      result.setProxyAuthenticationMethods(seq.asJava)
    }
    RetryPolicyProvider.create(dynamicAccess, pluginConfig).foreach { p => result.setRetryPolicy(p.create) }
    maxErrorRetry.foreach { v => result.setMaxErrorRetry(v) }
    retryMode.foreach {
      case com.github.j5ik2o.akka.persistence.dynamodb.config.client.RetryMode.LEGACY =>
        result.setRetryMode(RetryMode.LEGACY)
      case com.github.j5ik2o.akka.persistence.dynamodb.config.client.RetryMode.STANDARD =>
        result.setRetryMode(RetryMode.STANDARD)
      case com.github.j5ik2o.akka.persistence.dynamodb.config.client.RetryMode.ADAPTIVE =>
        result.setRetryMode(RetryMode.ADAPTIVE)
    }
    if (socketTimeout != Duration.Zero)
      result.setSocketTimeout(socketTimeout.toMillis.toInt)
    if (connectionTimeout != Duration.Zero)
      result
        .setConnectionTimeout(connectionTimeout.toMillis.toInt)
    result
      .setRequestTimeout(requestTimeout.toMillis.toInt)
    if (clientExecutionTimeout != Duration.Zero)
      result
        .setClientExecutionTimeout(clientExecutionTimeout.toMillis.toInt)
    result.setUseReaper(useReaper)

    // * public ClientConfiguration withThrottledRetries(boolean use) {
    result.setMaxConsecutiveRetriesBeforeThrottling(maxConsecutiveRetriesBeforeThrottling)
    result.setUseGzip(useGzip)
    socketBufferSizeHint.foreach { v => result.setSocketBufferSizeHints(v.send, v.receive) }
    signerOverride.foreach { v => result.setSignerOverride(v) }
    // * public ClientConfiguration withPreemptiveBasicProxyAuth(boolean preemptiveBasicProxyAuth) {
    connectionTtl.foreach { v =>
      if (v != Duration.Zero)
        result.setConnectionTTL(v.toMillis)
    }
    if (connectionMaxIdle != Duration.Zero)
      result.setConnectionMaxIdleMillis(connectionMaxIdle.toMillis)
    if (validateAfterInactivity != Duration.Zero)
      result.setValidateAfterInactivityMillis(validateAfterInactivity.toMillis.toInt)
    result.setUseTcpKeepAlive(tcpKeepAlive)
    val dnsResolverProvider = DnsResolverProvider.create(dynamicAccess, pluginConfig)
    dnsResolverProvider.create.foreach { dnsResolver => result.setDnsResolver(dnsResolver) }
    result.setCacheResponseMetadata(cacheResponseMetadata)
    result.setResponseMetadataCacheSize(responseMetadataCacheSize)
    if (useSecureRandom) {
      val secureRandomProvider = SecureRandomProvider.create(dynamicAccess, pluginConfig)
      result.setSecureRandom(secureRandomProvider.create)
    }
    result.setUseExpectContinue(useExpectContinue)
    headers.foreach { case (k, v) =>
      result.addHeader(k, v)
    }
    disableHostPrefixInjection.foreach { v => result.setDisableHostPrefixInjection(v) }
    // * public ClientConfiguration withTlsKeyManagersProvider(TlsKeyManagersProvider tlsKeyManagersProvider) {

    result
  }

}
