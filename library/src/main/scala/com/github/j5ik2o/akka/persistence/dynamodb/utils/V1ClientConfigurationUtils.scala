package com.github.j5ik2o.akka.persistence.dynamodb.utils

import java.net.InetAddress

import akka.actor.DynamicAccess
import com.amazonaws.{ ClientConfiguration, DnsResolver }
import com.github.j5ik2o.akka.persistence.dynamodb.config.client.DynamoDBClientConfig
import com.github.j5ik2o.akka.persistence.dynamodb.journal.dao.v1.{ V1RetryPolicyProvider, V1SecureRandomProvider }

import scala.collection.immutable.Seq

object V1ClientConfigurationUtils {

  def setup(dynamicAccess: DynamicAccess, dynamoDBClientConfig: DynamoDBClientConfig): ClientConfiguration = {
    val result = new ClientConfiguration()
    import dynamoDBClientConfig.v1ClientConfig._
    protocol.foreach { v => result.setProtocol(v) }
    result.setMaxConnections(maxConnections)
    userAgentPrefix.foreach { v => result.setUserAgentPrefix(v) }
    userAgentSuffix.foreach { v => result.setUserAgentSuffix(v) }
    localAddress.foreach { v => result.setLocalAddress(InetAddress.getByName(v)) }

    /**
      * * public void setProxyProtocol(Protocol proxyProtocol) {
      * * public void setProxyHost(String proxyHost) {
      * * public void setProxyPort(int proxyPort) {
      * * public void setDisableSocketProxy(boolean disableSocketProxy) {
      * * public void setProxyUsername(String proxyUsername) {
      * * public void setProxyPassword(String proxyPassword) {
      * * public void setProxyDomain(String proxyDomain) {
      * * public void setProxyWorkstation(String proxyWorkstation) {
      * * public void setNonProxyHosts(String nonProxyHosts) {
      * * public void setProxyAuthenticationMethods(List<ProxyAuthenticationMethod> proxyAuthenticationMethods) {
      */
    retryPolicyProviderClassName.foreach { v =>
      val retryPolicyProvider =
        dynamicAccess
          .createInstanceFor[V1RetryPolicyProvider](
            v,
            Seq(classOf[DynamoDBClientConfig] -> dynamoDBClientConfig)
          ).get
      result.setRetryPolicy(retryPolicyProvider.create)
    }
    maxErrorRetry.foreach { v => result.setMaxErrorRetry(v) }
    retryMode.foreach { v => result.setRetryMode(v) }
    result.setSocketTimeout(socketTimeout.toMillis.toInt)
    result
      .setConnectionTimeout(connectionTimeout.toMillis.toInt)
    result
      .setRequestTimeout(requestTimeout.toMillis.toInt)
    result
      .setClientExecutionTimeout(clientExecutionTimeout.toMillis.toInt)
    result.setUseReaper(useReaper)
    result.setMaxConsecutiveRetriesBeforeThrottling(maxConsecutiveRetriesBeforeThrottling)
    result.setUseGzip(useGzip)
    socketBufferSizeHint.foreach { v => result.setSocketBufferSizeHints(v.send, v.receive) }
    signerOverride.foreach { v => result.setSignerOverride(v) }
    // public void setPreemptiveBasicProxyAuth(Boolean preemptiveBasicProxyAuth) {
    connectionTtl.foreach { v => result.setConnectionTTL(v.toMillis) }
    result.setConnectionMaxIdleMillis(connectionMaxIdle.toMillis)
    result.setValidateAfterInactivityMillis(validateAfterInactivity.toMillis.toInt)
    result.setUseTcpKeepAlive(tcpKeepAlive)
    dnsResolverClassName.foreach { v =>
      val dnsResolver = dynamicAccess.createInstanceFor[DnsResolver](v, Seq.empty).get
      result.setDnsResolver(dnsResolver)
    }
    result.setCacheResponseMetadata(cacheResponseMetadata)
    result.setResponseMetadataCacheSize(responseMetadataCacheSize)
    secureRandomProviderClassName.foreach { v =>
      val retryPolicyProvider =
        dynamicAccess
          .createInstanceFor[V1SecureRandomProvider](
            v,
            Seq(classOf[DynamoDBClientConfig] -> dynamoDBClientConfig)
          ).get
      result.setSecureRandom(retryPolicyProvider.create)
    }
    result.setUseExpectContinue(useExpectContinue)
    headers.foreach {
      case (k, v) =>
        result.addHeader(k, v)
    }
    disableHostPrefixInjection.foreach { v => result.setDisableHostPrefixInjection(v) }
    // public void setTlsKeyManagersProvider(TlsKeyManagersProvider tlsKeyManagersProvider) {
    result
  }

}
