/*
 * Copyright 2019 Junichi Kato
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

import java.net.InetAddress
import java.time.{ Duration => JavaDuration }

import com.github.j5ik2o.akka.persistence.dynamodb.config.PluginConfig
import software.amazon.awssdk.http.Protocol
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.http.nio.netty.{ Http2Configuration, NettyNioAsyncHttpClient, SdkEventLoopGroup }

import scala.concurrent.duration.Duration

object V2HttpClientBuilderUtils {

  def setupSync(pluginConfig: PluginConfig): ApacheHttpClient.Builder = {
    import pluginConfig.clientConfig.v2ClientConfig.syncClientConfig._
    val result = ApacheHttpClient.builder()

    if (socketTimeout != Duration.Zero)
      result.socketTimeout(JavaDuration.ofMillis(socketTimeout.toMillis))
    if (socketTimeout != Duration.Zero)
      result.connectionTimeout(JavaDuration.ofMillis(connectionTimeout.toMillis))
    if (socketTimeout != Duration.Zero)
      result.connectionAcquisitionTimeout(JavaDuration.ofMillis(connectionAcquisitionTimeout.toMillis))

    result.maxConnections(maxConnections)

    localAddress.foreach { v => result.localAddress(InetAddress.getByName(v)) }
    expectContinueEnabled.foreach { v => result.expectContinueEnabled(v) }

    if (connectionTimeToLive != Duration.Zero)
      result.connectionTimeToLive(JavaDuration.ofMillis(connectionTimeToLive.toMillis))
    if (maxIdleConnectionTimeout != Duration.Zero)
      result.connectionMaxIdleTime(JavaDuration.ofMillis(maxIdleConnectionTimeout.toMillis))

    result.useIdleConnectionReaper(useConnectionReaper)

//    Builder httpRoutePlanner(HttpRoutePlanner proxyConfiguration);
//    Builder credentialsProvider(CredentialsProvider credentialsProvider);
//    Builder tlsKeyManagersProvider(TlsKeyManagersProvider tlsKeyManagersProvider);
//    Builder tlsTrustManagersProvider(TlsTrustManagersProvider tlsTrustManagersProvider);
    result
  }

  def setupAsync(pluginConfig: PluginConfig): NettyNioAsyncHttpClient.Builder = {
    val result = NettyNioAsyncHttpClient.builder()
    import pluginConfig.clientConfig.v2ClientConfig.asyncClientConfig._
    result.maxConcurrency(maxConcurrency)
    result.maxPendingConnectionAcquires(maxPendingConnectionAcquires)

    if (readTimeout != Duration.Zero)
      result.readTimeout(JavaDuration.ofMillis(readTimeout.toMillis))
    if (writeTimeout != Duration.Zero)
      result.writeTimeout(JavaDuration.ofMillis(writeTimeout.toMillis))
    if (connectionTimeout != Duration.Zero)
      result.connectionTimeout(JavaDuration.ofMillis(connectionTimeout.toMillis))
    if (connectionAcquisitionTimeout != Duration.Zero)
      result.connectionAcquisitionTimeout(JavaDuration.ofMillis(connectionAcquisitionTimeout.toMillis))
    if (connectionTimeToLive != Duration.Zero)
      result.connectionTimeToLive(JavaDuration.ofMillis(connectionTimeToLive.toMillis))
    if (maxIdleConnectionTimeout != Duration.Zero)
      result.connectionMaxIdleTime(JavaDuration.ofMillis(maxIdleConnectionTimeout.toMillis))

    result.useIdleConnectionReaper(useConnectionReaper)
    if (useHttp2)
      result.protocol(Protocol.HTTP2)
    else
      result.protocol(Protocol.HTTP1_1)
    val http2Builder = Http2Configuration.builder()
    http2Builder.maxStreams(http2MaxStreams)
    http2Builder.initialWindowSize(http2InitialWindowSize)
    http2HealthCheckPingPeriod.foreach(v => http2Builder.healthCheckPingPeriod(JavaDuration.ofMillis(v.toMillis)))
    result.http2Configuration(http2Builder.build())
    threadsOfEventLoopGroup.foreach(v => result.eventLoopGroup(SdkEventLoopGroup.builder().numberOfThreads(v).build()))
    result

  }

}
