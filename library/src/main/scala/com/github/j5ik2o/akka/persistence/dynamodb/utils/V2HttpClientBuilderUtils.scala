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

import com.github.j5ik2o.akka.persistence.dynamodb.config.DynamoDBClientConfig
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.{ Http2Configuration, NettyNioAsyncHttpClient, SdkEventLoopGroup }
import software.amazon.awssdk.http.{ Protocol, SdkHttpClient }

object V2HttpClientBuilderUtils {

  def setupSync(clientConfig: DynamoDBClientConfig): SdkHttpClient = {
    val result = ApacheHttpClient.builder()
    import clientConfig.v2ClientConfig.syncClientConfig._
    result.maxConnections(maxConnections)
    localAddress.foreach { v => result.localAddress(InetAddress.getByName(v)) }
    expectContinueEnabled.foreach { v => result.expectContinueEnabled(v) }
    connectionTimeToLive.foreach { v => result.connectionTimeToLive(JavaDuration.ofMillis(v.toMillis)) }
    maxIdleConnectionTimeout.foreach { v => result.connectionMaxIdleTime(JavaDuration.ofMillis(v.toMillis)) }
    useConnectionReaper.foreach { v => result.useIdleConnectionReaper(v) }
//    Builder httpRoutePlanner(HttpRoutePlanner proxyConfiguration);
//    Builder credentialsProvider(CredentialsProvider credentialsProvider);
//    Builder tlsKeyManagersProvider(TlsKeyManagersProvider tlsKeyManagersProvider);
//    Builder tlsTrustManagersProvider(TlsTrustManagersProvider tlsTrustManagersProvider);
    result.build()
  }

  def setupAsync(clientConfig: DynamoDBClientConfig): SdkAsyncHttpClient = {
    val result = NettyNioAsyncHttpClient.builder()
    import clientConfig.v2ClientConfig.asyncClientConfig._
    maxConcurrency.foreach(v => result.maxConcurrency(v))
    maxPendingConnectionAcquires.foreach(v => result.maxPendingConnectionAcquires(v))
    readTimeout.foreach(v => result.readTimeout(JavaDuration.ofMillis(v.toMillis)))
    writeTimeout.foreach(v => result.writeTimeout(JavaDuration.ofMillis(v.toMillis)))
    connectionTimeout.foreach(v => result.connectionTimeout(JavaDuration.ofMillis(v.toMillis)))
    connectionAcquisitionTimeout.foreach(v => result.connectionAcquisitionTimeout(JavaDuration.ofMillis(v.toMillis)))
    connectionTimeToLive.foreach(v => result.connectionTimeToLive(JavaDuration.ofMillis(v.toMillis)))
    maxIdleConnectionTimeout.foreach(v => result.connectionMaxIdleTime(JavaDuration.ofMillis(v.toMillis)))
    useConnectionReaper.foreach(v => result.useIdleConnectionReaper(v))
    useHttp2.foreach(v => if (v) result.protocol(Protocol.HTTP2) else result.protocol(Protocol.HTTP1_1))
    val http2Builder = Http2Configuration.builder()
    http2MaxStreams.foreach(v => http2Builder.maxStreams(v))
    http2InitialWindowSize.foreach(v => http2Builder.initialWindowSize(v))
    http2HealthCheckPingPeriod.foreach(v => http2Builder.healthCheckPingPeriod(JavaDuration.ofMillis(v.toMillis)))
    result.http2Configuration(http2Builder.build())
    threadsOfEventLoopGroup.foreach(v => result.eventLoopGroup(SdkEventLoopGroup.builder().numberOfThreads(v).build()))
    result.build()
  }

}
