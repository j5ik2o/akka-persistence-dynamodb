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

import java.time.{ Duration => JavaDuration }

import com.github.j5ik2o.akka.persistence.dynamodb.config.DynamoDBClientConfig
import software.amazon.awssdk.http.Protocol
import software.amazon.awssdk.http.nio.netty.{ Http2Configuration, NettyNioAsyncHttpClient, SdkEventLoopGroup }

import scala.concurrent.duration.Duration

object HttpClientBuilderUtils {

  def setup(clientConfig: DynamoDBClientConfig): NettyNioAsyncHttpClient.Builder = {
    val result = NettyNioAsyncHttpClient.builder()
    result.maxConcurrency(clientConfig.maxConcurrency)
    result.maxPendingConnectionAcquires(clientConfig.maxPendingConnectionAcquires)

    if (clientConfig.readTimeout != Duration.Zero)
      result.readTimeout(JavaDuration.ofMillis(clientConfig.readTimeout.toMillis))
    if (clientConfig.writeTimeout != Duration.Zero)
      result.writeTimeout(JavaDuration.ofMillis(clientConfig.writeTimeout.toMillis))
    if (clientConfig.connectionTimeout != Duration.Zero)
      result.connectionTimeout(JavaDuration.ofMillis(clientConfig.connectionTimeout.toMillis))
    if (clientConfig.connectionAcquisitionTimeout != Duration.Zero)
      result.connectionAcquisitionTimeout(JavaDuration.ofMillis(clientConfig.connectionAcquisitionTimeout.toMillis))
    if (clientConfig.connectionTimeToLive != Duration.Zero)
      result.connectionTimeToLive(JavaDuration.ofMillis(clientConfig.connectionTimeToLive.toMillis))
    if (clientConfig.maxIdleConnectionTimeout != Duration.Zero)
      result.connectionMaxIdleTime(JavaDuration.ofMillis(clientConfig.maxIdleConnectionTimeout.toMillis))

    result.useIdleConnectionReaper(clientConfig.useConnectionReaper)
    if (clientConfig.useHttp2)
      result.protocol(Protocol.HTTP2)
    else
      result.protocol(Protocol.HTTP1_1)
    val http2Builder = Http2Configuration.builder()
    http2Builder.maxStreams(clientConfig.http2MaxStreams)
    http2Builder.initialWindowSize(clientConfig.http2InitialWindowSize)
    clientConfig.http2HealthCheckPingPeriod.foreach(v =>
      http2Builder.healthCheckPingPeriod(JavaDuration.ofMillis(v.toMillis))
    )
    result.http2Configuration(http2Builder.build())
    clientConfig.threadsOfEventLoopGroup.foreach(v =>
      result.eventLoopGroup(SdkEventLoopGroup.builder().numberOfThreads(v).build())
    )
    result
  }

}
