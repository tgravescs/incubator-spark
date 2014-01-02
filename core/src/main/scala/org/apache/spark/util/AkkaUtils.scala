/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import scala.concurrent.duration.{Duration, FiniteDuration}

import akka.actor.{ActorSystem, ExtendedActorSystem, IndestructibleActorSystem}
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.{Logging, SecurityManager}

/**
 * Various utility classes for working with Akka.
 */
private[spark] object AkkaUtils extends Logging {

  /**
   * Creates an ActorSystem ready for remoting, with various Spark features. Returns both the
   * ActorSystem itself and its port (which is hard to get from Akka).
   *
   * Note: the `name` parameter is important, as even if a client sends a message to right
   * host + port, if the system name is incorrect, Akka will drop the message.
   *
   * If indestructible is set to true, the Actor System will continue running in the event
   * of a fatal exception. This is used by [[org.apache.spark.executor.Executor]].
   */
  def createActorSystem(name: String, host: String, port: Int, indestructible: Boolean = false,
    conf: SparkConf, securityManager: SecurityManager): (ActorSystem, Int) = {

    val akkaThreads   = conf.get("spark.akka.threads", "4").toInt
    val akkaBatchSize = conf.get("spark.akka.batchSize", "15").toInt

    val akkaTimeout = conf.get("spark.akka.timeout", "100").toInt

    val akkaFrameSize = conf.get("spark.akka.frameSize", "10").toInt
    val lifecycleEvents =
      if (conf.get("spark.akka.logLifecycleEvents", "false").toBoolean) "on" else "off"

    val akkaHeartBeatPauses = conf.get("spark.akka.heartbeat.pauses", "600").toInt
    val akkaFailureDetector =
      conf.get("spark.akka.failure-detector.threshold", "300.0").toDouble
    val akkaHeartBeatInterval = conf.get("spark.akka.heartbeat.interval", "1000").toInt

    val secretKey = securityManager.getSecretKey()
    val isAuthOn = securityManager.isAuthenticationEnabled()
    if (isAuthOn && secretKey == null) {
      throw new Exception("Secret key is null with authentication on")
    }
    val requireCookie = if (isAuthOn) "on" else "off"
    val secureCookie = if (isAuthOn) secretKey else ""
    logDebug("In createActorSystem, requireCookie is: " + requireCookie)

    val akkaConf = ConfigFactory.parseString(
      s"""
      |akka.daemonic = on
      |akka.loggers = [""akka.event.slf4j.Slf4jLogger""]
      |akka.stdout-loglevel = "ERROR"
      |akka.jvm-exit-on-fatal-error = off
      |akka.remote.transport-failure-detector.heartbeat-interval = $akkaHeartBeatInterval s
      |akka.remote.transport-failure-detector.acceptable-heartbeat-pause = $akkaHeartBeatPauses s
      |akka.remote.transport-failure-detector.threshold = $akkaFailureDetector
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
      |akka.remote.netty.tcp.hostname = "$host"
      |akka.remote.netty.tcp.port = $port
      |akka.remote.netty.tcp.tcp-nodelay = on
      |akka.remote.netty.tcp.connection-timeout = $akkaTimeout s
      |akka.remote.netty.tcp.maximum-frame-size = ${akkaFrameSize}MiB
      |akka.remote.netty.tcp.execution-pool-size = $akkaThreads
      |akka.actor.default-dispatcher.throughput = $akkaBatchSize
      |akka.remote.log-remote-lifecycle-events = $lifecycleEvents
      |akka.remote.netty.require-cookie = $requireCookie
      |akka.remote.netty.secure-cookie = $secureCookie
      """.stripMargin)

    val actorSystem = if (indestructible) {
      IndestructibleActorSystem(name, akkaConf)
    } else {
      ActorSystem(name, akkaConf)
    }

    val provider = actorSystem.asInstanceOf[ExtendedActorSystem].provider
    val boundPort = provider.getDefaultAddress.port.get
    (actorSystem, boundPort)
  }

  /** Returns the default Spark timeout to use for Akka ask operations. */
  def askTimeout(conf: SparkConf): FiniteDuration = {
    Duration.create(conf.get("spark.akka.askTimeout", "30").toLong, "seconds")
  }
}
