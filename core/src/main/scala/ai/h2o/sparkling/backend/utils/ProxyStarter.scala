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

package ai.h2o.sparkling.backend.utils

import java.net._

import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.SparkEnv
import org.apache.spark.expose.Logging
import org.apache.spark.h2o.H2OConf
import org.eclipse.jetty.server.Server
import water.H2O
import water.init.NetworkInit
import water.webserver.H2OHttpViewImpl
import water.webserver.jetty9.SparklingWaterJettyHelper

object ProxyStarter extends Logging {
  private var server: Server = _
  def startFlowProxy(conf: H2OConf): URI = {
    var port = findFlowProxyBasePort(conf)
    while (true) {
      try {
        val config = NetworkInit.webServerConfig(confToH2OArgs(conf))
        val h2oHttpView = new H2OHttpViewImpl(config)
        val helper = new SparklingWaterJettyHelper(conf, h2oHttpView)
        port = findNextFreeFlowPort(conf.clientWebPort, port)
        server = helper.startServer(port)
        return new URI(
          s"${conf.getScheme()}://${SparkEnv.get.blockManager.blockManagerId.host}:$port${conf.contextPath.getOrElse("")}")
      } catch {
        case _: BindException => port = port + 1
      }
    }
    throw new RuntimeException(s"Could not find any free port for the Flow proxy!")
  }

  private def confToH2OArgs(conf: H2OConf): H2O.OptArgs = {
    val args = new H2O.OptArgs
    args.jks = conf.jks.orNull
    args.jks_pass = conf.jksPass.orNull
    args.jks_alias = conf.jksAlias.orNull
    args.login_conf = conf.loginConf.orNull
    args.user_name = conf.userName.orNull
    args.context_path = conf.contextPath.orNull
    args.hash_login = conf.hashLogin
    args.ldap_login = conf.ldapLogin
    args.kerberos_login = conf.kerberosLogin
    args.embedded = true
    args
  }

  def stopFlowProxy(): Unit =
    try {
      server.stop()
    } catch {
      case _: Throwable =>
    }

  /**
    * In several scenarios we know that the port is likely to be occupied by H2O, so we can
    * start from higher port number right away
    */
  private def findFlowProxyBasePort(conf: H2OConf): Int = {
    // Regular expression used for local[N] and local[*] master formats
    val LOCAL_N_REGEX = """local\[([0-9]+|\*)\]""".r
    // Regular expression for local[N, maxRetries], used in tests with failing tasks
    val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+|\*)\s*,\s*([0-9]+)\]""".r
    // Regular expression for simulating a Spark cluster of [N, cores, memory] locally
    val LOCAL_CLUSTER_REGEX = """local-cluster\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*]""".r
    val LOCAL = "local"
    val master = SparkSessionUtils.active.sparkContext.master
    val numSkipped = if (conf.runsInInternalClusterMode && conf.internalPortOffset == 1) {
      master match {
        // In local mode of internal backend, we always run H2O in Spark driver and the base port is
        // always occupied by that node. In local-cluster mode of internal backend, we always run
        // several H2O nodes on the same machine and the ports are always occupied
        case LOCAL => 2
        case LOCAL_N_REGEX(_) => 2
        case LOCAL_N_FAILURES_REGEX(_, _) => 2
        case LOCAL_CLUSTER_REGEX(nodes, _, _) => nodes.toInt * 2
        case _ => 1
      }
    } else {
      1
    }
    conf.clientBasePort + numSkipped
  }

  private def isTcpPortAvailable(port: Int): Boolean = {
    scala.util
      .Try {
        val serverSocket = new ServerSocket()
        serverSocket.setReuseAddress(false)
        val host = SparkEnv.get.blockManager.blockManagerId.host
        val socketAddress = new InetSocketAddress(InetAddress.getByName(host), port)
        serverSocket.bind(socketAddress, 1)
        serverSocket.close()
        true
      }
      .getOrElse(false)
  }

  private def findNextFreeFlowPort(clientWebPort: Int, clientBasePort: Int): Int = {
    if (clientWebPort == -1) {
      var port = clientBasePort
      while (!isTcpPortAvailable(port)) {
        logWarning(s"Tried using port $port for Flow proxy, but port was already occupied!")
        port = port + 1
      }
      port
    } else {
      val port = clientWebPort
      if (!isTcpPortAvailable(port)) {
        throw new RuntimeException(
          s"Explicitly specified client web port $port is already occupied, please specify a free port!")
      } else {
        port
      }
    }
  }
}
