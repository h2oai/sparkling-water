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

package org.apache.spark.h2o.backends.external

import java.net.{InetAddress, InetSocketAddress, ServerSocket, URI}

import org.apache.spark.SparkEnv
import org.apache.spark.expose.Logging
import org.apache.spark.h2o.H2OConf
import org.eclipse.jetty.proxy.ProxyServlet
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHandler}

object ProxyStarter extends Logging {
  def startFlowProxy(conf: H2OConf): URI = {
    val port = findNextFreeFlowPort(conf)
    val server = new Server(port)

    val context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/")

    val handler = new ServletHandler()
    val holder = handler.addServletWithMapping(classOf[ProxyServlet.Transparent], "/*")

    val cloudV3 = RestApiUtils.getCloudInfo(conf)
    val ipPort = cloudV3.nodes(cloudV3.leader_idx).ip_port

    holder.setInitParameter("proxyTo", s"${conf.getScheme()}://$ipPort${conf.contextPath.getOrElse("")}")
    holder.setInitParameter("prefix", conf.contextPath.getOrElse("/"))
    context.setServletHandler(handler)
    server.setHandler(context)
    server.start()
    new URI(s"${conf.getScheme()}://${SparkEnv.get.blockManager.blockManagerId.host}:$port${conf.contextPath.getOrElse("")}")
  }

  private def isTcpPortAvailable(port: Int): Boolean = {
    scala.util.Try {
      val serverSocket = new ServerSocket()
      serverSocket.setReuseAddress(false)
      val host = SparkEnv.get.blockManager.blockManagerId.host
      val socketAddress = new InetSocketAddress(InetAddress.getByName(host), port)
      serverSocket.bind(socketAddress, 1)
      serverSocket.close()
      true
    }.getOrElse(false)
  }

  private def findNextFreeFlowPort(conf: H2OConf): Int = {
    if (conf.clientWebPort == -1) {
      var port = conf.clientBasePort
      while (!isTcpPortAvailable(port)) {
        logDebug(s"Tried using port $port for Flow proxy, but port was already occupied!")
        port = port + 1
      }
      port
    } else {
      val port = conf.clientWebPort
      if (!isTcpPortAvailable(port)) {
        throw new RuntimeException(s"Explicitly specified client web port $port is already occupied, please specify a free port!")
      } else {
        port
      }
    }
  }
}
