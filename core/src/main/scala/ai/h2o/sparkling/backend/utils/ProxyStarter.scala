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

import org.apache.spark.SparkEnv
import org.apache.spark.expose.Logging
import org.apache.spark.h2o.H2OConf
import org.spark_project.jetty.client.HttpClient
import org.spark_project.jetty.proxy.ProxyServlet.Transparent
import org.spark_project.jetty.server.{HttpConnectionFactory, Server, ServerConnector}
import org.spark_project.jetty.servlet.{ServletContextHandler, ServletHandler}
import org.spark_project.jetty.util.thread.{QueuedThreadPool, ScheduledExecutorScheduler, Scheduler}

object ProxyStarter extends Logging {
  def startFlowProxy(conf: H2OConf): URI = {
    var port = conf.clientBasePort
    while (true) {
      try {
        port = findNextFreeFlowPort(conf.clientWebPort, port + 1)
        val pool = new QueuedThreadPool()
        pool.setDaemon(true)
        val server = new Server(pool)
        val s = server.getBean(classOf[Scheduler])
        server.updateBean(s, new ScheduledExecutorScheduler(null, true))
        server.setHandler(getContextHandler(conf))
        val connector = new ServerConnector(server, new HttpConnectionFactory())
        connector.setPort(port)
        server.setConnectors(Array(connector))
        // the port discovered by findNextFreeFlowPort(conf) might get occupied since we discovered it
        server.start()
        return new URI(s"${conf.getScheme()}://${SparkEnv.get.blockManager.blockManagerId.host}:$port${conf.contextPath.getOrElse("")}")
      } catch {
        case _: BindException =>
      }
    }
    throw new RuntimeException(s"Could not find any free port for the Flow proxy!")
  }


  private def getContextHandler(conf: H2OConf): ServletContextHandler = {
    val context = new ServletContextHandler(ServletContextHandler.SESSIONS)
    context.setContextPath("/")
    val handler = new ServletHandler()
    val holder = handler.addServletWithMapping(classOf[H2OFlowProxyServlet], "/*")

    val ipPort = RestApiUtils.getLeaderNode(conf).ipPort()
    holder.setInitParameter("proxyTo", s"${conf.getScheme()}://$ipPort${conf.contextPath.getOrElse("")}")
    holder.setInitParameter("prefix", conf.contextPath.getOrElse("/"))
    context.setServletHandler(handler)
    context
  }

  class H2OFlowProxyServlet extends Transparent {
    override def newHttpClient(): HttpClient = {
      val client = super.newHttpClient()
      client.setScheduler(new ScheduledExecutorScheduler(null, true))
      client
    }
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
        throw new RuntimeException(s"Explicitly specified client web port $port is already occupied, please specify a free port!")
      } else {
        port
      }
    }
  }
}
