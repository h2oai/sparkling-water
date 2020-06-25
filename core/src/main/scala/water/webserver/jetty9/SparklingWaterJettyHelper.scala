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
package water.webserver.jetty9

import ai.h2o.sparkling.backend.api.ShutdownServlet
import ai.h2o.sparkling.backend.api.dataframes.DataFramesServlet
import ai.h2o.sparkling.backend.api.h2oframes.H2OFramesServlet
import ai.h2o.sparkling.backend.api.rdds.RDDsServlet
import ai.h2o.sparkling.backend.api.scalainterpreter.ScalaInterpreterServlet
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.proxy.ProxyServlet.Transparent
import org.eclipse.jetty.security.SecurityHandler
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHandler, ServletHolder, ServletMapping}
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler
import water.webserver.iface.{H2OHttpView, LoginType}

class SparklingWaterJettyHelper(hc: H2OContext, conf: H2OConf, h2oHttpView: H2OHttpView)
  extends Jetty9Helper(h2oHttpView) {

  override def createServletContextHandler(): ServletContextHandler = {
    val context = new ServletContextHandler(ServletContextHandler.NO_SECURITY | ServletContextHandler.NO_SESSIONS)
    context.setContextPath(conf.contextPath.getOrElse("/"))
    context.setServletHandler(proxyContextHandler(conf))
    if (conf.isH2OReplEnabled) {
      ScalaInterpreterServlet.register(context, conf, hc)
    }
    RDDsServlet.register(context, conf, hc)
    H2OFramesServlet.register(context, conf, hc)
    DataFramesServlet.register(context, conf, hc)
    ShutdownServlet.register(context, conf, hc)
    context
  }

  private def proxyContextHandler(conf: H2OConf): ServletHandler = {
    val handler = new ServletHandler()
    val holder = new ServletHolder(new H2OFlowProxyServlet(conf))
    handler.addServlet(holder)
    val m = new ServletMapping()
    m.setServletName(holder.getName)
    m.setPathSpec("/*")
    handler.addServletMapping(m)
    val ipPort = conf.h2oCluster.get
    holder.setInitParameter("proxyTo", s"${conf.getScheme()}://$ipPort${conf.contextPath.getOrElse("")}")
    handler
  }

  class H2OFlowProxyServlet(val conf: H2OConf) extends Transparent {
    override def newHttpClient(): HttpClient = {
      val client = if (conf.jks.isDefined) {
        val sslFactory = createSSLContextFactory(conf)
        new HttpClient(sslFactory)
      } else {
        new HttpClient()
      }
      client.setScheduler(new ScheduledExecutorScheduler(null, true))
      client
    }
  }

  private def createSSLContextFactory(conf: H2OConf) = {
    if (conf.jksPass.isEmpty) {
      throw new RuntimeException("JKS is specified but JKS password is missing!")
    }
    val sslFactory = new SslContextFactory(conf.jks.get)
    sslFactory.setKeyStorePassword(conf.jksPass.get)
    if (conf.jksAlias.isDefined) {
      sslFactory.setCertAlias(conf.jksAlias.get)
    }
    sslFactory
  }

  def startServer(port: Int): Server = {
    val jettyServer = createJettyServer("0.0.0.0", port)
    val context = createServletContextHandler()
    if (h2oHttpView.getConfig.loginType != LoginType.NONE) {
      context.setSecurityHandler(authWrapper(jettyServer).asInstanceOf[SecurityHandler])
    }
    jettyServer.setHandler(context)
    jettyServer.start()
    jettyServer
  }
}
