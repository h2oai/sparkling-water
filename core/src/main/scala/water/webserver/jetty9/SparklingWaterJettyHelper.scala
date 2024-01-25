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
import ai.h2o.sparkling.backend.api.options.OptionsServlet
import ai.h2o.sparkling.backend.api.rdds.RDDsServlet
import ai.h2o.sparkling.backend.api.scalainterpreter.ScalaInterpreterServlet
import ai.h2o.sparkling.{H2OConf, H2OContext, H2OCredentials}
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.proxy.ProxyServlet.Transparent
import org.eclipse.jetty.security.SecurityHandler
import org.eclipse.jetty.client.api.{Request => ClientRequest}
import org.eclipse.jetty.server.{Handler, Request, Server}
import org.eclipse.jetty.server.handler.{HandlerCollection, HandlerWrapper}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHandler, ServletHolder, ServletMapping}
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler
import water.webserver.iface.{H2OHttpView, LoginType}

import java.net.InetSocketAddress
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

class SparklingWaterJettyHelper(
    hc: H2OContext,
    conf: H2OConf,
    credentials: Option[H2OCredentials],
    h2oHttpView: H2OHttpView)
  extends Jetty9Helper(h2oHttpView) {

  override def createServletContextHandler(): ServletContextHandler = {
    val context = new ServletContextHandler(ServletContextHandler.NO_SECURITY | ServletContextHandler.NO_SESSIONS)
    context.setContextPath(conf.contextPath.getOrElse("/"))
    context.setServletHandler(proxyContextHandler(conf, context))
    if (conf.isH2OReplEnabled) {
      ScalaInterpreterServlet.register(context, conf, hc)
    }
    RDDsServlet.register(context, conf, hc)
    H2OFramesServlet.register(context, conf, hc)
    DataFramesServlet.register(context, conf, hc)
    ShutdownServlet.register(context, conf, hc)
    OptionsServlet.register(context, conf, hc)
    context
  }

  private def proxyContextHandler(conf: H2OConf, context: ServletContextHandler): ServletHandler = {
    val handler = new ServletHandler()
    val holder = new ServletHolder(new H2OFlowProxyServlet(conf, credentials))
    handler.addServlet(holder)
    val m = new ServletMapping()
    m.setServletName(holder.getName)
    m.setPathSpec("/*")
    handler.addServletMapping(m)
    val ipPort = conf.h2oCluster.get
    holder.setInitParameter("proxyTo", s"${conf.getScheme()}://$ipPort${conf.contextPath.getOrElse("")}")
    holder.setInitParameter("idleTimeout", conf.restApiTimeout.toString)
    holder.setInitParameter("timeout", conf.restApiTimeout.toString)
    holder.setInitParameter("requestBufferSize", conf.flowProxyRequestMaxSize().toString)
    holder.setInitParameter("responseBufferSize", conf.flowProxyResponseMaxSize().toString)
    handler
  }

  class H2OFlowProxyServlet(val conf: H2OConf, val credentials: Option[H2OCredentials]) extends Transparent {
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

    override protected def addProxyHeaders(clientRequest: HttpServletRequest, proxyRequest: ClientRequest) = {
      credentials.foreach { c =>
        proxyRequest.getHeaders().remove("Authorization");
        proxyRequest.header("Authorization", c.toBasicAuth)
      }
    }
  }

  private def createSSLContextFactory(conf: H2OConf) = {
    if (conf.jksPass.isEmpty) {
      throw new RuntimeException("JKS is specified but JKS password is missing!")
    }
    val sslFactory = new SslContextFactory.Server()
    sslFactory.setKeyStorePassword(conf.jksPass.get)
    sslFactory.setKeyStorePath(conf.jks.get)
    if (conf.jksAlias.isDefined) {
      sslFactory.setCertAlias(conf.jksAlias.get)
    }
    sslFactory
  }

  def startServer(port: Int): Server = {
    val address = new InetSocketAddress(port)
    val jettyServer = createJettyServer(address.getAddress.getHostAddress, address.getPort)
    val context = createServletContextHandler()
    if (h2oHttpView.getConfig.loginType != LoginType.NONE) {
      val securityHandler = authWrapper(jettyServer).asInstanceOf[SecurityHandler]
      val authHandlers = new HandlerCollection
      authHandlers.setHandlers(Array[Handler](authenticationHandler(), context))
      val loginHandler = new ProxyLoginHandler()
      loginHandler.setHandler(authHandlers)
      securityHandler.setHandler(loginHandler)
    } else {
      jettyServer.setHandler(context)
    }
    jettyServer.start()
    jettyServer
  }

  private class ProxyLoginHandler extends HandlerWrapper {

    override def handle(
        target: String,
        baseRequest: Request,
        request: HttpServletRequest,
        response: HttpServletResponse): Unit = {
      val handled = h2oHttpView.proxyLoginHandler(target, request, response)
      if (handled) {
        baseRequest.setHandled(true)
      } else {
        super.handle(target, baseRequest, request, response)
      }
    }
  }
}
