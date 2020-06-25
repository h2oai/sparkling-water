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

package ai.h2o.sparkling.backend.api

import ai.h2o.sparkling.{H2OConf, H2OContext}
import javax.servlet.Servlet
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder, ServletMapping}

private[api] trait ServletRegister {
  protected def getServlet(conf: H2OConf, hc: H2OContext): Servlet

  protected def getRequestPaths(): Array[String]

  def register(context: ServletContextHandler, conf: H2OConf, hc: H2OContext): Unit = {
    val holder = new ServletHolder(getServlet(conf, hc))
    context.getServletHandler.addServlet(holder)
    val m = new ServletMapping()
    m.setPathSpecs(getRequestPaths())
    m.setServletName(holder.getName)
    context.getServletHandler.addServletMapping(m)
  }
}
