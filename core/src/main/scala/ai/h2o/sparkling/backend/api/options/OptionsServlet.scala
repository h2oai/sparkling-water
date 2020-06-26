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
package ai.h2o.sparkling.backend.api.options

import ai.h2o.sparkling.{H2OConf, H2OContext}
import ai.h2o.sparkling.backend.api.h2oframes.{H2OFrameToDataFrame, H2OFramesServlet}
import ai.h2o.sparkling.backend.api.{ServletBase, ServletRegister}
import javax.servlet.Servlet
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import water.exceptions.H2ONotFoundArgumentException

/**
  * Handler for all H2OFrame related queries
  */
private[api] class OptionsServlet(conf: H2OConf) extends ServletBase {
  override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val obj = req.getPathInfo match {
      case null =>
        val parameters = Option.OptionParameters.parse(req)
        parameters.validate(conf)
        getOptionValue(parameters.name)
      case invalid => throw new H2ONotFoundArgumentException(s"Invalid endpoint $invalid")
    }
    sendResult(obj, resp)
  }

  private def getOptionValue(name: String): Option = {
    Option(name, conf.get(name))
  }
}

object OptionsServlet extends ServletRegister {
  override protected def getRequestPaths(): Array[String] = Array("/3/option")

  override protected def getServlet(conf: H2OConf, hc: H2OContext): Servlet = new OptionsServlet(conf: H2OConf)
}
