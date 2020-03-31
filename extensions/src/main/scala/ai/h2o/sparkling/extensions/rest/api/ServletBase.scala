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

package ai.h2o.sparkling.extensions.rest.api

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import water.server.ServletUtils

abstract class ServletBase extends HttpServlet {
  protected def getParameterAsString(request: HttpServletRequest, parameterName: String): String = {
    val result = request.getParameter(parameterName)
    if (result == null) {
      throw new IllegalArgumentException(s"Cannot find value for the parameter '$parameterName'")
    }
    result
  }

  protected def processRequest[R](request: HttpServletRequest, response: HttpServletResponse)(
      processor: => Unit): Unit = {
    val uri = ServletUtils.getDecodedUri(request)
    try {
      processor
      ServletUtils.setResponseStatus(response, HttpServletResponse.SC_OK)
    } catch {
      case e: Exception => ServletUtils.sendErrorResponse(response, e, uri)
    } finally {
      ServletUtils.logRequest(request.getMethod, request, response)
    }
  }
}
