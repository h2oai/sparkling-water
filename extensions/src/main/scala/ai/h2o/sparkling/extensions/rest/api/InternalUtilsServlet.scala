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

import ai.h2o.sparkling.extensions.rest.api.schema.{ScalaCodeResult, ScalaCodeResultV3}
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import com.google.gson.Gson
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import water.H2O.H2OCountedCompleter
import water.{Iced, Job, Key, Lockable}
import water.api.schemas3.{JobV3, KeyV3}
import water.server.ServletUtils

/**
  * This servlet class handles POST requests for the path /3/sw_internal
  */
class InternalUtilsServlet extends ServletBase {

  override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = {

    processRequest(request, response) {
      val obj = request.getRequestURI match {
        case s if s.startsWith("/3/sw_internal/start") =>
          val job =
            new Job[ScalaCodeResult](Key.make[ScalaCodeResult](), classOf[ScalaCodeResult].getName, "ScalaCodeResult")
          job.start(new H2OCountedCompleter() {
            override def compute2(): Unit = {
              while (true) {
                Thread.sleep(1000)
              }
              tryComplete()
            }
          }, 1)
          new JobV3(job)
        case s if s.startsWith("/3/sw_internal/stop/") =>
          null
      }
      val json = new Gson().toJson(obj)
      withResource(response.getWriter) { writer =>
        response.setContentType("application/json")
        response.setCharacterEncoding("UTF-8")
        writer.print(json)
      }
      response.setStatus(HttpServletResponse.SC_OK)
      ServletUtils.setResponseStatus(response, HttpServletResponse.SC_OK)
    }
  }
}
