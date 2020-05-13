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

import ai.h2o.sparkling.utils.ScalaUtils.withResource
import com.google.gson.Gson
import javax.servlet.http.{HttpServlet, HttpServletResponse}

private[api] trait ServletBase extends HttpServlet {

  protected def sendResult(obj: Any, resp: HttpServletResponse): Unit = {
    val json = new Gson().toJson(obj)
    withResource(resp.getWriter) { writer =>
      resp.setContentType("application/json")
      resp.setCharacterEncoding("UTF-8")
      writer.print(json)
    }
    resp.setStatus(HttpServletResponse.SC_OK)
  }
}
