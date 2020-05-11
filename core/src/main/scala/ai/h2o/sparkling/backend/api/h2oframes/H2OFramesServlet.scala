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
package ai.h2o.sparkling.backend.api.h2oframes

import ai.h2o.sparkling.backend.api.{POSTRequestBase, ServletRegister}
import ai.h2o.sparkling.utils.SparkSessionUtils
import javax.servlet.Servlet
import javax.servlet.http.HttpServletRequest
import org.apache.spark.h2o.H2OContext
import water.exceptions.H2ONotFoundArgumentException

/**
  * Handler for all H2OFrame related queries
  */
private[api] class H2OFramesServlet extends POSTRequestBase {
  override def handlePostRequest(request: HttpServletRequest): Any = {
    request.getRequestURI match {
      case s if s.matches(toScalaRegex("/3/h2oframes/*/dataframe")) =>
        val parameters = H2OFrameToDataFrame.H2OFrameToDataFrameParameters.parse(request)
        parameters.validate()
        toDataFrame(parameters.h2oFrameId, parameters.dataframeId)
      case invalid => throw new H2ONotFoundArgumentException(s"Invalid endpoint $invalid")
    }
  }

  private def toDataFrame(h2oFrameId: String, dataFrameId: Option[String]): H2OFrameToDataFrame = {
    val dataFrame = H2OContext.ensure().asSparkFrame(h2oFrameId)
    dataFrame.rdd.cache()
    val resp = new H2OFrameToDataFrame(h2oFrameId, dataFrameId.getOrElse("df_" + dataFrame.rdd.id.toString))
    dataFrame.createOrReplaceTempView(resp.dataframe_id)
    SparkSessionUtils.active.sqlContext.cacheTable(resp.dataframe_id)
    resp
  }
}

object H2OFramesServlet extends ServletRegister {
  override protected def getEndpoints(): Array[String] = Array("/3/h2oframes/*/dataframe")

  override protected def getServlet(): Servlet = new H2OFramesServlet
}
