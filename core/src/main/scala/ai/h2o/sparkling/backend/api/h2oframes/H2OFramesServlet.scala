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

import ai.h2o.sparkling.{H2OConf, H2OContext}
import ai.h2o.sparkling.backend.api.{ServletBase, ServletRegister}
import ai.h2o.sparkling.utils.SparkSessionUtils
import javax.servlet.Servlet
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import water.exceptions.H2ONotFoundArgumentException

/**
  * Handler for all H2OFrame related queries
  */
private[api] class H2OFramesServlet extends ServletBase {

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val obj = req.getPathInfo match {
      case s if s.matches("/.*/dataframe") =>
        val parameters = H2OFrameToDataFrame.H2OFrameToDataFrameParameters.parse(req)
        parameters.validate()
        toDataFrame(parameters.h2oFrameId, parameters.dataframeId)
      case invalid => throw new H2ONotFoundArgumentException(s"Invalid endpoint $invalid")
    }
    sendResult(obj, resp)
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
  override protected def getEndpoints(): Array[String] = Array("/3/h2oframes/*")

  override protected def getServlet(conf: H2OConf): Servlet = new H2OFramesServlet
}
