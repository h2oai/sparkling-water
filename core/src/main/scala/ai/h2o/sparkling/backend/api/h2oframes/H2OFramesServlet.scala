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

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.backend.api.{POSTRequestBase, ServletRegister}
import ai.h2o.sparkling.utils.SparkSessionUtils
import javax.servlet.Servlet
import javax.servlet.http.HttpServletRequest
import org.apache.spark.h2o.H2OContext
import water.exceptions.H2ONotFoundArgumentException

/**
  * Handler for all H2OFrame related queries
  */
class H2OFramesServlet extends POSTRequestBase {
  private case class DataFrameToH2OFrameParameters(h2oFrameId: String, dataframeId: Option[String]) {
    def validate(): Unit = {
      if (!H2OFrame.exists(h2oFrameId)) {
        throw new H2ONotFoundArgumentException(
          s"H2OFrame with id '$h2oFrameId' does not exist, can not proceed with the transformation!")
      }
    }
  }

  private object DataFrameToH2OFrameParameters {
    def parse(request: HttpServletRequest): DataFrameToH2OFrameParameters = {
      val h2oFrameId = getParameterAsString(request, "h2oframe_id")
      val dataFrameId = getParameterAsString(request, "dataframe_id")
      DataFrameToH2OFrameParameters(h2oFrameId, Option(dataFrameId).map(_.toLowerCase()))
    }
  }

  override def handlePostRequest(request: HttpServletRequest): Any = {
    request.getServletPath match {
      case H2OFramesServlet.dataFrameToH2OFramePath =>
        val parameters = DataFrameToH2OFrameParameters.parse(request)
        parameters.validate()
        toDataFrame(parameters.h2oFrameId, parameters.dataframeId)
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
  private val dataFrameToH2OFramePath = "/3/h2oframes/*/dataframe"
  override protected def getServletClass(): Class[_ <: Servlet] = classOf[H2OFramesServlet]

  override protected def getEndpoints(): Array[String] = Array(dataFrameToH2OFramePath)
}
