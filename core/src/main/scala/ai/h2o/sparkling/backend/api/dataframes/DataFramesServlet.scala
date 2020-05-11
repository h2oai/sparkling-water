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
package ai.h2o.sparkling.backend.api.dataframes

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.backend.api.{GETRequestBase, POSTRequestBase, ServletRegister}
import ai.h2o.sparkling.utils.SparkSessionUtils
import javax.servlet.Servlet
import javax.servlet.http.HttpServletRequest
import org.apache.spark.h2o.H2OContext
import water.exceptions.H2ONotFoundArgumentException

/**
  * Handler for all Spark's DataFrame related queries
  */
private[api] class DataFramesServlet extends GETRequestBase with POSTRequestBase {
  private lazy val sqlContext = SparkSessionUtils.active.sqlContext

  override def handlePostRequest(request: HttpServletRequest): Any = {
    request.getRequestURI match {
      case s if s.matches(toScalaRegex("/3/dataframes/*/h2oframe")) =>
        val parameters = DataFrameToH2OFrame.DataFrameToH2OFrameParameters.parse(request)
        parameters.validate()
        toH2OFrame(parameters.dataFrameId, parameters.h2oFrameId)
      case s if s.matches(toScalaRegex("/3/dataframes/*")) =>
        val parameters = DataFrameInfo.DataFrameInfoParameters.parse(request)
        parameters.validate()
        getDataFrame(parameters.dataFrameId)
      case invalid => throw new H2ONotFoundArgumentException(s"Invalid endpoint $invalid")
    }
  }

  override def handleGetRequest(request: HttpServletRequest): Any = {
    request.getRequestURI match {
      case "/3/dataframes" =>
        list()
      case invalid => throw new H2ONotFoundArgumentException(s"Invalid endpoint $invalid")
    }
  }

  def list(): DataFrames = {
    DataFrames(fetchAll())
  }

  def fetchAll(): Array[DataFrameInfo] = {
    val names = sqlContext.tableNames()
    names.map(name => getDataFrame(name))
  }

  def getDataFrame(dataFrameId: String): DataFrameInfo = {
    val dataFrame = sqlContext.table(dataFrameId)
    DataFrameInfo(dataFrameId, dataFrame.rdd.partitions.length, dataFrame.schema.json)
  }

  def toH2OFrame(dataFrameId: String, h2oFrameId: Option[String]): DataFrameToH2OFrame = {
    val dataFrame = sqlContext.table(dataFrameId)
    val h2oFrame = H2OFrame(H2OContext.ensure().asH2OFrameKeyString(dataFrame, h2oFrameId))
    DataFrameToH2OFrame(dataFrameId, h2oFrame.frameId)
  }
}

object DataFramesServlet extends ServletRegister {

  override protected def getEndpoints(): Array[String] = {
    Array("/3/dataframes", "/3/dataframes/*")
  }

  override protected def getServlet(): Servlet = new DataFramesServlet
}
