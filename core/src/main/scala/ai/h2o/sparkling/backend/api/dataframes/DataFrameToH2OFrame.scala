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

import ai.h2o.sparkling.backend.api.ParameterBase
import javax.servlet.http.HttpServletRequest
import org.apache.spark.sql.SparkSession
import water.exceptions.H2ONotFoundArgumentException

/** Schema representing /3/dataframe/[dataframe_id]/h2oframe endpoint */
case class DataFrameToH2OFrame(dataframe_id: String, h2oframe_id: String)

object DataFrameToH2OFrame extends ParameterBase {
  private[dataframes] case class DataFrameToH2OFrameParameters(dataFrameId: String, h2oFrameId: Option[String]) {
    def validate(): Unit = {
      if (!SparkSession.active.sqlContext.tableNames().toList.contains(dataFrameId)) {
        throw new H2ONotFoundArgumentException(s"DataFrame with id '$dataFrameId' does not exist!")
      }
    }
  }

  object DataFrameToH2OFrameParameters {
    private[dataframes] def parse(request: HttpServletRequest): DataFrameToH2OFrameParameters = {
      val dataFrameId = request.getRequestURI.split("/")(3)
      val h2oFrameId = getParameterAsString(request, "h2oframe_id")
      DataFrameToH2OFrameParameters(dataFrameId, Option(h2oFrameId).map(_.toLowerCase()))
    }
  }
}