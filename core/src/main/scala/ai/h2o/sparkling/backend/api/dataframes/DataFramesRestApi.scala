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

import java.net.URI

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.backend.utils.RestApiUtils

trait DataFramesRestApi extends RestApiUtils {

  protected def listDataFrames(): DataFrames = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    query[DataFrames](endpoint, "/3/dataframes", hc.getConf)
  }

  protected def getDataFrame(dataFrameId: String): DataFrameInfo = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    update[DataFrameInfo](endpoint, s"/3/dataframes/$dataFrameId", hc.getConf)
  }

  protected def convertToH2OFrame(dataFrameId: String, h2oFrameId: String): DataFrameToH2OFrame = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    val params = if (h2oFrameId == null) Map[String, String]() else Map("h2oframe_id" -> h2oFrameId)
    update[DataFrameToH2OFrame](endpoint, s"/3/dataframes/$dataFrameId/h2oframe", hc.getConf, params)
  }
}
