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

/** Schema representing /3/dataframes/[dataframe_id] endpoint */
case class DataFrameInfo(dataframe_id: String, partitions: Int, schema: String)

object DataFrameInfo extends ParameterBase with DataFrameCommons {

  private[dataframes] case class DataFrameInfoParameters(dataFrameId: String) {
    def validate(): Unit = validateDataFrameId(dataFrameId)
  }

  private[dataframes] object DataFrameInfoParameters {
    def parse(request: HttpServletRequest): DataFrameInfoParameters = {
      val dataFrameId = request.getPathInfo.drop(1).split("/").head
      DataFrameInfoParameters(dataFrameId)
    }
  }
}
