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
import ai.h2o.sparkling.backend.api.ParameterBase
import javax.servlet.http.HttpServletRequest
import water.exceptions.H2ONotFoundArgumentException

/** Schema representing /3/h2oframes/[h2oframe_id]/dataframe */
case class H2OFrameToDataFrame(h2oframe_id: String, dataframe_id: String)

object H2OFrameToDataFrame extends ParameterBase {
  private[h2oframes] case class H2OFrameToDataFrameParameters(h2oFrameId: String, dataframeId: Option[String]) {
    def validate(): Unit = {
      if (!H2OFrame.exists(h2oFrameId)) {
        throw new H2ONotFoundArgumentException(
          s"H2OFrame with id '$h2oFrameId' does not exist, can not proceed with the transformation!")
      }
    }
  }

  private[h2oframes] object H2OFrameToDataFrameParameters {
    def parse(request: HttpServletRequest): H2OFrameToDataFrameParameters = {
      val h2oFrameId = request.getRequestURI.split("/")(3)
      val dataFrameId = getParameterAsString(request, "dataframe_id")
      H2OFrameToDataFrameParameters(h2oFrameId, Option(dataFrameId).map(_.toLowerCase()))
    }
  }
}
