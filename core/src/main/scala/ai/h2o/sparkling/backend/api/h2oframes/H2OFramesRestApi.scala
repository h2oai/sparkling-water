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

import java.net.URI

import ai.h2o.sparkling.backend.utils.{RestApiUtils, RestEncodingUtils}
import org.apache.spark.h2o.H2OContext

trait H2OFramesRestApi extends RestApiUtils with RestEncodingUtils {

  protected def convertToDataFrame(h2oFrameId: String, dataFrameId: String): H2OFrameToDataFrame = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    val params = if (dataFrameId == null) Map[String, String]() else Map("dataframe_id" -> dataFrameId)
    update[H2OFrameToDataFrame](endpoint, s"/3/h2oframes/$h2oFrameId/dataframe", hc.getConf, params)
  }
}
