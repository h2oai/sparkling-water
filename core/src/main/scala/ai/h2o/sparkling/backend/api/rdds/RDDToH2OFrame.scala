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
package ai.h2o.sparkling.backend.api.rdds

import ai.h2o.sparkling.backend.api.ParameterBase
import javax.servlet.http.HttpServletRequest

/** Schema representing /3/RDDs/[rdd_id]/h2oframe endpoint */
case class RDDToH2OFrame(rdd_id: Int, h2oframe_id: String)

object RDDToH2OFrame extends ParameterBase with RDDCommons {
  private[rdds] case class RDDToH2OFrameParameters(rddId: Int, h2oFrameId: Option[String]) {
    def validate(): Unit = validateRDDId(rddId)
  }

  object RDDToH2OFrameParameters {
    private[rdds] def parse(request: HttpServletRequest): RDDToH2OFrameParameters = {
      val rddId = request.getPathInfo.drop(1).split("/").head.toInt
      val h2oFrameId = getParameterAsString(request, "h2oframe_id")
      RDDToH2OFrameParameters(rddId, Option(h2oFrameId).map(_.toLowerCase()))
    }
  }
}
