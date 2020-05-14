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

import java.net.URI

import ai.h2o.sparkling.backend.utils.RestApiUtils
import org.apache.spark.h2o.H2OContext

trait RDDsRestApi extends RestApiUtils {

  protected def listRDDs(): RDDs = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    query[RDDs](endpoint, "/3/RDDs", hc.getConf)
  }

  protected def getRDD(rddId: Int): RDDInfo = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    update[RDDInfo](endpoint, s"/3/RDDs/$rddId", hc.getConf)
  }

  protected def convertToH2OFrame(rddId: Int, h2oFrameId: String): RDDToH2OFrame = {
    val hc = H2OContext.ensure()
    val endpoint = new URI(hc.flowURL())
    val params = Map("h2oframe_id" -> h2oFrameId)
    update[RDDToH2OFrame](endpoint, s"/3/RDDs/$rddId/h2oframe", hc.getConf, params)
  }
}
