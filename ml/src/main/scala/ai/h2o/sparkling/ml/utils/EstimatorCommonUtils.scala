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
package ai.h2o.sparkling.ml.utils

import ai.h2o.sparkling.backend.utils.{RestApiUtils, RestCommunication}
import ai.h2o.sparkling.job.H2OJob
import hex.schemas.ModelBuilderSchema
import org.apache.spark.h2o.H2OContext

trait EstimatorCommonUtils extends RestCommunication {
  protected def trainAndGetDestinationKey(endpointSuffix: String,
                                          params: Map[String, Any],
                                          encodeParamsAsJson: Boolean = false): String = {
    val conf = H2OContext.ensure().getConf
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val modelBuilder = update[ModelBuilderSchema[_, _, _]](
      endpoint,
      endpointSuffix,
      conf,
      params,
      Seq((classOf[ModelBuilderSchema[_, _, _]], "parameters")),
      encodeParamsAsJson
    )
    val jobId = modelBuilder.job.key.name
    H2OJob(jobId).waitForFinish()
    modelBuilder.job.dest.name
  }
}
