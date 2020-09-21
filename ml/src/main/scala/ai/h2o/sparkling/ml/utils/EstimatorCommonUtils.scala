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

import java.io.File

import ai.h2o.sparkling.backend.H2OJob
import ai.h2o.sparkling.backend.utils.{RestApiUtils, RestCommunication}
import ai.h2o.sparkling.ml.internals.H2OModel
import ai.h2o.sparkling.{H2OConf, H2OContext}
import hex.schemas.ModelBuilderSchema
import org.apache.spark.expose

trait EstimatorCommonUtils extends RestCommunication {
  protected def trainAndGetDestinationKey(
      endpointSuffix: String,
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
      encodeParamsAsJson)
    val jobId = modelBuilder.job.key.name
    H2OJob(jobId).waitForFinish()
    modelBuilder.job.dest.name
  }

  private[sparkling] def downloadBinaryModel(modelId: String, conf: H2OConf): File = {
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val sparkTmpDir = expose.Utils.createTempDir(expose.Utils.getLocalDir(conf.sparkConf))
    val target = new File(sparkTmpDir, s"$modelId.bin")
    downloadBinaryURLContent(endpoint, s"/3/Models.fetch.bin/$modelId", conf, target)
    target
  }

  protected def convertModelIdToKey(key: String): String = {
    if (H2OModel.modelExists(key)) {
      val replacement = findAlternativeKey(key)
      logWarning(
        s"Model id '$key' is already used by a different H2O model. Replacing the original id with '$replacement' ...")
      replacement
    } else {
      key
    }
  }

  private def findAlternativeKey(modelId: String): String = {
    var suffixNumber = 0
    var replacement: String = null
    do {
      suffixNumber = suffixNumber + 1
      replacement = s"${modelId}_$suffixNumber"
    } while (H2OModel.modelExists(replacement))
    replacement
  }
}
