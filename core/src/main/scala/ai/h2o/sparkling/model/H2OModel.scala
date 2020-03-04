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

package ai.h2o.sparkling.model

import java.io.File
import java.nio.file.Files

import ai.h2o.sparkling.backend.external.RestApiUtils.getClusterEndpoint
import ai.h2o.sparkling.backend.external.{RestApiUtils, RestCommunication}
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import com.google.gson.{Gson, JsonElement}
import org.apache.commons.io.IOUtils
import org.apache.spark.expose.Utils
import org.apache.spark.h2o.{H2OBaseModel, H2OConf, H2OContext}
import water.support.ModelSerializationSupport

class H2OModel private(val modelId: String,
                       val modelCategory: H2OModelCategory.Value,
                       val metrics: H2OMetricsHolder,
                       val trainingParams: Map[String, String],
                       private[sparkling] var mojoData: Option[Array[Byte]] = None)
  extends RestCommunication {
  private val conf = H2OContext.ensure("H2OContext needs to be running!").getConf

  private[sparkling] def getOrDownloadMojoData(): Array[Byte] = synchronized {
    if (mojoData.isEmpty) {
      val endpoint = RestApiUtils.getClusterEndpoint(conf)
      val sparkTmpDir = Utils.createTempDir(Utils.getLocalDir(conf.sparkConf))
      val target = new File(sparkTmpDir, this.modelId)
      downloadBinaryURLContent(endpoint, s"/3/Models/${this.modelId}/mojo", conf, target)
      mojoData = Some(Files.readAllBytes(target.toPath))
    }
    mojoData.get
  }

  def getCurrentMetrics(nfolds: Int, splitRatio: Double): Map[H2OMetric, Double] = {
    if (nfolds > 1) {
      metrics.crossValidationMetrics
    } else if (splitRatio < 1) {
      metrics.validationMetrics
    } else {
      metrics.trainingMetrics
    }
  }
}

object H2OModel extends RestCommunication with H2OModelExtractionUtils {

  def apply(modelId: String): H2OModel = {
    val conf = H2OContext.ensure().getConf
    getModel(conf, modelId)
  }

  private def getModel(conf: H2OConf, modelId: String): H2OModel = {
    val endpoint = getClusterEndpoint(conf)

    val content = withResource(readURLContent(endpoint, "GET", s"/3/Models/$modelId", conf)) { response =>
      IOUtils.toString(response)
    }
    val gson = new Gson()
    val modelJson = gson.fromJson(content, classOf[JsonElement]).getAsJsonObject.get("models").getAsJsonArray.get(0).getAsJsonObject
    val modelCategory = extractModelCategory(modelJson)
    val metrics = extractAllMetrics(modelJson)
    val params = extractParams(modelJson)
    new H2OModel(modelId, modelCategory, metrics, params)
  }

  def fromBinary(model: H2OBaseModel, selectedParams: Array[String]): H2OModel = {
    val modelCategory = extractModelCategory(model)
    val metrics = extractAllMetrics(model)
    val params = extractParams(model, selectedParams: Array[String])
    new H2OModel(model._key.toString, modelCategory, metrics, params, Some(ModelSerializationSupport.getMojoData(model)))
  }
}
