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

package ai.h2o.sparkling.ml.internals

import java.io.File

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.backend.utils.{RestApiUtils, RestCommunication}
import ai.h2o.sparkling.ml.models.{H2OMOJOModel, H2OMOJOSettings}
import org.apache.spark.expose.Utils
import ai.h2o.sparkling.utils.ScalaUtils._
import com.google.gson.JsonObject
import org.apache.commons.io.IOUtils
import water.api.schemas3.ModelsV3

private[sparkling] class H2OModel private (val modelId: String) extends RestCommunication {
  private val conf = H2OContext.ensure("H2OContext needs to be running!").getConf
  private val endpoint = RestApiUtils.getClusterEndpoint(conf)

  private[sparkling] def downloadMojo(): File = {
    val sparkTmpDir = Utils.createTempDir(Utils.getLocalDir(conf.sparkConf))
    val target = new File(sparkTmpDir, this.modelId)
    downloadBinaryURLContent(endpoint, s"/3/Models/${this.modelId}/mojo", conf, target)
    target
  }

  private[sparkling] def getDetails(): JsonObject = {
    val jsonObject = withResource(readURLContent(endpoint, "GET", s"/3/Models/${this.modelId}", conf)) { response =>
      val content = IOUtils.toString(response)
      deserializeAsJsonObject(content, Seq.empty)
    }
    jsonObject
      .getAsJsonArray("models")
      .get(0)
      .getAsJsonObject()
  }

  private[sparkling] def delete(): Unit = delete(endpoint, s"/3/Models/${this.modelId}", conf)

  private[sparkling] def tryDelete(): Unit =
    try {
      getCrossValidationModels().foreach(_.foreach(_.tryDelete()))
      delete()
    } catch {
      case e: Throwable => logWarning(s"Unsuccessful try to delete model '${this.modelId}'", e)
    }

  private def getCrossValidationModels(): Option[Array[H2OModel]] = {
    val cvModelsJson = getDetails()
      .getAsJsonObject("output")
      .get("cross_validation_models")

    if (cvModelsJson.isJsonNull) {
      None
    } else {
      val cvModelsArray = cvModelsJson.getAsJsonArray()
      val result = new Array[H2OModel](cvModelsArray.size())
      for (i <- 0 until cvModelsArray.size()) {
        val cvModelnName = cvModelsArray
          .get(i)
          .getAsJsonObject
          .getAsJsonPrimitive("name")
          .getAsString
        result(i) = H2OModel(cvModelnName)
      }
      Some(result)
    }
  }

  private def getCrossValidationMOJOModels(parentUid: String, settings: H2OMOJOSettings): Array[H2OMOJOModel] = {
    getCrossValidationModels() match {
      case None => null
      case Some(models) =>
        models.zipWithIndex.map {
          case (model, i) => model.toMOJOModel(s"${parentUid}_cv_$i", settings, false)
        }
    }
  }

  private[sparkling] def toMOJOModel(uid: String, settings: H2OMOJOSettings, withCVModels: Boolean): H2OMOJOModel = {
    val mojo = downloadMojo()
    val result = H2OMOJOModel.createFromMojo(mojo, uid, settings)
    if (withCVModels) {
      val cvModels = getCrossValidationMOJOModels(uid, settings)
      result.setCrossValidationModels(cvModels)
    }
    result
  }
}

private[sparkling] object H2OModel extends RestCommunication {

  private[sparkling] def listAllModels(): Array[String] = {
    val conf = H2OContext.ensure().getConf
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val models = query[ModelsV3](endpoint, "/3/Models", conf)
    models.models.map(_.model_id.name)
  }

  private[sparkling] def modelExists(modelId: String): Boolean = listAllModels().contains(modelId)

  def apply(modelId: String): H2OModel = new H2OModel(modelId)
}
