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

import ai.h2o.sparkling.backend.utils.{RestApiUtils, RestCommunication}
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import com.google.gson._
import org.apache.commons.io.IOUtils
import org.apache.spark.expose.Utils
import org.apache.spark.h2o.{H2OConf, H2OContext}
import water.api.schemas3.ModelsV3

import scala.collection.JavaConverters._


class H2OModel private(val modelId: String,
                       val modelCategory: H2OModelCategory.Value,
                       val metrics: H2OMetricsHolder,
                       val trainingParams: Map[String, String])
  extends RestCommunication {
  private val conf = H2OContext.ensure("H2OContext needs to be running!").getConf

  private[sparkling] def downloadMojoData(): Array[Byte] = {
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val sparkTmpDir = Utils.createTempDir(Utils.getLocalDir(conf.sparkConf))
    val target = new File(sparkTmpDir, this.modelId)
    downloadBinaryURLContent(endpoint, s"/3/Models/${this.modelId}/mojo", conf, target)
    Files.readAllBytes(target.toPath)
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

object H2OModel extends RestCommunication {

  private[sparkling] def listAllModels(): Array[String] = {
    val conf = H2OContext.ensure().getConf
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val models = query[ModelsV3](endpoint, "/3/Models", conf)
    models.models.map(_.model_id.name)
  }

  private[sparkling] def modelExists(modelId: String): Boolean = listAllModels().contains(modelId)

  def apply(modelId: String): H2OModel = {
    val conf = H2OContext.ensure().getConf
    getModel(conf, modelId)
  }

  private def getModel(conf: H2OConf, modelId: String): H2OModel = {
    val endpoint = RestApiUtils.getClusterEndpoint(conf)

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

  protected def extractModelCategory(modelJson: JsonObject): H2OModelCategory.Value = {
    val json = modelJson.get("output").getAsJsonObject
    H2OModelCategory.fromString(json.get("model_category").getAsString)
  }

  protected def extractAllMetrics(modelJson: JsonObject): H2OMetricsHolder = {
    val json = modelJson.get("output").getAsJsonObject
    val trainingMetrics = extractMetrics(json, "training_metrics")
    val validationMetrics = extractMetrics(json, "validation_metrics")
    val crossValidationMetrics = extractMetrics(json, "cross_validation_metrics")
    H2OMetricsHolder(trainingMetrics, validationMetrics, crossValidationMetrics)
  }

  private def stringifyJSON(value: JsonElement): Option[String] = {
    value match {
      case v: JsonPrimitive => Some(v.getAsString)
      case v: JsonArray =>
        val stringElements = v.asScala.flatMap(stringifyJSON)
        val arrayAsString = stringElements.mkString("[", ", ", "]")
        Some(arrayAsString)
      case _ =>
        // don't put more complex type to output yet
        None
    }
  }

  protected def extractParams(modelJson: JsonObject): Map[String, String] = {
    val parameters = modelJson.get("parameters").getAsJsonArray.asScala.toArray
    parameters.flatMap { param =>
      val name = param.getAsJsonObject.get("name").getAsString
      val value = param.getAsJsonObject.get("actual_value")
      val stringValue = stringifyJSON(value)
      stringValue.map(name -> _)
    }.sorted.toMap
  }

  private def extractMetrics(json: JsonObject, metricType: String): Map[H2OMetric, Double] = {
    if (json.get(metricType).isJsonNull) {
      Map.empty
    } else {
      val metricGroup = json.getAsJsonObject(metricType)
      val fields = metricGroup.entrySet().asScala.map(_.getKey)
      val metrics = H2OMetric.values().flatMap { metric =>
        val metricName = metric.toString
        val fieldName = fields.find(field => field.replaceAll("_", "").equalsIgnoreCase(metricName))
        if (fieldName.isDefined) {
          Some(metric -> metricGroup.get(fieldName.get).getAsDouble)
        } else {
          None
        }
      }
      metrics.sorted(H2OMetricOrdering).toMap
    }
  }

  private object H2OMetricOrdering extends Ordering[(H2OMetric, Double)] {
    def compare(a: (H2OMetric, Double), b: (H2OMetric, Double)): Int = a._1.name().compare(b._1.name())
  }

}
