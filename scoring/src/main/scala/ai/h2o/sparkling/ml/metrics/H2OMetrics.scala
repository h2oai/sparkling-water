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

package ai.h2o.sparkling.ml.metrics

import ai.h2o.sparkling.ml.internals.H2OModelCategory
import ai.h2o.sparkling.ml.models.H2OMOJOModelUtils
import ai.h2o.sparkling.ml.params.ParameterConstructorMethods
import com.google.gson.JsonObject
import org.apache.spark.expose.Logging

trait H2OMetrics extends ParameterConstructorMethods with H2OMOJOModelUtils with Logging {

  def setMetrics(json: JsonObject, context: String): Unit = {}
}

object H2OMetrics {
  def loadMetrics(
      json: JsonObject,
      metricsType: String,
      algoName: String,
      modelCategory: H2OModelCategory.Value): H2OMetrics = {

    val metricsObject = modelCategory match {
      case H2OModelCategory.Binomial if algoName == "glm" => new H2OBinomialGLMMetrics()
      case H2OModelCategory.Binomial => new H2OBinomialMetrics()
      case H2OModelCategory.Multinomial if algoName == "glm" => new H2OMultinomialGLMMetrics()
      case H2OModelCategory.Multinomial => new H2OMultinomialMetrics()
      case H2OModelCategory.Ordinal if algoName == "glm" => new H2OOrdinalGLMMetrics()
      case H2OModelCategory.Ordinal => new H2OOrdinalMetrics()
      case H2OModelCategory.Regression if algoName == "glm" => new H2ORegressionGLMMetrics()
      case H2OModelCategory.Regression => new H2ORegressionMetrics()
      case H2OModelCategory.Clustering => new H2OClusteringMetrics()
      case H2OModelCategory.AnomalyDetection => new H2OAnomalyMetrics()
      case H2OModelCategory.AutoEncoder => new H2OAutoEncoderMetrics()
      case H2OModelCategory.CoxPH => new H2ORegressionCoxPHMetrics()
      case _ if algoName == "glrm" => new H2OGLRMMetrics()
      case _ if algoName == "pca" => new H2OPCAMetrics()
      case _ => new H2OCommonMetrics()
    }
    metricsObject.setMetrics(json, s"${algoName}.mojo_details.output.${metricsType}")
    metricsObject
  }
}
