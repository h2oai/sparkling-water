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
package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.backend.exceptions.RestApiCommunicationException
import ai.h2o.sparkling.backend.utils.RestCommunication
import ai.h2o.sparkling.frame.H2OFrame
import ai.h2o.sparkling.ml.models.{H2OMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.ml.params.H2OAlgoCommonParams
import ai.h2o.sparkling.model.H2OModel
import hex.Model
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag

/**
 * Base class for H2O algorithm wrapper as a Spark transformer.
 */
abstract class H2OAlgorithm[P <: Model.Parameters : ClassTag]
  extends Estimator[H2OMOJOModel]
    with H2OAlgoCommonUtils
    with DefaultParamsWritable
    with H2OAlgoCommonParams[P]
    with RestCommunication {

  protected def prepareH2OTrainFrameForFitting(frame: H2OFrame): Unit = {}

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val (train, valid, internalFeatureCols) = prepareDatasetForFitting(dataset)
    prepareH2OTrainFrameForFitting(train)
    val params = getH2OAlgorithmParams() ++
      Map(
        "training_frame" -> train.frameId,
        "model_id" -> convertModelIdToKey()
      ) ++
      valid.map { fr => Map("validation_frame" -> fr.frameId) }.getOrElse(Map())
    val modelId = try {
      trainAndGetDestinationKey(s"/3/ModelBuilders/${parameters.algoName().toLowerCase}", params)
    } catch {
      case e: RestApiCommunicationException if e.getMessage.contains("There are no usable columns to generate model") =>
        throw new IllegalArgumentException(s"H2O could not use any of the specified feature" +
          s" columns: '${getFeaturesCols().mkString(", ")}'. H2O ignores constant columns, are all the columns constants?")
    }
    val mojoData = H2OModel(modelId).downloadMojoData()
    val modelSettings = H2OMOJOSettings.createFromModelParams(this)
    H2OMOJOModel.createFromMojo(
      mojoData,
      Identifiable.randomUID(parameters.algoName()),
      modelSettings,
      internalFeatureCols)
  }

  private def convertModelIdToKey(): String = {
    val key = getModelId()
    if (H2OModel.modelExists(key)) {
      val replacement = findAlternativeKey(key)
      logWarning(s"Model id '$modelId' is already used by a different H2O model. Replacing the original id with '$replacement' ...")
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

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}
