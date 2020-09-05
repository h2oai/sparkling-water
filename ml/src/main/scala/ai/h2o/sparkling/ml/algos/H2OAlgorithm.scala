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

import ai.h2o.sparkling.backend.utils.RestCommunication
import ai.h2o.sparkling.ml.internals.H2OModel
import ai.h2o.sparkling.ml.models.{H2OBinaryModel, H2OMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.ml.params.H2OCommonParams
import ai.h2o.sparkling.{H2OContext, H2OFrame}
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
abstract class H2OAlgorithm[P <: Model.Parameters: ClassTag]
  extends Estimator[H2OMOJOModel]
  with H2OCommonParams
  with H2OAlgoCommonUtils
  with DefaultParamsWritable
  with RestCommunication {

  def getFoldCol(): String

  def getWeightCol(): String

  def getModelId(): String

  def setFoldCol(value: String): this.type

  def setWeightCol(value: String): this.type

  def setModelId(value: String): this.type

  // Class tag for parameters to get runtime class
  protected def paramTag: ClassTag[P]

  protected var parameters: P = paramTag.runtimeClass.newInstance().asInstanceOf[P]

  protected def prepareH2OTrainFrameForFitting(frame: H2OFrame): Unit = {}

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val (train, valid) = prepareDatasetForFitting(dataset)
    prepareH2OTrainFrameForFitting(train)
    val params = getH2OAlgorithmParams(train) ++
      Map("training_frame" -> train.frameId, "model_id" -> convertModelIdToKey()) ++
      valid
        .map { fr =>
          Map("validation_frame" -> fr.frameId)
        }
        .getOrElse(Map())
    val modelId = trainAndGetDestinationKey(s"/3/ModelBuilders/${parameters.algoName().toLowerCase}", params)
    deleteRegisteredH2OFrames()
    val downloadedModel = downloadBinaryModel(modelId, H2OContext.ensure().getConf)
    binaryModel = Some(H2OBinaryModel.read("file://" + downloadedModel.getAbsolutePath, Some(modelId)))
    H2OModel(modelId)
      .toMOJOModel(Identifiable.randomUID(parameters.algoName()), H2OMOJOSettings.createFromModelParams(this))
  }

  private def convertModelIdToKey(): String = {
    val key = getModelId()
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

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    require(
      getWeightCol() == null || getWeightCol() != getFoldCol(),
      "Specified weight column cannot be the same as the fold column!")
    schema
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}
