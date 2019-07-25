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

import ai.h2o.sparkling.ml.params.{H2OAlgoCommonParams, H2OAlgoSupervisedParams}
import hex.Model
import hex.genmodel.utils.DistributionFamily
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.h2o.models.{H2OMOJOModel, H2OMOJOSettings}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType
import water.support.{H2OFrameSupport, ModelSerializationSupport}
import water.{H2O, Key}

import scala.reflect.{ClassTag, classTag}

/**
  * Base class for H2O algorithm wrapper as a Spark transformer.
  */
abstract class H2OAlgorithm[B <: H2OBaseModelBuilder : ClassTag, M <: H2OBaseModel, P <: Model.Parameters : ClassTag]
  extends Estimator[H2OMOJOModel] with H2OAlgoCommonUtils with DefaultParamsWritable with H2OAlgoCommonParams[P] {

  protected def preProcessBeforeFit(trainFrame: Frame): Unit = {}

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    // Update H2O params based on provided configuration
    updateH2OParams()

    val (train, valid) = prepareDatasetForFitting(dataset)
    parameters._train = train._key
    parameters._valid = valid.map(_._key).orNull

    val trainFrame = parameters._train.get()
    if (getAllStringColumnsToCategorical()) {
      H2OFrameSupport.allStringVecToCategorical(trainFrame)
    }
    H2OFrameSupport.columnsToCategorical(trainFrame, getColumnsToCategorical())

    preProcessBeforeFit(trainFrame)

    water.DKV.put(trainFrame)
    
    // Train
    val binaryModel: H2OBaseModel = trainModel(parameters)
    val mojoData = ModelSerializationSupport.getMojoData(binaryModel)
    val modelSettings = H2OMOJOSettings(
      predictionCol = getPredictionCol(),
      convertUnknownCategoricalLevelsToNa = getConvertUnknownCategoricalLevelsToNa(),
      convertInvalidNumbersToNa = getConvertInvalidNumbersToNa())
    H2OMOJOModel.createFromMojo(
      mojoData,
      Identifiable.randomUID(binaryModel._parms.algoName()),
      modelSettings)
  }

  private def trainModel(params: P): H2OBaseModel = {
    val modelId = getModelId()
    val algoClass = classTag[B].runtimeClass
    val parameterClass = classTag[P].runtimeClass
    val builder = if (modelId == null || modelId.isEmpty) {
      val constructor = algoClass.getConstructor(parameterClass)
      constructor.newInstance(params)
    } else {
      val constructor = algoClass.getConstructor(parameterClass, classOf[Key[M]])
      constructor.newInstance(params, convertModelIdToKey(modelId))
    }
    builder.asInstanceOf[B].trainModel().get()
  }

  private def convertModelIdToKey(modelId: String): Key[M] = {
    val key = Key.make[M](modelId)
    if (H2O.containsKey(key)) {
      val replacement = findAlternativeKey(modelId)
      logWarning(s"Model id '$modelId' is already used by a different H2O model. Replacing the original id with '$replacement' ...")
      replacement
    } else {
      key
    }
  }

  private def findAlternativeKey(modelId: String): Key[M] = {
    var suffixNumber = 0
    var replacement: Key[M] = null
    do {
      suffixNumber = suffixNumber + 1
      replacement = Key.make[M](s"${modelId}_$suffixNumber")
    } while (H2O.containsKey(replacement))
    replacement
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}
