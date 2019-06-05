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
package org.apache.spark.ml.h2o.algos

import hex.Model
import hex.genmodel.utils.DistributionFamily
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o._
import org.apache.spark.ml.h2o.models.{H2OMOJOModel, H2OMOJOSettings}
import org.apache.spark.ml.h2o.param.H2OAlgoParams
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.Estimator
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Dataset
import water.{H2O, Key}
import water.support.{H2OFrameSupport, ModelSerializationSupport}

import scala.reflect.ClassTag

/**
  * Base class for H2O algorithm wrapper as a Spark transformer.
  */
abstract class H2OAlgorithm[P <: Model.Parameters : ClassTag] extends Estimator[H2OMOJOModel]
  with H2OAlgorithmCommons with DefaultParamsWritable with H2OAlgoParams[P] {

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

    if ((parameters._distribution == DistributionFamily.bernoulli
      || parameters._distribution == DistributionFamily.multinomial)
      && !trainFrame.vec(getLabelCol()).isCategorical) {
      trainFrame.replace(trainFrame.find(getLabelCol()),
        trainFrame.vec(getLabelCol()).toCategoricalVec).remove()
    }
    water.DKV.put(trainFrame)
    
    // Train

    val binaryModel: H2OBaseModel = trainModel(parameters)
    val mojoData = ModelSerializationSupport.getMojoData(binaryModel)
    val modelSettings = H2OMOJOSettings(
      convertUnknownCategoricalLevelsToNa = getConvertUnknownCategoricalLevelsToNa(),
      convertInvalidNumbersToNa = getConvertInvalidNumbersToNa())
    H2OMOJOModel.createFromMojo(
      mojoData,
      Identifiable.randomUID(binaryModel._parms.algoName()),
      modelSettings)
  }

  def trainModel(params: P): H2OBaseModel

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    require(schema.fields.exists(f => f.name.compareToIgnoreCase(getLabelCol()) == 0),
      s"Specified label column '${getLabelCol()} was not found in input dataset!")
    require(!getFeaturesCols().exists(n => n.compareToIgnoreCase(getLabelCol()) == 0),
      s"Specified input features cannot contain the label column!")
    schema
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  protected def createKey[T](modelId: String): Key[T] = {
    val key = Key.make[T](modelId)
    if(H2O.containsKey(key)) {
      var i = 0
      var replacement = key
      do {
        i = i + 1
        replacement = Key.make[T](s"${modelId}_$i")
      } while (H2O.containsKey(replacement))
      logWarning(s"Model id '$modelId' is already used by a different H2O model. Replacing the original id with '$replacement' ...")
      replacement
    } else {
      key
    }
  }
}
