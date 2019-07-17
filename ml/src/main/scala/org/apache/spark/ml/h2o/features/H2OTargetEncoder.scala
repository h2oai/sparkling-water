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

package org.apache.spark.ml.h2o.features

import ai.h2o.automl.targetencoding._
import org.apache.spark.h2o.{H2OContext, Frame}
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.h2o.models.H2OTargetEncoderModel
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{Dataset, SparkSession}

class H2OTargetEncoder(override val uid: String)
  extends Estimator[H2OTargetEncoderModel]
  with H2OTargetEncoderBase
  with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("H2OTargetEncoder"))

  override def fit(dataset: Dataset[_]): H2OTargetEncoderModel = {
    val h2oContext = H2OContext.getOrCreate(SparkSession.builder().getOrCreate())
    val input = h2oContext.asH2OFrame(dataset.toDF())
    convertRelevantColumnsToCategorical(input)
    val targetEncoderModel = trainTargetEncodingModel(input)
    val model = new H2OTargetEncoderModel(uid, targetEncoderModel).setParent(this)
    copyValues(model)
  }

  private def trainTargetEncodingModel(trainingFrame: Frame) = {
    val targetEncoderParameters = new TargetEncoderModel.TargetEncoderParameters()
    targetEncoderParameters._withBlending = getBlendedAvgEnabled()
    targetEncoderParameters._blendingParams = new BlendingParams(getBlendedAvgInflectionPoint(), getBlendedAvgSmoothing())
    targetEncoderParameters._response_column = getLabelCol()
    targetEncoderParameters._teFoldColumnName = getFoldCol()
    targetEncoderParameters._columnNamesToEncode = getInputCols()
    targetEncoderParameters.setTrain(trainingFrame._key)

    val builder = new TargetEncoderBuilder(targetEncoderParameters)
    builder.trainModel().get() // Calling get() to wait until the model training is finished.
    builder.getTargetEncoderModel()
  }

  override def copy(extra: ParamMap): H2OTargetEncoder = defaultCopy(extra)


  //
  // Parameter Setters
  //
  def setFoldCol(value: String): this.type = set(foldCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  def setHoldoutStrategy(value: H2OTargetEncoderHoldoutStrategy): this.type = set(holdoutStrategy, value)

  def setBlendedAvgEnabled(value: Boolean): this.type = set(blendedAvgEnabled, value)

  def setBlendedAvgInflectionPoint(value: Double): this.type = set(blendedAvgInflectionPoint, value)

  def setBlendedAvgSmoothing(value: Double): this.type = {
    require(value > 0.0, "The smoothing value has to be a positive number.")
    set(blendedAvgSmoothing, value)
  }

  def setNoise(value: Double): this.type = {
    require(value >= 0.0, "Noise can't be a negative value.")
    set(noise, value)
  }

  def setNoiseSeed(value: Long): this.type = set(noiseSeed, value)
}

object H2OTargetEncoder extends DefaultParamsReadable[H2OTargetEncoder]
