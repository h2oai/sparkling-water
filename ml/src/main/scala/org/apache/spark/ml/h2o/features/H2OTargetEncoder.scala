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
import org.apache.spark.h2o.{Frame, H2OContext}
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.h2o.models.H2OTargetEncoderModel
import org.apache.spark.ml.h2o.param.H2OTargetEncoderParams
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{Dataset, SparkSession}

class H2OTargetEncoder(override val uid: String)
  extends Estimator[H2OTargetEncoderModel]
  with H2OTargetEncoderParams
  with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("H2OTargetEncoder"))

  override def fit(dataset: Dataset[_]): H2OTargetEncoderModel = {
    val h2oContext = H2OContext.getOrCreate(SparkSession.builder().getOrCreate())
    val input = h2oContext.asH2OFrame(dataset.toDF())
    changeRelevantColumnsToCategorical(input)
    val targetEncoderModel = trainTargetEncodingModel(input)
    val model = new H2OTargetEncoderModel(uid, targetEncoderModel).setParent(this)
    copyValues(model)
  }

  private def trainTargetEncodingModel(trainingFrame: Frame) = {
    val targetEncoderParameters = new TargetEncoderModel.TargetEncoderParameters()
    val blending = Option(getBlending())
    targetEncoderParameters._withBlending = blending.isDefined
    targetEncoderParameters._blendingParams = blending.map(_.toBlendingParams()).getOrElse(null)
    targetEncoderParameters._response_column = getLabelCol()
    targetEncoderParameters._teFoldColumnName = getFoldCol()
    targetEncoderParameters._columnNamesToEncode = getInputCols()
    targetEncoderParameters.setTrain(trainingFrame._key)

    val builder = new TargetEncoderBuilder(targetEncoderParameters)
    builder.trainModel().get()
  }

  override def copy(extra: ParamMap): H2OTargetEncoder = defaultCopy(extra)


  //
  // Parameter Setters
  //
  def setFoldCol(value: String): this.type = set(foldCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  def setHoldoutStrategy(value: H2OTargetEncoderHoldoutStrategy): this.type = set(holdoutStrategy, value)

  def setBlending(settings: H2OTargetEncoderBlendingSettings): this.type = set(blending, settings)

  def setNoise(settings: H2OTargetEncoderNoiseSettings): this.type = set(noise, settings)
}

object H2OTargetEncoder extends DefaultParamsReadable[H2OTargetEncoder]

case class H2OTargetEncoderBlendingSettings(inflectionPoint: Double, smoothing: Double) {
  def toBlendingParams(): BlendingParams = new BlendingParams(inflectionPoint, smoothing)
}

case class H2OTargetEncoderNoiseSettings(amount: Double = 0.01, seed: Long = -1)
