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

package ai.h2o.sparkling.ml.features

import ai.h2o.sparkling.ml.models.{H2OTargetEncoderBase, H2OTargetEncoderModel}
import ai.h2o.sparkling.ml.params.H2OAlgoParamsHelper
import ai.h2o.targetencoding._
import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.h2o.{Frame, H2OContext}
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Dataset

class H2OTargetEncoder(override val uid: String)
  extends Estimator[H2OTargetEncoderModel]
  with H2OTargetEncoderBase
  with DefaultParamsWritable
  with H2OTargetEncoderModelUtils {

  def this() = this(Identifiable.randomUID("H2OTargetEncoder"))

  override def fit(dataset: Dataset[_]): H2OTargetEncoderModel = {
    val h2oContext = H2OContext.getOrCreate(SparkSessionUtils.active)
    val input = h2oContext.asH2OFrame(dataset.toDF())
    convertRelevantColumnsToCategorical(input)
    val columnsToKeep = getInputCols() ++ Seq(getFoldCol(), getLabelCol()).map(Option(_)).flatten
    val ignoredColumns = dataset.columns.diff(columnsToKeep)
    val targetEncoderModel = trainTargetEncodingModel(input, ignoredColumns)
    val model = new H2OTargetEncoderModel(uid, targetEncoderModel).setParent(this)
    copyValues(model)
  }

  private def trainTargetEncodingModel(trainingFrame: Frame, ignoredColumns: Array[String]) = try {
    val targetEncoderParameters = new TargetEncoderModel.TargetEncoderParameters()
    targetEncoderParameters._blending = getBlendedAvgEnabled()
    targetEncoderParameters._k = getBlendedAvgInflectionPoint()
    targetEncoderParameters._f = getBlendedAvgSmoothing()
    targetEncoderParameters._response_column = getLabelCol()
    targetEncoderParameters._fold_column = getFoldCol()
    targetEncoderParameters._ignored_columns = ignoredColumns
    targetEncoderParameters.setTrain(trainingFrame._key)

    val builder = new TargetEncoderBuilder(targetEncoderParameters)
    builder.trainModel().get() // Calling get() to wait until the model training is finished.
    builder.getTargetEncoderModel()
  } catch {
    case e: IllegalStateException if e.getMessage.contains("We do not support multi-class target case") =>
      throw new RuntimeException("The label column can not contain more than two unique values.")
  }

  override def copy(extra: ParamMap): H2OTargetEncoder = defaultCopy(extra)


  //
  // Parameter Setters
  //
  def setFoldCol(value: String): this.type = set(foldCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  def setOutputCols(values: Array[String]): this.type = set(outputCols, values)

  def setHoldoutStrategy(value: String): this.type = {
    set(holdoutStrategy, H2OAlgoParamsHelper.getValidatedEnumValue[TargetEncoder.DataLeakageHandlingStrategy](value))
  }

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
