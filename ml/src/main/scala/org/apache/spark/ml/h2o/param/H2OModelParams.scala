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
package org.apache.spark.ml.h2o.param

import org.apache.spark.ml.param._

/**
  * Parameters which need to be available on the model itself for prediction purposes. This can't be backed
  * byt H2OAlgoParamsHelper as at the time of prediction we might be using mojo and binary parameters are not available.
  */
trait H2OModelParams extends Params {

  //
  // Param definitions
  //
  final val predictionCol: Param[String] = new Param[String](this, "predictionCol", "Prediction column name")
  final val featuresCols: StringArrayParam = new StringArrayParam(this, "featuresCols", "Name of feature columns")
  final val outputCol: Param[String] = new Param[String](this, "outputCol", "Column where predictions are created")
  final val convertUnknownCategoricalLevelsToNa = new BooleanParam(this,
         "convertUnknownCategoricalLevelsToNa",
         "Convert unknown categorical levels to NA during predictions")
  //
  // Default values
  //
  setDefault(predictionCol -> "prediction")
  setDefault(featuresCols -> Array.empty[String])
  setDefault(outputCol -> "prediction_output")
  setDefault(convertUnknownCategoricalLevelsToNa -> false)

  //
  // Getters
  //
  /** @group getParam */
  def getPredictionsCol() = $(predictionCol)

  /** @group getParam */
  def getFeaturesCols() = $(featuresCols)

  /** @group getParam */
  def getOutputCol() = $(outputCol)

  /** @group getParam */
  def getConvertUnknownCategoricalLevelsToNa() = $(convertUnknownCategoricalLevelsToNa)


  //
  // Setters
  //
  /** @group setParam */
  def setFeaturesCols(cols: Array[String]): this.type = set(featuresCols, cols)

  /** @group setParam */
  def setPredictionsCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setConvertUnknownCategoricalLevelsToNa(value: Boolean): this.type = set(convertUnknownCategoricalLevelsToNa, value)
}
