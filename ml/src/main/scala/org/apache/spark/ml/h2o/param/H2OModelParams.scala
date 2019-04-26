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
trait H2OModelParams extends DeprecatableParams {

  override protected def renamingMap: Map[String, String] = Map(
    "predictionCol" -> "labelCol"
  )

  //
  // Param definitions
  //
  private final val predictionCol: Param[String] = new Param[String](this, "predictionCol", "Prediction column name")
  private final val convertUnknownCategoricalLevelsToNa = new BooleanParam(this,
         "convertUnknownCategoricalLevelsToNa",
         "Convert unknown categorical levels to NA during predictions")
  //
  // Default values
  //
  setDefault(predictionCol -> "prediction")
  setDefault(convertUnknownCategoricalLevelsToNa -> false)

  //
  // Getters
  //
  def getPredictionCol(): String = $(predictionCol)

  def getConvertUnknownCategoricalLevelsToNa(): Boolean = $(convertUnknownCategoricalLevelsToNa)


  //
  // Setters
  //
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setConvertUnknownCategoricalLevelsToNa(value: Boolean): this.type = set(convertUnknownCategoricalLevelsToNa, value)
}
