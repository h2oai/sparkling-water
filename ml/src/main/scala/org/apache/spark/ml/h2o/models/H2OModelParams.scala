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
package org.apache.spark.ml.h2o.models

import org.apache.spark.ml.param.{Param, Params, StringArrayParam}


trait H2OModelParams extends Params{

  /**
    * By default it is set to 1.0 which use whole frame for training
    */
  final val predictionCol: Param[String] = new Param[String](this, "predictionCol", "Prediction column name")
  final val featuresCols: StringArrayParam = new StringArrayParam(this, "featuresCols", "Name of feature columns")
  setDefault(predictionCol -> "prediction")
  setDefault(featuresCols -> Array.empty[String])

  /** @group getParam */
  final def getFeaturesCols: Array[String] = $(featuresCols)

  /** @group setParam */
  def setFeaturesCols(cols: Array[String]) = set(featuresCols, cols)

  /** @group getParam */
  def getPredictionsCol: String = $(predictionCol)

  /** @group setParam */
  def setPredictionsCol(value: String) = set(predictionCol, value)
}
