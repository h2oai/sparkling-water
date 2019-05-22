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

import org.apache.spark.internal.Logging
import org.apache.spark.ml.param.{DoubleParam, Param, Params, StringArrayParam}

/**
  * This trait contains parameters that are shared across all algorithms.
  */
trait H2OCommonParams extends Params with Logging {

  protected final val featuresCols = new StringArrayParam(this, "featuresCols", "Name of feature columns")
  private val labelCol = new Param[String](this, "labelCol", "Label column name")
  private val foldCol = new NullableStringParam(this, "foldCol", "Fold column name")
  private val weightCol = new NullableStringParam(this, "weightCol", "Weight column name")
  private val splitRatio = new DoubleParam(this, "splitRatio",
    "Accepts value from 0 to 1.0 and determine how large part of dataset will be used for training and for validation")

  //
  // Default values
  //
  setDefault(
    featuresCols -> Array.empty[String],
    labelCol -> "label",
    foldCol -> null,
    weightCol -> null,
    splitRatio -> 1.0 // Use whole frame as training frame
  )

  //
  // Getters
  //
  def getFeaturesCols(): Array[String] = {
    val excludedCols = getExcludedCols()
    $(featuresCols).filter(c => excludedCols.forall(e => c.compareToIgnoreCase(e) != 0))
  }

  def getLabelCol(): String = $(labelCol)

  def getFoldCol(): String = $(foldCol)

  def getWeightCol(): String = $(weightCol)

  def getSplitRatio(): Double = $(splitRatio)
  //
  // Setters
  //
  def setFeaturesCol(first: String): this.type = setFeaturesCols(first)

  def setFeaturesCols(first: String, others: String*): this.type = set(featuresCols, Array(first) ++ others)

  def setFeaturesCols(columnNames: Array[String]): this.type = {
    require(columnNames.length > 0, "Array with feature columns must contain at least one column.")
    set(featuresCols, columnNames)
  }

  def setLabelCol(columnName: String): this.type = set(labelCol, columnName)

  def setFoldCol(columnName: String): this.type = set(foldCol, columnName)

  def setWeightCol(columnName: String): this.type = set(weightCol, columnName)

  def setSplitRatio(ratio: Double): this.type = set(splitRatio, ratio)
  //
  // Other methods
  //
  protected def getExcludedCols(): Seq[String] = {
    Seq(getLabelCol(), getFoldCol(), getWeightCol())
      .flatMap(Option(_)) // Remove nulls
  }
}
