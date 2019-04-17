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

import hex.Model.Parameters
import hex.genmodel.utils.DistributionFamily
import org.apache.spark.internal.Logging
import org.apache.spark.ml.param.Params
import water.util.DeprecatedMethod

/**
  * A trait extracting a shared parameters among all models.
  *
  * TODO: There are still bunch of parameters defined Model.ModelParameters which need to be ported here
  */
trait H2OAlgoParams[P <: Parameters] extends H2OAlgoParamsHelper[P] with Logging {

  //
  // Param definitions
  //
  final val ratio = doubleParam(
    "ratio",
    "Determines in which ratios split the dataset")

  final val labelCol = stringParam(
    "labelCol",
    "Label column name")

  final val weightCol = nullableStringParam(
    "weightCol",
    "Weight column name")

  final val featuresCols = stringArrayParam(
    "featuresCols",
    "Name of feature columns")

  final val allStringColumnsToCategorical = booleanParam(
    "allStringColumnsToCategorical",
    "Transform all strings columns to categorical")

  final val columnsToCategorical = stringArrayParam(
    "columnsToCategorical",
    "List of columns to convert to categorical before modelling")

  final val nfolds = intParam("nfolds")
  final val foldCol = nullableStringParam("foldCol", "Fold column name")
  final val keepCrossValidationPredictions = booleanParam("keepCrossValidationPredictions")
  final val keepCrossValidationFoldAssignment = booleanParam("keepCrossValidationFoldAssignment")
  final val parallelizeCrossValidation = booleanParam("parallelizeCrossValidation")
  final val seed = longParam("seed")
  final val distribution = H2ODistributionParam("distribution")
  final val convertUnknownCategoricalLevelsToNa = booleanParam(
    "convertUnknownCategoricalLevelsToNa",
    "Convert unknown categorical levels to NA during predictions")
  //
  // Default values
  //
  setDefault(
    ratio -> 1.0, // 1.0 means use whole frame as training frame
    labelCol -> "label",
    weightCol -> null,
    featuresCols -> Array.empty[String],
    nfolds -> parameters._nfolds,
    foldCol -> null,
    allStringColumnsToCategorical -> true,
    columnsToCategorical -> Array.empty[String],
    keepCrossValidationPredictions -> parameters._keep_cross_validation_predictions,
    keepCrossValidationFoldAssignment -> parameters._keep_cross_validation_fold_assignment,
    parallelizeCrossValidation -> parameters._parallelize_cross_validation,
    seed -> parameters._seed,
    distribution -> parameters._distribution,
    convertUnknownCategoricalLevelsToNa -> false
  )

  //
  // Getters
  //
  def getTrainRatio(): Double = $(ratio)

  @DeprecatedMethod("getLabelCol")
  def getPredictionCol(): String = getLabelCol

  def getLabelCol(): String = $(labelCol)

  def getWeightCol(): String = $(weightCol)

  def getFeaturesCols(): Array[String] = {
    val lblCol = getLabelCol()
    if ($(featuresCols).contains(lblCol)) {
      logDebug(s"The label col '$lblCol' removed from the list of features.")
      $(featuresCols).filter(_ != lblCol)
    } else {
      $(featuresCols)
    }
  }

  def getAllStringColumnsToCategorical(): Boolean = $(allStringColumnsToCategorical)

  def getColumnsToCategorical(): Array[String] = $(columnsToCategorical)

  def getNfolds(): Int = $(nfolds)

  def getKeepCrossValidationPredictions(): Boolean = $(keepCrossValidationPredictions)

  def getKeepCrossValidationFoldAssignment(): Boolean = $(keepCrossValidationFoldAssignment)

  def getParallelizeCrossValidation(): Boolean = $(parallelizeCrossValidation)

  def getSeed(): Long = $(seed)

  def getDistribution(): DistributionFamily = $(distribution)

  def getConvertUnknownCategoricalLevelsToNa(): Boolean = $(convertUnknownCategoricalLevelsToNa)

  def getFoldCol(): String = $(foldCol)

  //
  // Setters
  //
  def setTrainRatio(value: Double): this.type = set(ratio, value)

  @DeprecatedMethod("setLabelCol")
  def setPredictionCol(value: String): this.type = setLabelCol(value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

  def setFeaturesCols(first: String, others: String*): this.type = set(featuresCols, Array(first) ++ others)

  def setFeaturesCols(cols: Array[String]): this.type = {
    if (cols.length == 0) {
      throw new IllegalArgumentException("Array with feature columns must contain at least one column.")
    }
    set(featuresCols, cols)
  }

  def setFeaturesCol(first: String): this.type = setFeaturesCols(first)

  def setAllStringColumnsToCategorical(transform: Boolean): this.type = set(allStringColumnsToCategorical, transform)

  def setColumnsToCategorical(first: String, others: String*): this.type = set(columnsToCategorical, Array(first) ++ others)

  def setColumnsToCategorical(columns: Array[String]): this.type = set(columnsToCategorical, columns)


  def setNfolds(value: Int): this.type = set(nfolds, value)

  def setKeepCrossValidationPredictions(value: Boolean): this.type = set(keepCrossValidationPredictions, value)

  def setKeepCrossValidationFoldAssignment(value: Boolean): this.type = set(keepCrossValidationFoldAssignment, value)

  def setParallelizeCrossValidation(value: Boolean): this.type = set(parallelizeCrossValidation, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setDistribution(value: DistributionFamily): this.type = set(distribution, value)

  def setConvertUnknownCategoricalLevelsToNa(value: Boolean): this.type = set(convertUnknownCategoricalLevelsToNa, value)

  def H2ODistributionParam(name: String): H2ODistributionParam = {
    new H2ODistributionParam(this, name, getDoc(None, name))
  }

  def setFoldCol(value: String): this.type = set(foldCol, value)

  /** Update H2O params based on provided parameters to Spark Transformer/Estimator */
  protected def updateH2OParams(): Unit = {
    parameters._response_column = $(labelCol)
    parameters._weights_column = $(weightCol)
    parameters._nfolds = $(nfolds)
    parameters._fold_column = $(foldCol)
    parameters._keep_cross_validation_predictions = $(keepCrossValidationPredictions)
    parameters._keep_cross_validation_fold_assignment = $(keepCrossValidationFoldAssignment)
    parameters._parallelize_cross_validation = $(parallelizeCrossValidation)
    parameters._seed = $(seed)
    parameters._distribution = $(distribution)
  }
}

class H2ODistributionParam(parent: Params, name: String, doc: String, isValid: DistributionFamily => Boolean)
  extends EnumParam[DistributionFamily](parent, name, doc) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}
