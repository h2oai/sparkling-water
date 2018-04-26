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
import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JNull, JString, JValue}

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

  final val predictionCol = stringParam(
    "predictionCol",
    "Prediction column name")

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
    predictionCol -> "prediction",
    featuresCols -> Array.empty[String],
    nfolds -> parameters._nfolds,
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
  /** @group getParam */
  def getTrainRatio() = $(ratio)

  /** @group getParam */
  def getPredictionsCol() = $(predictionCol)

  /** @group getParam */
  def getFeaturesCols() = {
    if ($(featuresCols).contains($(predictionCol))) {
      logDebug("Prediction col '" + $(predictionCol) + "' removed from the list of features.")
      $(featuresCols).filter(_ != $(predictionCol))
    } else {
      $(featuresCols)
    }
  }

  /** @group getParam */
  def getAllStringColumnsToCategorical() = $(allStringColumnsToCategorical)

  /** @group getParam */
  def getColumnsToCategorical() = $(columnsToCategorical)

  /** @group getParam */
  def getNfolds() = $(nfolds)

  /** @group getParam */
  def getKeepCrossValidationPredictions() = $(keepCrossValidationPredictions)

  /** @group getParam */
  def getKeepCrossValidationFoldAssignment() = $(keepCrossValidationFoldAssignment)

  /** @group getParam */
  def getParallelizeCrossValidation() = $(parallelizeCrossValidation)

  /** @group getParam */
  def getSeed() = $(seed)

  /** @group getParam */
  def getDistribution() = $(distribution)

  /** @group getParam */
  def getConvertUnknownCategoricalLevelsToNa() = $(convertUnknownCategoricalLevelsToNa)

  //
  // Setters
  //
  /** @group setParam */
  def setTrainRatio(value: Double): this.type = set(ratio, value)

  /** @group setParam */
  def setPredictionsCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setFeaturesCols(first: String, others: String*): this.type = set(featuresCols, Array(first) ++ others)

  /** @group setParam */
  def setFeaturesCols(cols: Array[String]): this.type = {
    if (cols.length == 0) {
      throw new IllegalArgumentException("Array with feature columns must contain at least one column.")
    }
    set(featuresCols, cols)
  }

  /** @group setParam */
  def setFeaturesCol(first: String): this.type = setFeaturesCols(first)

  /** @group setParam */
  def setAllStringColumnsToCategorical(transform: Boolean): this.type = set(allStringColumnsToCategorical, transform)

  /** @group setParam */
  def setColumnsToCategorical(first: String, others: String*): this.type = set(columnsToCategorical, Array(first) ++ others)

  /** @group setParam */
  def setColumnsToCategorical(columns: Array[String]): this.type = set(columnsToCategorical, columns)


  /** @group setParam */
  def setNfolds(value: Int): this.type = set(nfolds, value)

  /** @group setParam */
  def setKeepCrossValidationPredictions(value: Boolean): this.type = set(keepCrossValidationPredictions, value)

  /** @group setParam */
  def setKeepCrossValidationFoldAssignment(value: Boolean): this.type = set(keepCrossValidationFoldAssignment, value)

  /** @group setParam */
  def setParallelizeCrossValidation(value: Boolean): this.type = set(parallelizeCrossValidation, value)

  /** @group setParam */
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  def setDistribution(value: DistributionFamily): this.type = set(distribution, value)

  /** @group setParam */
  def setConvertUnknownCategoricalLevelsToNa(value: Boolean): this.type = set(convertUnknownCategoricalLevelsToNa, value)

  def H2ODistributionParam(name: String): H2ODistributionParam = {
    new H2ODistributionParam(this, name, getDoc(None, name))
  }

  /** Update H2O params based on provided parameters to Spark Transformer/Estimator */
  protected def updateH2OParams(): Unit = {
    parameters._response_column = $(predictionCol)
    parameters._nfolds = $(nfolds)
    parameters._keep_cross_validation_predictions = $(keepCrossValidationPredictions)
    parameters._keep_cross_validation_fold_assignment = $(keepCrossValidationFoldAssignment)
    parameters._parallelize_cross_validation = $(parallelizeCrossValidation)
    parameters._seed = $(seed)
    parameters._distribution = $(distribution)
  }
}

class H2ODistributionParam private(parent: Params, name: String, doc: String, isValid: DistributionFamily => Boolean)
  extends Param[DistributionFamily](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: DistributionFamily): ParamPair[DistributionFamily] = super.w(value)

  override def jsonEncode(value: DistributionFamily): String = {
    val encoded: JValue = if (value == null) {
      JNull
    } else {
      JString(value.toString)
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): DistributionFamily = {
    val parsed = parse(json)
    parsed match {
      case JString(x) =>
        DistributionFamily.valueOf(x)
      case JNull =>
        null
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $parsed to DistributionFamily.")
    }

  }
}
