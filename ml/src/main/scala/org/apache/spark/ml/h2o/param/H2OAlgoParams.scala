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
import org.apache.spark.ml.param.Params
import water.util.DeprecatedMethod

/**
  * A trait extracting a shared parameters among all models.
  *
  * TODO: There are still bunch of parameters defined Model.ModelParameters which need to be ported here
  */
trait H2OAlgoParams[P <: Parameters] extends H2OAlgoParamsHelper[P] with H2OCommonParams with DeprecatableParams {

  override protected def renamingMap: Map[String, String] = Map(
    "predictionCol" -> "labelCol",
    "ratio" -> "splitRatio"
  )

  //
  // Param definitions
  //
  private val keepCrossValidationPredictions = booleanParam("keepCrossValidationPredictions")
  private val keepCrossValidationFoldAssignment = booleanParam("keepCrossValidationFoldAssignment")
  private val parallelizeCrossValidation = booleanParam("parallelizeCrossValidation")
  private val distribution = H2ODistributionParam("distribution")

  //
  // Default values
  //
  setDefault(
    keepCrossValidationPredictions -> parameters._keep_cross_validation_predictions,
    keepCrossValidationFoldAssignment -> parameters._keep_cross_validation_fold_assignment,
    parallelizeCrossValidation -> parameters._parallelize_cross_validation,
    distribution -> parameters._distribution
  )

  //
  // Getters
  //
  @DeprecatedMethod("getSplitRatio")
  def getTrainRatio(): Double = getSplitRatio()

  @DeprecatedMethod("getLabelCol")
  def getPredictionCol(): String = getLabelCol()

  def getKeepCrossValidationPredictions(): Boolean = $(keepCrossValidationPredictions)

  def getKeepCrossValidationFoldAssignment(): Boolean = $(keepCrossValidationFoldAssignment)

  def getParallelizeCrossValidation(): Boolean = $(parallelizeCrossValidation)

  def getDistribution(): DistributionFamily = $(distribution)

  //
  // Setters
  //
  @DeprecatedMethod("setSplitRatio")
  def setTrainRatio(value: Double): this.type = setSplitRatio(value)

  @DeprecatedMethod("setLabelCol")
  def setPredictionCol(value: String): this.type = setLabelCol(value)

  def setKeepCrossValidationPredictions(value: Boolean): this.type = set(keepCrossValidationPredictions, value)

  def setKeepCrossValidationFoldAssignment(value: Boolean): this.type = set(keepCrossValidationFoldAssignment, value)

  def setParallelizeCrossValidation(value: Boolean): this.type = set(parallelizeCrossValidation, value)

  def setDistribution(value: DistributionFamily): this.type = set(distribution, value)

  def H2ODistributionParam(name: String): H2ODistributionParam = {
    new H2ODistributionParam(this, name, getDoc(None, name))
  }

  /** Update H2O params based on provided parameters to Spark Transformer/Estimator */
  protected def updateH2OParams(): Unit = {
    parameters._response_column = getLabelCol()
    parameters._weights_column = getWeightCol()
    parameters._nfolds = $(nfolds)
    parameters._fold_column = getFoldCol()
    parameters._keep_cross_validation_predictions = $(keepCrossValidationPredictions)
    parameters._keep_cross_validation_fold_assignment = $(keepCrossValidationFoldAssignment)
    parameters._parallelize_cross_validation = $(parallelizeCrossValidation)
    parameters._seed = getSeed()
    parameters._distribution = $(distribution)
  }
}

class H2ODistributionParam(parent: Params, name: String, doc: String, isValid: DistributionFamily => Boolean)
  extends EnumParam[DistributionFamily](parent, name, doc) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}
