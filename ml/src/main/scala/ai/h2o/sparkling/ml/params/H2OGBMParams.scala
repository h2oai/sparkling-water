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

package ai.h2o.sparkling.ml.params

import hex.schemas.GBMV3.GBMParametersV3
import hex.tree.gbm.GBMModel.GBMParameters

trait H2OGBMParams extends H2OAlgoSharedTreeParams[GBMParameters]
  with HasMonotoneConstraints
  with HasQuantileAlpha {

  type H2O_SCHEMA = GBMParametersV3

  protected def paramTag = reflect.classTag[GBMParameters]

  protected def schemaTag = reflect.classTag[H2O_SCHEMA]

  //
  // Param definitions
  //
  private val learnRate = doubleParam("learnRate")
  private val learnRateAnnealing = doubleParam("learnRateAnnealing")
  private val colSampleRate = doubleParam("colSampleRate")
  private val maxAbsLeafnodePred = doubleParam("maxAbsLeafnodePred")
  private val predNoiseBandwidth = doubleParam("predNoiseBandwidth")

  //
  // Default values
  //
  setDefault(
    learnRate -> parameters._learn_rate,
    learnRateAnnealing -> parameters._learn_rate_annealing,
    colSampleRate -> parameters._col_sample_rate,
    maxAbsLeafnodePred -> parameters._max_abs_leafnode_pred,
    predNoiseBandwidth -> parameters._pred_noise_bandwidth
  )

  //
  // Getters
  //
  def getLearnRate(): Double = $(learnRate)

  def getLearnRateAnnealing(): Double = $(learnRateAnnealing)

  def getColSampleRate(): Double = $(colSampleRate)

  def getMaxAbsLeafnodePred(): Double = $(maxAbsLeafnodePred)

  def getPredNoiseBandwidth(): Double = $(predNoiseBandwidth)

  //
  // Setters
  //
  def setLearnRate(value: Double): this.type = set(learnRate, value)

  def setLearnRateAnnealing(value: Double): this.type = set(learnRateAnnealing, value)

  def setColSampleRate(value: Double): this.type = set(colSampleRate, value)

  def setMaxAbsLeafnodePred(value: Double): this.type = set(maxAbsLeafnodePred, value)

  def setPredNoiseBandwidth(value: Double): this.type = set(predNoiseBandwidth, value)

  override private[sparkling] def getH2OAlgorithmParams(): Map[String, Any] = {
    super.getH2OAlgorithmParams() ++
      Map(
        "learn_rate" -> getLearnRate(),
        "learn_rate_annealing" -> getLearnRateAnnealing(),
        "col_sample_rate" -> getColSampleRate(),
        "max_abs_leafnode_pred" -> getMaxAbsLeafnodePred(),
        "pred_noise_bandwidth" -> getPredNoiseBandwidth(),
        "monotone_constraints" -> getMonotoneConstraints(),
        "quantile_alpha" -> getQuantileAlpha()
      )
  }
}
