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
package org.apache.spark.ml.h2o.algos

import hex.schemas.GBMV3.GBMParametersV3
import hex.tree.gbm.GBM
import hex.tree.gbm.GBMModel.GBMParameters
import org.apache.spark.annotation.Since
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.h2o.models._
import org.apache.spark.ml.h2o.param.H2OSharedTreeParams
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader}
import org.apache.spark.sql.SQLContext
import water.support.ModelSerializationSupport

/**
  * H2O GBM algorithm exposed via Spark ML pipelines.
  */
class H2OGBM(parameters: Option[GBMParameters], override val uid: String)
            (implicit h2oContext: H2OContext, sqlContext: SQLContext)
  extends H2OAlgorithm[GBMParameters, H2OMOJOModel](parameters)
    with H2OGBMParams {

  def this()(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(None, Identifiable.randomUID("gbm"))

  def this(uid: String, hc: H2OContext, sqlContext: SQLContext) = this(None, uid)(hc, sqlContext)

  def this(parameters: GBMParameters)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters), Identifiable.randomUID("gbm"))

  def this(parameters: GBMParameters, uid: String)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters), uid)

  override def defaultFileName: String = H2OGBM.defaultFileName

  override def trainModel(params: GBMParameters): H2OMOJOModel = {
    val model = new GBM(params).trainModel().get()
    new H2OMOJOModel(ModelSerializationSupport.getMojoData(model))
  }

}

object H2OGBM extends MLReadable[H2OGBM] {

  private final val defaultFileName = "gbm_params"

  @Since("1.6.0")
  override def read: MLReader[H2OGBM] = new H2OAlgorithmReader[H2OGBM, GBMParameters](defaultFileName)

  @Since("1.6.0")
  override def load(path: String): H2OGBM = super.load(path)
}


/**
  * Parameters for Spark's API exposing underlying H2O model.
  */
trait H2OGBMParams extends H2OSharedTreeParams[GBMParameters] {

  type H2O_SCHEMA = GBMParametersV3

  protected def paramTag = reflect.classTag[GBMParameters]

  protected def schemaTag = reflect.classTag[H2O_SCHEMA]

  //
  // Param definitions
  //
  final val learnRate = doubleParam("learnRate")
  final val learnRateAnnealing = doubleParam("learnRateAnnealing")
  final val colSampleRate = doubleParam("colSampleRate")
  final val maxAbsLeafnodePred = doubleParam("maxAbsLeafnodePred")
  final val predNoiseBandwidth = doubleParam("predNoiseBandwidth")

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
  /** @group getParam */
  def getLearnRate() = $(learnRate)
  /** @group getParam */
  def getLearnRateAnnealing() = $(learnRateAnnealing)
  /** @group getParam */
  def getColSampleRate() = $(colSampleRate)
  /** @group getParam */
  def getMaxAbsLeafnodePred() = $(maxAbsLeafnodePred)
  /** @group getParam */
  def getPredNoiseBandwidth() = $(predNoiseBandwidth)

  //
  // Setters
  //
  /** @group setParam */
  def setLearnRate(value: Double): this.type = set(learnRate, value)
  /** @group setParam */
  def setLearnRateAnnealing(value: Double): this.type = set(learnRateAnnealing, value)
  /** @group setParam */
  def setColSampleRate(value: Double): this.type = set(colSampleRate, value)
  /** @group setParam */
  def setMaxAbsLeafnodePred(value: Double): this.type = set(maxAbsLeafnodePred, value)
  /** @group setParam */
  def setPredNoiseBandwidth(value: Double): this.type = set(predNoiseBandwidth, value)


  override def updateH2OParams(): Unit = {
    super.updateH2OParams()
    parameters._learn_rate = $(learnRate)
    parameters._learn_rate_annealing = $(learnRateAnnealing)
    parameters._col_sample_rate = $(colSampleRate)
    parameters._max_abs_leafnode_pred = $(maxAbsLeafnodePred)
    parameters._pred_noise_bandwidth = $(predNoiseBandwidth)
    
  }
}
