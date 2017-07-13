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
import org.apache.spark.ml.h2o.algos.params.H2OSharedTreeParams
import org.apache.spark.ml.h2o.models._
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader}
import org.apache.spark.sql.SQLContext
import water.support.ModelSerializationSupport

/**
  * H2O GBM Algo exposed via Spark ML pipelines.
  */
class H2OGBM(parameters: Option[GBMParameters], override val uid: String)
            (implicit h2oContext: H2OContext, sqlContext: SQLContext)
  extends H2OAlgorithm[GBMParameters, H2OGBMModel](parameters)
    with H2OGBMParams {

  type SELF = H2OGBM

  /** @group setParam */
  def setResponseColumn(value: String) = set(responseColumn, value) {
    getParams._response_column = value
  }

  /**
    * Param for features column name.
    *
    * @group param
    */
  final val featuresCol: Param[String] = new Param[String](this, "featuresCol", "features column name")

  setDefault(featuresCol, "features")

  def setFeaturesCol(value: String) = set(featuresCol, value) {}

  /** @group getParam */
  final def getFeaturesCol: String = $(featuresCol)

  /**
    * Param for features column name.
    *
    * @group param
    */
  final val predictionCol: Param[String] = new Param[String](this, "predictionCol", "prediction column name")

  setDefault(predictionCol, "prediction")

  def setPredictionsCol(value: String) = set(predictionCol, value) {}

  override def defaultFileName: String = H2OGBM.defaultFileName


  override def trainModel(params: GBMParameters): H2OGBMModel = {
    set(responseColumn, $(predictionCol)) {
      getParams._response_column = $(predictionCol)
    }
    val model = new GBM(params).trainModel().get()
    val mojoModel = ModelSerializationSupport.getMojoModel(model)
    val mojoData = ModelSerializationSupport.getMojoData(model)
    val m = new H2OGBMModel(mojoModel, mojoData)(sqlContext)

    // pass some parameters set on algo to model
    m.featuresCol = $(featuresCol)
    m.predictionCol = $(predictionCol)
    m
  }

  def this()(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(None, Identifiable.randomUID("dl"))

  def this(parameters: GBMParameters)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters), Identifiable.randomUID("dl"))

  def this(parameters: GBMParameters, uid: String)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters), uid)
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

  /**
    * All parameters should be set here along with their documentation and explained default values
    */
  final val learnRate = doubleParam("learnRate")
  final val learnRateAnnealing = doubleParam("learnRateAnnealing")
  final val colSampleRate = doubleParam("colSampleRate")
  final val maxAbsLeafnodePred = doubleParam("maxAbsLeafnodePred")
  final val responseColumn = param[String]("responseColumn")

  setDefault(learnRate -> parameters._learn_rate)
  setDefault(learnRateAnnealing -> parameters._learn_rate_annealing)
  setDefault(colSampleRate -> parameters._col_sample_rate)
  setDefault(maxAbsLeafnodePred -> parameters._max_abs_leafnode_pred)
  setDefault(responseColumn -> parameters._response_column)
}
