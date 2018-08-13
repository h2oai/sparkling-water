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

import hex.schemas.XGBoostV3.XGBoostParametersV3
import hex.tree.xgboost.XGBoost
import hex.tree.xgboost.XGBoostModel.XGBoostParameters
import org.apache.spark.annotation.Since
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.h2o.models._
import org.apache.spark.ml.h2o.param.H2OAlgoParams
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader}
import org.apache.spark.sql.SQLContext
import water.support.ModelSerializationSupport

/**
  * H2O XGBoost algorithm exposed via Spark ML pipelines.
  */
class H2OXGBoost(parameters: Option[XGBoostParameters], override val uid: String)
            (implicit h2oContext: H2OContext, sqlContext: SQLContext)
  extends H2OAlgorithm[XGBoostParameters, H2OMOJOModel](parameters)
    with H2OXGBoostParams {

  def this()(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(None, Identifiable.randomUID("xgboost"))

  def this(uid: String, hc: H2OContext, sqlContext: SQLContext) = this(None, uid)(hc, sqlContext)

  def this(parameters: XGBoostParameters)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters), Identifiable.randomUID("gbm"))

  def this(parameters: XGBoostParameters, uid: String)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters), uid)

  override def defaultFileName: String = H2OXGBoost.defaultFileName

  override def trainModel(params: XGBoostParameters): H2OMOJOModel = {
    val model = new XGBoost(params).trainModel().get()
    new H2OMOJOModel(ModelSerializationSupport.getMojoData(model))
  }

}

object H2OXGBoost extends MLReadable[H2OXGBoost] {

  private final val defaultFileName = "xgboost_params"

  @Since("1.6.0")
  override def read: MLReader[H2OXGBoost] = H2OAlgorithmReader.create[H2OXGBoost, XGBoostParameters](defaultFileName)

  @Since("1.6.0")
  override def load(path: String): H2OXGBoost = super.load(path)
}


/**
  * Parameters for Spark's API exposing underlying H2O model.
  */
trait H2OXGBoostParams extends H2OAlgoParams[XGBoostParameters] {

  type H2O_SCHEMA = XGBoostParametersV3

  protected def paramTag = reflect.classTag[XGBoostParameters]

  protected def schemaTag = reflect.classTag[H2O_SCHEMA]

  //
  // Param definitions
  //


  //
  // Default values
  //
  setDefault(
  )

  //
  // Getters
  //


  //
  // Setters
  //

  override def updateH2OParams(): Unit = {
    super.updateH2OParams()

  }

}
