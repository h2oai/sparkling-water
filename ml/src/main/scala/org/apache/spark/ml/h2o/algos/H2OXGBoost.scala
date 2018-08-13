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
import hex.tree.xgboost.XGBoostModel.XGBoostParameters._
import org.apache.spark.annotation.Since
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.h2o.models._
import org.apache.spark.ml.h2o.param.{EnumParam, H2OAlgoParams}
import org.apache.spark.ml.param.Params
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
  private final val quietMode = booleanParam("quietMode")
  private final val missingValuesHandling = new MissingValuesHandlingParam(this, "missingValuesHandling", "Missing Values Handling")
  private final val ntrees = intParam("ntrees")
  private final val nEstimators = intParam("nEstimators")
  private final val maxDepth = intParam("maxDepth")
  private final val minRows = doubleParam("minRows")
  private final val minChildWeight = doubleParam("minChildWeight")
  private final val learnRate = doubleParam("learnRate")
  private final val eta = doubleParam("eta")
  private final val learnRateAnnealing = doubleParam("learnRateAnnealing")
  private final val sampleRate = doubleParam("sampleRate")
  private final val subsample = doubleParam("subsample")
  private final val colSampleRate = doubleParam("colSampleRate")
  private final val colSampleByLevel = doubleParam("colSampleByLevel")
  private final val colSampleRatePerTree = doubleParam("colSampleRatePerTree")
  private final val colsampleBytree = doubleParam("colsampleBytree")
  private final val maxAbsLeafnodePred = floatParam("maxAbsLeafnodePred")
  private final val maxDeltaStep = floatParam("maxDeltaStep")
  private final val scoreTreeInterval = intParam("scoreTreeInterval")
  private final val initialScoreInterval = intParam("initialScoreInterval")
  private final val scoreInterval = intParam("scoreInterval")
  private final val minSplitImprovement = floatParam("minSplitImprovement")
  private final val gamma = floatParam("gamma")
  private final val nthread = intParam("nthread")
  private final val maxBins = intParam("maxBins")
  private final val maxLeaves = intParam("maxLeaves")
  private final val minSumHessianInLeaf = floatParam("minSumHessianInLeaf")
  private final val minDataInLeaf = floatParam("minDataInLeaf")
  private final val treeMethod = new TreeMethodParam(this, "treeMethod", "Tree Method")
  private final val growPolicy = new GrowPolicyParam(this, "growPolicy", "Grow Policy")
  private final val booster = new BoosterParam(this, "booster", "Booster")
  private final val dmatrixType = new DMatrixTypeParam(this, "dmatrixType", "DMatrix type")
  private final val regLambda = floatParam("regLambda")
  private final val regAlpha = floatParam("regAlpha")
  private final val sampleType = new DartSampleTypeParam(this, "sampleType", "Dart Sample Type")
  private final val normalizeType = new DartNormalizeTypeParam(this, "normalizeType", "Dart Normalize Type")
  private final val rateDrop = floatParam("rateDrop")
  private final val oneDrop = booleanParam("oneDrop")
  private final val skipDrop = floatParam("skipDrob")
  private final val gpuId = intParam("gpuId")
  private final val backend = new BackendParam(this, "backend", "Backend")
  //
  // Default values
  //
  setDefault(
    quietMode -> true,
    missingValuesHandling -> null,
    ntrees -> 50, // Number of trees in the final model. Grid Search, comma sep values:50,100,150,200
    nEstimators -> 0,  // This doesn't seem to be used anywhere... (not in clients)
    maxDepth -> 6, // Maximum tree depth. Grid Search, comma sep values:5,7
    minRows -> 1,
    minChildWeight -> 1,
    learnRate -> 0.3,
    eta -> 0.3,
    learnRateAnnealing -> 1,
    sampleRate -> 1.0,
    subsample -> 1.0,
    colSampleRate -> 1.0,
    colSampleByLevel -> 1.0,
    colSampleRatePerTree -> 1.0, //fraction of columns to sample for each tree
    colsampleBytree -> 1.0,
    maxAbsLeafnodePred -> 0,
    maxDeltaStep -> 0,
    scoreTreeInterval -> 0, // score every so many trees (no matter what)
    initialScoreInterval -> 4000, //Adding this parameter to take away the hard coded value of 4000 for scoring the first  4 secs
    scoreInterval -> 4000, //Adding this parameter to take away the hard coded value of 4000 for scoring each iteration every 4 secs
    minSplitImprovement -> 0,
    gamma -> 0.0f,
    nthread -> -1,
    maxBins -> 256,
    maxLeaves -> 0,
    minSumHessianInLeaf -> 100,
    minDataInLeaf -> 0,
    treeMethod -> TreeMethod.auto,
    growPolicy -> GrowPolicy.depthwise,
    booster -> Booster.gbtree,
    dmatrixType -> DMatrixType.auto,
    regLambda -> 0,
    regAlpha -> 0,
    sampleType -> DartSampleType.uniform,
    normalizeType -> DartNormalizeType.tree,
    rateDrop -> 0,
    oneDrop -> false,
    skipDrop -> 0,
    gpuId -> 0, // which GPU to use
    backend -> Backend.auto
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

class TreeMethodParam private[h2o](parent: Params, name: String, doc: String,
                                                isValid: XGBoostParameters.TreeMethod => Boolean)
  extends EnumParam[XGBoostParameters.TreeMethod](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}

class GrowPolicyParam private[h2o](parent: Params, name: String, doc: String,
                                   isValid: XGBoostParameters.GrowPolicy => Boolean)
  extends EnumParam[XGBoostParameters.GrowPolicy](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}

class BoosterParam private[h2o](parent: Params, name: String, doc: String,
                                   isValid: XGBoostParameters.Booster => Boolean)
  extends EnumParam[XGBoostParameters.Booster](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}

class MissingValuesHandlingParam private[h2o](parent: Params, name: String, doc: String,
                                isValid: XGBoostParameters.MissingValuesHandling => Boolean)
  extends EnumParam[XGBoostParameters.MissingValuesHandling](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}

class DartSampleTypeParam private[h2o](parent: Params, name: String, doc: String,
                                              isValid: XGBoostParameters.DartSampleType => Boolean)
  extends EnumParam[XGBoostParameters.DartSampleType](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}

class DartNormalizeTypeParam private[h2o](parent: Params, name: String, doc: String,
                                       isValid: XGBoostParameters.DartNormalizeType => Boolean)
  extends EnumParam[XGBoostParameters.DartNormalizeType](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}

class DMatrixTypeParam private[h2o](parent: Params, name: String, doc: String,
                                          isValid: XGBoostParameters.DMatrixType => Boolean)
  extends EnumParam[XGBoostParameters.DMatrixType](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}

class BackendParam private[h2o](parent: Params, name: String, doc: String,
                                    isValid: XGBoostParameters.Backend => Boolean)
  extends EnumParam[XGBoostParameters.Backend](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}