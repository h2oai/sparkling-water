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
import hex.tree.xgboost.{XGBoost, XGBoostModel}
import hex.tree.xgboost.XGBoostModel.XGBoostParameters
import hex.tree.xgboost.XGBoostModel.XGBoostParameters._
import org.apache.spark.annotation.Since
import org.apache.spark.ml.h2o.param.{EnumParam, H2OAlgoParams}
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader}

/**
  * H2O XGBoost algorithm exposed via Spark ML pipelines.
  */
class H2OXGBoost(override val uid: String) extends H2OAlgorithm[XGBoostParameters]
    with H2OXGBoostParams {

  def this() = this(Identifiable.randomUID("xgboost"))

  override def defaultFileName: String = H2OXGBoost.defaultFileName

  override def trainModel(params: XGBoostParameters): XGBoostModel = new XGBoost(params).trainModel().get()
}

object H2OXGBoost extends MLReadable[H2OXGBoost] {

  private final val defaultFileName = "xgboost_params"

  @Since("1.6.0")
  override def read: MLReader[H2OXGBoost] = H2OAlgorithmReader.create[H2OXGBoost](defaultFileName)

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
  private val quietMode = booleanParam("quietMode")
  private val missingValuesHandling = new MissingValuesHandlingParam(this, "missingValuesHandling", "Missing Values Handling")
  private val ntrees = intParam("ntrees")
  private val nEstimators = intParam("nEstimators")
  private val maxDepth = intParam("maxDepth")
  private val minRows = doubleParam("minRows")
  private val minChildWeight = doubleParam("minChildWeight")
  private val learnRate = doubleParam("learnRate")
  private val eta = doubleParam("eta")
  private val learnRateAnnealing = doubleParam("learnRateAnnealing", "Learn Rate Annealing")
  private val sampleRate = doubleParam("sampleRate")
  private val subsample = doubleParam("subsample")
  private val colSampleRate = doubleParam("colSampleRate")
  private val colSampleByLevel = doubleParam("colSampleByLevel", "Col Sample By Level")
  private val colSampleRatePerTree = doubleParam("colSampleRatePerTree")
  private val colsampleBytree = doubleParam("colsampleBytree")
  private val maxAbsLeafnodePred = floatParam("maxAbsLeafnodePred")
  private val maxDeltaStep = floatParam("maxDeltaStep")
  private val scoreTreeInterval = intParam("scoreTreeInterval")
  private val initialScoreInterval = intParam("initialScoreInterval", "Initial Score Interval")
  private val scoreInterval = intParam("scoreInterval", "Score Interval")
  private val minSplitImprovement = floatParam("minSplitImprovement")
  private val gamma = floatParam("gamma")
  private val nthread = intParam("nthread")
  private val maxBins = intParam("maxBins")
  private val maxLeaves = intParam("maxLeaves")
  private val minSumHessianInLeaf = floatParam("minSumHessianInLeaf")
  private val minDataInLeaf = floatParam("minDataInLeaf")
  private val treeMethod = new TreeMethodParam(this, "treeMethod", "Tree Method")
  private val growPolicy = new GrowPolicyParam(this, "growPolicy", "Grow Policy")
  private val booster = new BoosterParam(this, "booster", "Booster")
  private val dmatrixType = new DMatrixTypeParam(this, "dmatrixType", "DMatrix type")
  private val regLambda = floatParam("regLambda")
  private val regAlpha = floatParam("regAlpha")
  private val sampleType = new DartSampleTypeParam(this, "sampleType", "Dart Sample Type")
  private val normalizeType = new DartNormalizeTypeParam(this, "normalizeType", "Dart Normalize Type")
  private val rateDrop = floatParam("rateDrop")
  private val oneDrop = booleanParam("oneDrop")
  private val skipDrop = floatParam("skipDrop")
  private val gpuId = intParam("gpuId")
  private val backend = new BackendParam(this, "backend", "Backend")
  //
  // Default values
  //
  setDefault(
    quietMode -> true,
    missingValuesHandling -> null,
    ntrees -> 50,
    nEstimators -> 0,
    maxDepth -> 6,
    minRows -> 1,
    minChildWeight -> 1,
    learnRate -> 0.3,
    eta -> 0.3,
    learnRateAnnealing -> 1,
    sampleRate -> 1.0,
    subsample -> 1.0,
    colSampleRate -> 1.0,
    colSampleByLevel -> 1.0,
    colSampleRatePerTree -> 1.0,
    colsampleBytree -> 1.0,
    maxAbsLeafnodePred -> 0,
    maxDeltaStep -> 0,
    scoreTreeInterval -> 0,
    initialScoreInterval -> 4000,
    scoreInterval -> 4000,
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
  def getQuietMode(): Boolean = $(quietMode)

  def getMissingValuesHandling(): MissingValuesHandling = $(missingValuesHandling)

  def getNtrees(): Int = $(ntrees)

  def getNEstimators(): Int = $(nEstimators)

  def getMaxDepth(): Int = $(maxDepth)

  def getMinRows(): Double = $(minRows)

  def getMinChildWeight(): Double = $(minChildWeight)

  def getLearnRate(): Double = $(learnRate)

  def getEta(): Double = $(eta)

  def getLearnRateAnnealing(): Double = $(learnRateAnnealing)

  def getSampleRate(): Double = $(sampleRate)

  def getSubsample(): Double = $(subsample)

  def getColSampleRate(): Double = $(colSampleRate)

  def getColSampleByLevel(): Double = $(colSampleByLevel)

  def getColSampleRatePerTree(): Double = $(colSampleRatePerTree)

  def getColsampleBytree(): Double = $(colsampleBytree)

  def getMaxAbsLeafnodePred(): Float = $(maxAbsLeafnodePred)

  def getMaxDeltaStep(): Float = $(maxDeltaStep)

  def getScoreTreeInterval(): Int = $(scoreTreeInterval)

  def getInitialScoreInterval(): Int = $(initialScoreInterval)

  def getScoreInterval(): Int = $(scoreInterval)

  def getMinSplitImprovement(): Float = $(minSplitImprovement)

  def getGamma(): Float = $(gamma)

  def getNthread(): Int = $(nthread)

  def getMaxBins(): Int = $(maxBins)

  def getMaxLeaves(): Int = $(maxLeaves)

  def getMinSumHessianInLeaf(): Float = $(minSumHessianInLeaf)

  def getMinDataInLeaf(): Float = $(minDataInLeaf)

  def getTreeMethod(): TreeMethod = $(treeMethod)

  def getGrowPolicy(): GrowPolicy = $(growPolicy)

  def getBooster(): Booster = $(booster)

  def getDmatrixType(): DMatrixType = $(dmatrixType)

  def getRegLambda(): Float = $(regLambda)

  def getRegAlpha(): Float = $(regAlpha)

  def getSampleType(): DartSampleType = $(sampleType)

  def getNormalizeType(): DartNormalizeType = $(normalizeType)

  def getRateDrop(): Float = $(rateDrop)

  def getOneDrop(): Boolean = $(oneDrop)

  def getSkipDrop(): Float = $(skipDrop)

  def getGpuId(): Int = $(gpuId)

  def getBackend(): Backend = $(backend)

  //
  // Setters
  //
  def setQuietMode(value: Boolean): this.type = set(quietMode, value)

  def setMissingValuesHandling(value: MissingValuesHandling): this.type = set(missingValuesHandling, value)

  def setNtrees(value: Int): this.type = set(ntrees, value)

  def setNEstimators(value: Int): this.type = set(nEstimators, value)

  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  def setMinRows(value: Double): this.type = set(minRows, value)

  def setMinChildWeight(value: Double): this.type = set(minChildWeight, value)

  def setLearnRate(value: Double): this.type = set(learnRate, value)

  def setEta(value: Double): this.type = set(eta, value)

  def setLearnRateAnnealing(value: Double): this.type = set(learnRateAnnealing, value)

  def setSampleRate(value: Double): this.type = set(sampleRate, value)

  def setSubsample(value: Double): this.type = set(subsample, value)

  def setColSampleRate(value: Double): this.type = set(colSampleRate, value)

  def setColSampleByLevel(value: Double): this.type = set(colSampleByLevel, value)

  def setColSampleRatePerTree(value: Double): this.type = set(colSampleRatePerTree, value)

  def setColsampleBytree(value: Double): this.type = set(colsampleBytree, value)

  def setMaxAbsLeafnodePred(value: Float): this.type = set(maxAbsLeafnodePred, value)

  def setMaxDeltaStep(value: Float): this.type = set(maxDeltaStep, value)

  def setScoreTreeInterval(value: Int): this.type = set(scoreTreeInterval, value)

  def setInitialScoreInterval(value: Int): this.type = set(initialScoreInterval, value)

  def setScoreInterval(value: Int): this.type = set(scoreInterval, value)

  def setMinSplitImprovement(value: Float): this.type = set(minSplitImprovement, value)

  def setGamma(value: Float): this.type = set(gamma, value)

  def setNthread(value: Int): this.type = set(nthread, value)

  def setMaxBins(value: Int): this.type = set(maxBins, value)

  def setMaxLeaves(value: Int): this.type = set(maxLeaves, value)

  def setMinSumHessianInLeaf(value: Float): this.type = set(minSumHessianInLeaf, value)

  def setMinDataInLeaf(value: Float): this.type = set(minDataInLeaf, value)

  def setTreeMethod(value: TreeMethod): this.type = set(treeMethod, value)

  def setGrowPolicy(value: GrowPolicy): this.type = set(growPolicy, value)

  def setBooster(value: Booster): this.type = set(booster, value)

  def setDmatrixType(value: DMatrixType): this.type = set(dmatrixType, value)

  def setRegLambda(value: Float): this.type = set(regLambda, value)

  def setRegAlpha(value: Float): this.type = set(regAlpha, value)

  def setSampleType(value: DartSampleType): this.type = set(sampleType, value)

  def setNormalizeType(value: DartNormalizeType): this.type = set(normalizeType, value)

  def setRateDrop(value: Float): this.type = set(rateDrop, value)

  def setOneDrop(value: Boolean): this.type = set(oneDrop, value)

  def setSkipDrop(value: Float): this.type = set(skipDrop, value)

  def setGpuId(value: Int): this.type = set(gpuId, value)

  def setBackend(value: Backend): this.type = set(backend, value)

  override def updateH2OParams(): Unit = {
    super.updateH2OParams()
    parameters._quiet_mode = $(quietMode)
    parameters._missing_values_handling = $(missingValuesHandling)
    parameters._ntrees = $(ntrees)
    parameters._n_estimators = $(nEstimators)
    parameters._max_depth = $(maxDepth)
    parameters._min_rows = $(minRows)
    parameters._min_child_weight = $(minChildWeight)
    parameters._learn_rate = $(learnRate)
    parameters._eta = $(eta)
    parameters._learn_rate_annealing = $(learnRateAnnealing)
    parameters._sample_rate = $(sampleRate)
    parameters._subsample = $(subsample)
    parameters._col_sample_rate = $(colSampleRate)
    parameters._colsample_bylevel = $(colSampleByLevel)
    parameters._col_sample_rate_per_tree = $(colSampleRatePerTree)
    parameters._colsample_bytree = $(colsampleBytree)
    parameters._max_abs_leafnode_pred = $(maxAbsLeafnodePred)
    parameters._max_delta_step = $(maxDeltaStep)
    parameters._score_tree_interval = $(scoreTreeInterval)
    parameters._initial_score_interval = $(initialScoreInterval)
    parameters._score_interval = $(scoreInterval)
    parameters._min_split_improvement = $(minSplitImprovement)
    parameters._gamma = $(gamma)
    parameters._nthread = $(nthread)
    parameters._max_bins = $(maxBins)
    parameters._max_leaves = $(maxLeaves)
    parameters._min_sum_hessian_in_leaf = $(minSumHessianInLeaf)
    parameters._min_data_in_leaf = $(minDataInLeaf)
    parameters._tree_method = $(treeMethod)
    parameters._grow_policy = $(growPolicy)
    parameters._booster = $(booster)
    parameters._dmatrix_type = $(dmatrixType)
    parameters._reg_lambda = $(regLambda)
    parameters._reg_alpha = $(regAlpha)
    parameters._sample_type = $(sampleType)
    parameters._normalize_type = $(normalizeType)
    parameters._rate_drop = $(rateDrop)
    parameters._one_drop = $(oneDrop)
    parameters._skip_drop = $(skipDrop)
    parameters._gpu_id = $(gpuId)
    parameters._backend = $(backend)
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
