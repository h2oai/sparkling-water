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
package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.macros.DeprecatedMethod
import ai.h2o.sparkling.ml.params.H2OAlgoParamsHelper._
import ai.h2o.sparkling.ml.params.{DeprecatableParams, H2OAlgoSupervisedParams, H2OTreeBasedSupervisedMOJOParams, HasMonotoneConstraints, HasStoppingCriteria}
import ai.h2o.sparkling.ml.utils.H2OParamsReadable
import hex.ScoreKeeper.StoppingMetric
import hex.schemas.XGBoostV3.XGBoostParametersV3
import hex.tree.xgboost.XGBoostModel.XGBoostParameters
import hex.tree.xgboost.XGBoostModel.XGBoostParameters._
import hex.tree.xgboost.{XGBoost, XGBoostModel}
import org.apache.spark.ml.util.Identifiable

/**
  * H2O XGBoost algorithm exposed via Spark ML pipelines.
  */
class H2OXGBoost(override val uid: String)
  extends H2OTreeBasedSupervisedAlgorithm[XGBoost, XGBoostModel, XGBoostParameters] with H2OXGBoostParams {

  def this() = this(Identifiable.randomUID(classOf[H2OXGBoost].getSimpleName))
}

object H2OXGBoost extends H2OParamsReadable[H2OXGBoost]


/**
  * Parameters for Spark's API exposing underlying H2O model.
  */
trait H2OXGBoostParams extends H2OAlgoSupervisedParams[XGBoostParameters]
  with H2OTreeBasedSupervisedMOJOParams with HasMonotoneConstraints with HasStoppingCriteria[XGBoostParameters]
  with DeprecatableParams {

  type H2O_SCHEMA = XGBoostParametersV3

  protected def paramTag = reflect.classTag[XGBoostParameters]

  protected def schemaTag = reflect.classTag[H2O_SCHEMA]

  //
  // Param definitions
  //
  private val quietMode = booleanParam("quietMode")
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
  private val colSampleByTree = doubleParam("colSampleByTree", "Col Sample By Tree")
  private val maxAbsLeafnodePred = floatParam("maxAbsLeafnodePred")
  private val maxDeltaStep = floatParam("maxDeltaStep")
  private val scoreTreeInterval = intParam("scoreTreeInterval")
  private val minSplitImprovement = floatParam("minSplitImprovement")
  private val gamma = floatParam("gamma")
  private val nthread = intParam("nthread")
  private val maxBins = intParam("maxBins")
  private val maxLeaves = intParam("maxLeaves")
  private val minSumHessianInLeaf = floatParam("minSumHessianInLeaf")
  private val minDataInLeaf = floatParam("minDataInLeaf")
  private val treeMethod = stringParam("treeMethod", "Tree Method")
  private val growPolicy = stringParam("growPolicy", "Grow Policy")
  private val booster = stringParam("booster", "Booster")
  private val dmatrixType = stringParam("dmatrixType", "DMatrix type")
  private val regLambda = floatParam("regLambda")
  private val regAlpha = floatParam("regAlpha")
  private val sampleType = stringParam("sampleType", "Dart Sample Type")
  private val normalizeType = stringParam("normalizeType", "Dart Normalize Type")
  private val rateDrop = floatParam("rateDrop")
  private val oneDrop = booleanParam("oneDrop")
  private val skipDrop = floatParam("skipDrop")
  private val gpuId = intParam("gpuId")
  private val backend = stringParam("backend", "Backend")
  //
  // Default values
  //
  setDefault(
    quietMode -> true,
    ntrees -> 50,
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
    colSampleByTree -> 1.0,
    maxAbsLeafnodePred -> 0,
    maxDeltaStep -> 0,
    scoreTreeInterval -> 0,
    minSplitImprovement -> 0,
    gamma -> 0.0f,
    nthread -> -1,
    maxBins -> 256,
    maxLeaves -> 0,
    minSumHessianInLeaf -> 100,
    minDataInLeaf -> 0,
    treeMethod -> TreeMethod.auto.name(),
    growPolicy -> GrowPolicy.depthwise.name(),
    booster -> Booster.gbtree.name(),
    dmatrixType -> DMatrixType.auto.name(),
    regLambda -> 0,
    regAlpha -> 0,
    sampleType -> DartSampleType.uniform.name(),
    normalizeType -> DartNormalizeType.tree.name(),
    rateDrop -> 0,
    oneDrop -> false,
    skipDrop -> 0,
    gpuId -> 0, // which GPU to use
    backend -> Backend.auto.name()
  )

  //
  // Getters
  //
  def getQuietMode(): Boolean = $(quietMode)

  @DeprecatedMethod
  def getNEstimators(): Int = 0

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

  def getColSampleByTree(): Double = $(colSampleByTree)

  def getMaxAbsLeafnodePred(): Float = $(maxAbsLeafnodePred)

  def getMaxDeltaStep(): Float = $(maxDeltaStep)

  def getScoreTreeInterval(): Int = $(scoreTreeInterval)

  @DeprecatedMethod(version = "3.30")
  def getInitialScoreInterval(): Int = 4000

  @DeprecatedMethod(version = "3.30")
  def getScoreInterval(): Int = 4000

  def getMinSplitImprovement(): Float = $(minSplitImprovement)

  def getGamma(): Float = $(gamma)

  def getNthread(): Int = $(nthread)

  def getMaxBins(): Int = $(maxBins)

  def getMaxLeaves(): Int = $(maxLeaves)

  def getMinSumHessianInLeaf(): Float = $(minSumHessianInLeaf)

  def getMinDataInLeaf(): Float = $(minDataInLeaf)

  def getTreeMethod(): String = $(treeMethod)

  def getGrowPolicy(): String = $(growPolicy)

  def getBooster(): String = $(booster)

  def getDmatrixType(): String = $(dmatrixType)

  def getRegLambda(): Float = $(regLambda)

  def getRegAlpha(): Float = $(regAlpha)

  def getSampleType(): String = $(sampleType)

  def getNormalizeType(): String = $(normalizeType)

  def getRateDrop(): Float = $(rateDrop)

  def getOneDrop(): Boolean = $(oneDrop)

  def getSkipDrop(): Float = $(skipDrop)

  def getGpuId(): Int = $(gpuId)

  def getBackend(): String = $(backend)

  //
  // Setters
  //
  def setQuietMode(value: Boolean): this.type = set(quietMode, value)

  def setNtrees(value: Int): this.type = set(ntrees, value)

  @DeprecatedMethod
  def setNEstimators(value: Int): this.type = this

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

  def setColSampleByTree(value: Double): this.type = set(colSampleByTree, value)

  def setMaxAbsLeafnodePred(value: Float): this.type = set(maxAbsLeafnodePred, value)

  def setMaxDeltaStep(value: Float): this.type = set(maxDeltaStep, value)

  def setScoreTreeInterval(value: Int): this.type = set(scoreTreeInterval, value)

  @DeprecatedMethod(version = "3.30")
  def setInitialScoreInterval(value: Int): this.type = this

  @DeprecatedMethod(version = "3.30")
  def setScoreInterval(value: Int): this.type = this

  def setMinSplitImprovement(value: Float): this.type = set(minSplitImprovement, value)

  def setGamma(value: Float): this.type = set(gamma, value)

  def setNthread(value: Int): this.type = set(nthread, value)

  def setMaxBins(value: Int): this.type = set(maxBins, value)

  def setMaxLeaves(value: Int): this.type = set(maxLeaves, value)

  def setMinSumHessianInLeaf(value: Float): this.type = set(minSumHessianInLeaf, value)

  def setMinDataInLeaf(value: Float): this.type = set(minDataInLeaf, value)

  def setTreeMethod(value: String): this.type = {
    val validated = getValidatedEnumValue[TreeMethod](value)
    set(treeMethod, validated)
  }

  def setGrowPolicy(value: String): this.type = {
    val validated = getValidatedEnumValue[GrowPolicy](value)
    set(growPolicy, validated)
  }

  def setBooster(value: String): this.type = {
    val validated = getValidatedEnumValue[Booster](value)
    set(booster, validated)
  }


  def setDmatrixType(value: String): this.type = {
    val validated = getValidatedEnumValue[DMatrixType](value)
    set(dmatrixType, validated)
  }

  def setRegLambda(value: Float): this.type = set(regLambda, value)

  def setRegAlpha(value: Float): this.type = set(regAlpha, value)

  def setSampleType(value: String): this.type = {
    val validated = getValidatedEnumValue[DartSampleType](value)
    set(sampleType, validated)
  }

  def setNormalizeType(value: String): this.type = {
    val validated = getValidatedEnumValue[DartNormalizeType](value)
    set(normalizeType, validated)
  }

  def setRateDrop(value: Float): this.type = set(rateDrop, value)

  def setOneDrop(value: Boolean): this.type = set(oneDrop, value)

  def setSkipDrop(value: Float): this.type = set(skipDrop, value)

  def setGpuId(value: Int): this.type = set(gpuId, value)

  def setBackend(value: String): this.type = {
    val validated = getValidatedEnumValue[Backend](value)
    set(backend, validated)
  }

  override def updateH2OParams(): Unit = {
    super.updateH2OParams()
    parameters._quiet_mode = $(quietMode)
    parameters._ntrees = $(ntrees)
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
    parameters._colsample_bytree = $(colSampleByTree)
    parameters._max_abs_leafnode_pred = $(maxAbsLeafnodePred)
    parameters._max_delta_step = $(maxDeltaStep)
    parameters._score_tree_interval = $(scoreTreeInterval)
    parameters._min_split_improvement = $(minSplitImprovement)
    parameters._gamma = $(gamma)
    parameters._nthread = $(nthread)
    parameters._max_bins = $(maxBins)
    parameters._max_leaves = $(maxLeaves)
    parameters._min_sum_hessian_in_leaf = $(minSumHessianInLeaf)
    parameters._min_data_in_leaf = $(minDataInLeaf)
    parameters._tree_method = TreeMethod.valueOf($(treeMethod))
    parameters._grow_policy = GrowPolicy.valueOf($(growPolicy))
    parameters._booster = Booster.valueOf($(booster))
    parameters._dmatrix_type = DMatrixType.valueOf($(dmatrixType))
    parameters._reg_lambda = $(regLambda)
    parameters._reg_alpha = $(regAlpha)
    parameters._sample_type = DartSampleType.valueOf($(sampleType))
    parameters._normalize_type = DartNormalizeType.valueOf($(normalizeType))
    parameters._rate_drop = $(rateDrop)
    parameters._one_drop = $(oneDrop)
    parameters._skip_drop = $(skipDrop)
    parameters._gpu_id = $(gpuId)
    parameters._backend = Backend.valueOf($(backend))
    parameters._monotone_constraints = getMonotoneConstraintsAsKeyValuePairs()
    parameters._stopping_rounds = getStoppingRounds()
    parameters._stopping_metric = StoppingMetric.valueOf(getStoppingMetric())
    parameters._stopping_tolerance = getStoppingTolerance()
  }

  /**
    * When a parameter is renamed, the mapping 'old name' -> 'new name' should be added into this map.
    */
  override protected def renamingMap: Map[String, String] = Map[String, String]()
}
