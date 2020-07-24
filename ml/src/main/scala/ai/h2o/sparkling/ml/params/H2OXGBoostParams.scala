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

import ai.h2o.sparkling.macros.DeprecatedMethod
import ai.h2o.sparkling.ml.params.H2OAlgoParamsHelper.getValidatedEnumValue
import hex.schemas.XGBoostV3.XGBoostParametersV3
import hex.tree.xgboost.XGBoostModel.XGBoostParameters
import hex.tree.xgboost.XGBoostModel.XGBoostParameters._

trait H2OXGBoostParams
  extends H2OAlgoSupervisedParams[XGBoostParameters]
  with H2OTreeBasedSupervisedMOJOParams
  with HasMonotoneConstraints
  with HasStoppingCriteria[XGBoostParameters] {

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
  private val gainsliftBins = intParam(
    name = "gainsliftBins",
    doc = "Gains/Lift table number of bins. 0 means disabled.. Default value -1 means automatic binning.")

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
    backend -> Backend.auto.name(),
    gainsliftBins -> -1)

  //
  // Getters
  //
  def getQuietMode(): Boolean = $(quietMode)

  def getMaxDepth(): Int = $(maxDepth)

  def getMinRows(): Double = $(minRows)

  def getMinChildWeight(): Double = $(minChildWeight)

  def getLearnRate(): Double = $(learnRate)

  def getEta(): Double = $(eta)

  def getSampleRate(): Double = $(sampleRate)

  def getSubsample(): Double = $(subsample)

  def getColSampleRate(): Double = $(colSampleRate)

  def getColSampleByLevel(): Double = $(colSampleByLevel)

  def getColSampleRatePerTree(): Double = $(colSampleRatePerTree)

  def getColSampleByTree(): Double = $(colSampleByTree)

  def getMaxAbsLeafnodePred(): Float = $(maxAbsLeafnodePred)

  def getMaxDeltaStep(): Float = $(maxDeltaStep)

  def getScoreTreeInterval(): Int = $(scoreTreeInterval)

  def getMinSplitImprovement(): Float = $(minSplitImprovement)

  def getGamma(): Float = $(gamma)

  def getNthread(): Int = $(nthread)

  def getMaxBins(): Int = $(maxBins)

  def getMaxLeaves(): Int = $(maxLeaves)

  @DeprecatedMethod(version = "3.32")
  def getMinSumHessianInLeaf(): Float = 100

  @DeprecatedMethod(version = "3.32")
  def getMinDataInLeaf(): Float = 0

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

  def getGainsliftBins(): Int = $(gainsliftBins)

  //
  // Setters
  //
  def setQuietMode(value: Boolean): this.type = set(quietMode, value)

  def setNtrees(value: Int): this.type = set(ntrees, value)

  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  def setMinRows(value: Double): this.type = set(minRows, value)

  def setMinChildWeight(value: Double): this.type = set(minChildWeight, value)

  def setLearnRate(value: Double): this.type = set(learnRate, value)

  def setEta(value: Double): this.type = set(eta, value)

  def setSampleRate(value: Double): this.type = set(sampleRate, value)

  def setSubsample(value: Double): this.type = set(subsample, value)

  def setColSampleRate(value: Double): this.type = set(colSampleRate, value)

  def setColSampleByLevel(value: Double): this.type = set(colSampleByLevel, value)

  def setColSampleRatePerTree(value: Double): this.type = set(colSampleRatePerTree, value)

  def setColSampleByTree(value: Double): this.type = set(colSampleByTree, value)

  def setMaxAbsLeafnodePred(value: Float): this.type = set(maxAbsLeafnodePred, value)

  def setMaxDeltaStep(value: Float): this.type = set(maxDeltaStep, value)

  def setScoreTreeInterval(value: Int): this.type = set(scoreTreeInterval, value)

  def setMinSplitImprovement(value: Float): this.type = set(minSplitImprovement, value)

  def setGamma(value: Float): this.type = set(gamma, value)

  def setNthread(value: Int): this.type = set(nthread, value)

  def setMaxBins(value: Int): this.type = set(maxBins, value)

  def setMaxLeaves(value: Int): this.type = set(maxLeaves, value)

  @DeprecatedMethod(version = "3.32")
  def setMinSumHessianInLeaf(value: Float): this.type = this

  @DeprecatedMethod(version = "3.32")
  def setMinDataInLeaf(value: Float): this.type = this

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

  def setGainsliftBins(value: Int): this.type = {
    set(gainsliftBins, value)
  }

  override private[sparkling] def getH2OAlgorithmParams(): Map[String, Any] = {
    super.getH2OAlgorithmParams() ++
      Map(
        "quiet_mode" -> getQuietMode(),
        "ntrees" -> getNtrees(),
        "max_depth" -> getMaxDepth(),
        "min_rows" -> getMinRows(),
        "min_child_weight" -> getMinChildWeight(),
        "learn_rate" -> getLearnRate(),
        "eta" -> getEta(),
        "sample_rate" -> getSampleRate(),
        "subsample" -> getSubsample(),
        "col_sample_rate" -> getColSampleRate(),
        "colsample_bylevel" -> getColSampleByLevel(),
        "col_sample_rate_per_tree" -> getColSampleRatePerTree(),
        "colsample_bytree" -> getColSampleByTree(),
        "max_abs_leafnode_pred" -> getMaxAbsLeafnodePred(),
        "max_delta_step" -> getMaxDeltaStep(),
        "score_tree_interval" -> getScoreTreeInterval(),
        "min_split_improvement" -> getMinSplitImprovement(),
        "gamma" -> getGamma(),
        "nthread" -> getNthread(),
        "max_bins" -> getMaxBins(),
        "max_leaves" -> getMaxLeaves(),
        "tree_method" -> getTreeMethod(),
        "grow_policy" -> getGrowPolicy(),
        "booster" -> getBooster(),
        "dmatrix_type" -> getDmatrixType(),
        "reg_lambda" -> getRegLambda(),
        "reg_alpha" -> getRegAlpha(),
        "sample_type" -> getSampleType(),
        "normalize_type" -> getNormalizeType(),
        "rate_drop" -> getRateDrop(),
        "one_drop" -> getOneDrop(),
        "skip_drop" -> getSkipDrop(),
        "gpu_id" -> getGpuId(),
        "backend" -> getBackend(),
        "monotone_constraints" -> getMonotoneConstraints(),
        "stopping_rounds" -> getStoppingRounds(),
        "stopping_metric" -> getStoppingMetric(),
        "stopping_tolerance" -> getStoppingTolerance(),
        "gainslift_bins" -> getGainsliftBins())
  }
}
