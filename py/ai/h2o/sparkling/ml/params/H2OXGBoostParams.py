#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from h2o.utils.typechecks import assert_is_type
from pyspark.ml.param import *

from ai.h2o.sparkling.ml.params.H2OAlgoSupervisedParams import H2OAlgoSupervisedParams
from ai.h2o.sparkling.ml.utils import getValidatedEnumValue, getValidatedEnumValues


class H2OXGBoostParams(H2OAlgoSupervisedParams):

    ##
    # Param definitions
    ##
    quietMode = Param(Params._dummy(), "quietMode", "Quiet mode")
    ntrees = Param(Params._dummy(), "ntrees", "Number of trees")
    nEstimators = Param(Params._dummy(), "nEstimators", "number of estimators")
    maxDepth = Param(Params._dummy(), "maxDepth", "Maximal depth")
    minRows = Param(Params._dummy(), "minRows", "Min rows")
    minChildWeight = Param(Params._dummy(), "minChildWeight", "minimal child weight")
    learnRate = Param(Params._dummy(), "learnRate", "learn rate")
    eta = Param(Params._dummy(), "eta", "eta")
    learnRateAnnealing = Param(Params._dummy(), "learnRateAnnealing", "Learn Rate Annealing")
    sampleRate = Param(Params._dummy(), "sampleRate", "Sample rate")
    subsample = Param(Params._dummy(), "subsample", "subsample")
    colSampleRate = Param(Params._dummy(), "colSampleRate", "col sample rate")
    colSampleByLevel = Param(Params._dummy(), "colSampleByLevel", "Col Sample By Level")
    colSampleRatePerTree = Param(Params._dummy(), "colSampleRatePerTree", "col samle rate")
    colsampleBytree = Param(Params._dummy(), "colsampleBytree", "col sample by tree")
    maxAbsLeafnodePred = Param(Params._dummy(), "maxAbsLeafnodePred", "max abs lead node prediction")
    maxDeltaStep = Param(Params._dummy(), "maxDeltaStep", "max delta step")
    scoreTreeInterval = Param(Params._dummy(), "scoreTreeInterval", "score tree interval")
    initialScoreInterval = Param(Params._dummy(), "initialScoreInterval", "Initial Score Interval")
    scoreInterval = Param(Params._dummy(), "scoreInterval", "Score Interval")
    minSplitImprovement = Param(Params._dummy(), "minSplitImprovement", "Min split improvement")
    gamma = Param(Params._dummy(), "gamma", "gamma")
    nthread = Param(Params._dummy(), "nthread", "nthread")
    maxBins = Param(Params._dummy(), "maxBins", "nbins")
    maxLeaves = Param(Params._dummy(), "maxLeaves", "max leaves")
    minSumHessianInLeaf = Param(Params._dummy(), "minSumHessianInLeaf", "min sum hessian in leaf")
    minDataInLeaf = Param(Params._dummy(), "minDataInLeaf", "min data in leaf")
    treeMethod = Param(Params._dummy(), "treeMethod", "Tree Method")
    growPolicy = Param(Params._dummy(), "growPolicy", "Grow Policy")
    booster = Param(Params._dummy(), "booster", "Booster")
    dmatrixType = Param(Params._dummy(), "dmatrixType", "DMatrix type")
    regLambda = Param(Params._dummy(), "regLambda", "req lambda")
    regAlpha = Param(Params._dummy(), "regAlpha", "req aplha")
    sampleType = Param(Params._dummy(), "sampleType", "Dart Sample Type")
    normalizeType = Param(Params._dummy(), "normalizeType", "Dart Normalize Type")
    rateDrop = Param(Params._dummy(), "rateDrop", "rate drop")
    oneDrop = Param(Params._dummy(), "oneDrop", "onde drop")
    skipDrop = Param(Params._dummy(), "skipDrop", "skip drop")
    gpuId = Param(Params._dummy(), "gpuId", "GPU id")
    backend = Param(Params._dummy(), "backend", "Backend")

    ##
    # Getters
    ##
    def getQuietMode(self):
        return self.getOrDefault(self.quietMode)

    def getNtrees(self):
        return self.getOrDefault(self.ntrees)

    def getNEstimators(self):
        return self.getOrDefault(self.nEstimators)

    def getMaxDepth(self):
        return self.getOrDefault(self.maxDepth)

    def getMinRows(self):
        return self.getOrDefault(self.minRows)

    def getMinChildWeight(self):
        return self.getOrDefault(self.minChildWeight)

    def getLearnRate(self):
        return self.getOrDefault(self.learnRate)

    def getEta(self):
        return self.getOrDefault(self.eta)

    def getLearnRateAnnealing(self):
        return self.getOrDefault(self.learnRateAnnealing)

    def getSampleRate(self):
        return self.getOrDefault(self.sampleRate)

    def getSubsample(self):
        return self.getOrDefault(self.subsample)

    def getColSampleRate(self):
        return self.getOrDefault(self.colSampleRate)

    def getColSampleByLevel(self):
        return self.getOrDefault(self.colSampleByLevel)

    def getColSampleRatePerTree(self):
        return self.getOrDefault(self.colSampleRatePerTree)

    def getColsampleBytree(self):
        return self.getOrDefault(self.colsampleBytree)

    def getMaxAbsLeafnodePred(self):
        return self.getOrDefault(self.maxAbsLeafnodePred)

    def getMaxDeltaStep(self):
        return self.getOrDefault(self.maxDeltaStep)

    def getScoreTreeInterval(self):
        return self.getOrDefault(self.scoreTreeInterval)

    def getInitialScoreInterval(self):
        return self.getOrDefault(self.initialScoreInterval)

    def getScoreInterval(self):
        return self.getOrDefault(self.scoreInterval)

    def getMinSplitImprovement(self):
        return self.getOrDefault(self.minSplitImprovement)

    def getGamma(self):
        return self.getOrDefault(self.gamma)

    def getNthread(self):
        return self.getOrDefault(self.nthread)

    def getMaxBins(self):
        return self.getOrDefault(self.maxBins)

    def getMaxLeaves(self):
        return self.getOrDefault(self.maxLeaves)

    def getMinSumHessianInLeaf(self):
        return self.getOrDefault(self.minSumHessianInLeaf)

    def getMinDataInLeaf(self):
        return self.getOrDefault(self.minDataInLeaf)

    def getTreeMethod(self):
        return self.getOrDefault(self.treeMethod)

    def getGrowPolicy(self):
        return self.getOrDefault(self.growPolicy)

    def getBooster(self):
        return self.getOrDefault(self.booster)

    def getDmatrixType(self):
        return self.getOrDefault(self.dmatrixType)

    def getRegLambda(self):
        return self.getOrDefault(self.regLambda)

    def getRegAlpha(self):
        return self.getOrDefault(self.regAlpha)

    def getSampleType(self):
        return self.getOrDefault(self.sampleType)

    def getNormalizeType(self):
        return self.getOrDefault(self.normalizeType)

    def getRateDrop(self):
        return self.getOrDefault(self.rateDrop)

    def getOneDrop(self):
        return self.getOrDefault(self.oneDrop)

    def getSkipDrop(self):
        return self.getOrDefault(self.skipDrop)

    def getGpuId(self):
        return self.getOrDefault(self.gpuId)

    def getBackend(self):
        return self.getOrDefault(self.backend)


    ##
    # Setters
    ##
    def setQuietMode(self, value):
        assert_is_type(value, bool)
        return self._set(quietMode=value)

    def setNtrees(self, value):
        assert_is_type(value, int)
        return self._set(ntrees=value)

    def setNEstimators(self, value):
        assert_is_type(value, int)
        return self._set(nEstimators=value)

    def setMaxDepth(self, value):
        assert_is_type(value, int)
        return self._set(maxDepth=value)

    def setMinRows(self, value):
        assert_is_type(value, int, float)
        return self._set(minRows=float(value))

    def setMinChildWeight(self, value):
        assert_is_type(value, int, float)
        return self._set(minChildWeight=float(value))

    def setLearnRate(self, value):
        assert_is_type(value, int, float)
        return self._set(learnRate=float(value))

    def setEta(self, value):
        assert_is_type(value, int, float)
        return self._set(eta=float(value))

    def setLearnRateAnnealing(self, value):
        assert_is_type(value, int, float)
        return self._set(learnRateAnnealing=float(value))

    def setSampleRate(self, value):
        assert_is_type(value, int, float)
        return self._set(sampleRate=float(value))

    def setSubsample(self, value):
        assert_is_type(value, int, float)
        return self._set(subsample=float(value))

    def setColSampleRate(self, value):
        assert_is_type(value, int, float)
        return self._set(colSampleRate=float(value))

    def setColSampleByLevel(self, value):
        assert_is_type(value, int, float)
        return self._set(colSampleByLevel=float(value))

    def setColSampleRatePerTree(self, value):
        assert_is_type(value, int, float)
        return self._set(colSampleRatePerTree=float(value))

    def setColsampleBytree(self, value):
        assert_is_type(value, int, float)
        return self._set(colsampleBytree=float(value))

    def setMaxAbsLeafnodePred(self, value):
        assert_is_type(value, int, float)
        return self._set(maxAbsLeafnodePred=float(value))

    def setMaxDeltaStep(self, value):
        assert_is_type(value, int, float)
        return self._set(maxDeltaStep=float(value))

    def setScoreTreeInterval(self, value):
        assert_is_type(value, int)
        return self._set(scoreTreeInterval=value)

    def setInitialScoreInterval(self, value):
        assert_is_type(value, int)
        return self._set(initialScoreInterval=value)

    def setScoreInterval(self, value):
        assert_is_type(value, int)
        return self._set(scoreInterval=value)

    def setMinSplitImprovement(self, value):
        assert_is_type(value, int, float)
        return self._set(minSplitImprovement=float(value))

    def setGamma(self, value):
        assert_is_type(value, int, float)
        return self._set(gamma=float(value))

    def setNthread(self, value):
        assert_is_type(value, int)
        return self._set(nthread=value)

    def setMaxBins(self, value):
        assert_is_type(value, int)
        return self._set(maxBins=value)

    def setMaxLeaves(self, value):
        assert_is_type(value, int)
        return self._set(maxLeaves=value)

    def setMinSumHessianInLeaf(self, value):
        assert_is_type(value, int, float)
        return self._set(minSumHessianInLeaf=float(value))

    def setMinDataInLeaf(self, value):
        assert_is_type(value, int, float)
        return self._set(minDataInLeaf=float(value))

    def setTreeMethod(self, value):
        validated = getValidatedEnumValue(self.__getTreeMethodEnum(), value)
        return self._set(treeMethod=validated)

    def __getTreeMethodEnum(self):
        return "hex.tree.xgboost.XGBoostModel$XGBoostParameters$TreeMethod"

    def setGrowPolicy(self, value):
        validated = getValidatedEnumValue(self.__getGrowPolicyEnum(), value)
        return self._set(growPolicy=validated)

    def __getGrowPolicyEnum(self):
        return "hex.tree.xgboost.XGBoostModel$XGBoostParameters$GrowPolicy"

    def setBooster(self, value):
        validated = getValidatedEnumValue(self.__getBoosterEnum(), value)
        return self._set(booster=validated)

    def __getBoosterEnum(self):
        return "hex.tree.xgboost.XGBoostModel$XGBoostParameters$Booster"

    def setDmatrixType(self, value):
        validated = getValidatedEnumValue(self.__getDmatrixTypeEnum(), value)
        return self._set(dmatrixType=validated)

    def __getDmatrixTypeEnum(self):
        return "hex.tree.xgboost.XGBoostModel$XGBoostParameters$DMatrixType"

    def setRegLambda(self, value):
        assert_is_type(value, int, float)
        return self._set(regLambda=float(value))

    def setRegAlpha(self, value):
        assert_is_type(value, int, float)
        return self._set(regAlpha=float(value))

    def setSampleType(self, value):
        validated = getValidatedEnumValue(self.__getSampleTypeEnum(), value)
        return self._set(sampleType=validated)

    def __getSampleTypeEnum(self):
        return "hex.tree.xgboost.XGBoostModel$XGBoostParameters$DartSampleType"

    def setNormalizeType(self, value):
        validated = getValidatedEnumValues(self.__getNormalizeTypeEnum(), value)
        return self._set(normalizeType=validated)

    def __getNormalizeTypeEnum(self):
        return "hex.tree.xgboost.XGBoostModel$XGBoostParameters$DartNormalizeType"

    def setRateDrop(self, value):
        assert_is_type(value, int, float)
        return self._set(rateDrop=float(value))

    def setOneDrop(self, value):
        assert_is_type(value, bool)
        return self._set(oneDrop=value)

    def setSkipDrop(self, value):
        assert_is_type(value, int, float)
        return self._set(skipDrop=float(value))

    def setGpuId(self, value):
        assert_is_type(value, int)
        return self._set(gpuId=value)

    def setBackend(self, value):
        validated = getValidatedEnumValue(self.__getBackendEnum(), value)
        return self._set(backend=validated)

    def __getBackendEnum(self):
        return "hex.tree.xgboost.XGBoostModel$XGBoostParameters$Backend"
