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

from pyspark.ml.param import *

from ai.h2o.sparkling.ml.params.H2OAlgoSupervisedParams import H2OAlgoSupervisedParams
from ai.h2o.sparkling.ml.params.H2OTreeBasedSupervisedMOJOParams import H2OTreeBasedSupervisedMOJOParams
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from ai.h2o.sparkling.ml.params.HasMonotoneConstraints import HasMonotoneConstraints
from ai.h2o.sparkling.ml.params.HasStoppingCriteria import HasStoppingCriteria
from ai.h2o.sparkling.ml.Utils import Utils

class H2OXGBoostParams(H2OAlgoSupervisedParams, H2OTreeBasedSupervisedMOJOParams, HasMonotoneConstraints,
                       HasStoppingCriteria):
    ##
    # Param definitions
    ##
    quietMode = Param(
        Params._dummy(),
        "quietMode",
        "Quiet mode",
        H2OTypeConverters.toBoolean())

    maxDepth = Param(
        Params._dummy(),
        "maxDepth",
        "Maximal depth",
        H2OTypeConverters.toInt())

    minRows = Param(
        Params._dummy(),
        "minRows",
        "Min rows",
        H2OTypeConverters.toFloat())

    minChildWeight = Param(
        Params._dummy(),
        "minChildWeight",
        "minimal child weight",
        H2OTypeConverters.toFloat())

    learnRate = Param(
        Params._dummy(),
        "learnRate",
        "learn rate",
        H2OTypeConverters.toFloat())

    eta = Param(
        Params._dummy(),
        "eta",
        "eta",
        H2OTypeConverters.toFloat())

    sampleRate = Param(
        Params._dummy(),
        "sampleRate",
        "Sample rate",
        H2OTypeConverters.toFloat())

    subsample = Param(
        Params._dummy(),
        "subsample",
        "subsample",
        H2OTypeConverters.toFloat())

    colSampleRate = Param(
        Params._dummy(),
        "colSampleRate",
        "col sample rate",
        H2OTypeConverters.toFloat())

    colSampleByLevel = Param(
        Params._dummy(),
        "colSampleByLevel",
        "Col Sample By Level",
        H2OTypeConverters.toFloat())

    colSampleRatePerTree = Param(
        Params._dummy(),
        "colSampleRatePerTree",
        "col samle rate",
        H2OTypeConverters.toFloat())

    colSampleByTree = Param(
        Params._dummy(),
        "colSampleByTree",
        "col sample by tree",
        H2OTypeConverters.toFloat())

    maxAbsLeafnodePred = Param(
        Params._dummy(),
        "maxAbsLeafnodePred",
        "max abs lead node prediction",
        H2OTypeConverters.toFloat())

    maxDeltaStep = Param(
        Params._dummy(),
        "maxDeltaStep",
        "max delta step",
        H2OTypeConverters.toFloat())

    scoreTreeInterval = Param(
        Params._dummy(),
        "scoreTreeInterval",
        "score tree interval",
        H2OTypeConverters.toInt())

    minSplitImprovement = Param(
        Params._dummy(),
        "minSplitImprovement",
        "Min split improvement",
        H2OTypeConverters.toFloat())

    gamma = Param(
        Params._dummy(),
        "gamma",
        "gamma",
        H2OTypeConverters.toFloat())

    nthread = Param(
        Params._dummy(),
        "nthread",
        "nthread",
        H2OTypeConverters.toInt())

    maxBins = Param(
        Params._dummy(),
        "maxBins",
        "nbins",
        H2OTypeConverters.toInt())

    maxLeaves = Param(
        Params._dummy(),
        "maxLeaves",
        "max leaves",
        H2OTypeConverters.toInt())

    minSumHessianInLeaf = Param(
        Params._dummy(),
        "minSumHessianInLeaf",
        "min sum hessian in leaf",
        H2OTypeConverters.toFloat())

    minDataInLeaf = Param(
        Params._dummy(),
        "minDataInLeaf",
        "min data in leaf",
        H2OTypeConverters.toFloat())

    treeMethod = Param(
        Params._dummy(),
        "treeMethod",
        "Tree Method",
        H2OTypeConverters.toEnumString("hex.tree.xgboost.XGBoostModel$XGBoostParameters$TreeMethod"))

    growPolicy = Param(
        Params._dummy(),
        "growPolicy",
        "Grow Policy",
        H2OTypeConverters.toEnumString("hex.tree.xgboost.XGBoostModel$XGBoostParameters$GrowPolicy"))

    booster = Param(
        Params._dummy(),
        "booster",
        "Booster",
        H2OTypeConverters.toEnumString("hex.tree.xgboost.XGBoostModel$XGBoostParameters$Booster"))

    dmatrixType = Param(
        Params._dummy(),
        "dmatrixType",
        "DMatrix type",
        H2OTypeConverters.toEnumString("hex.tree.xgboost.XGBoostModel$XGBoostParameters$DMatrixType"))

    regLambda = Param(
        Params._dummy(),
        "regLambda",
        "req lambda",
        H2OTypeConverters.toFloat())

    regAlpha = Param(
        Params._dummy(),
        "regAlpha",
        "req aplha",
        H2OTypeConverters.toFloat())

    sampleType = Param(
        Params._dummy(),
        "sampleType",
        "Dart Sample Type",
        H2OTypeConverters.toEnumString("hex.tree.xgboost.XGBoostModel$XGBoostParameters$DartSampleType"))

    normalizeType = Param(
        Params._dummy(),
        "normalizeType",
        "Dart Normalize Type",
        H2OTypeConverters.toEnumString("hex.tree.xgboost.XGBoostModel$XGBoostParameters$DartNormalizeType"))

    rateDrop = Param(
        Params._dummy(),
        "rateDrop",
        "rate drop",
        H2OTypeConverters.toFloat())

    oneDrop = Param(
        Params._dummy(),
        "oneDrop",
        "onde drop",
        H2OTypeConverters.toBoolean())

    skipDrop = Param(
        Params._dummy(),
        "skipDrop",
        "skip drop",
        H2OTypeConverters.toFloat())

    gpuId = Param(
        Params._dummy(),
        "gpuId",
        "GPU id",
        H2OTypeConverters.toInt())

    backend = Param(
        Params._dummy(),
        "backend",
        "Backend",
        H2OTypeConverters.toEnumString("hex.tree.xgboost.XGBoostModel$XGBoostParameters$Backend"))

    ##
    # Getters
    ##
    def getQuietMode(self):
        return self.getOrDefault(self.quietMode)

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

    def getColSampleByTree(self):
        return self.getOrDefault(self.colSampleByTree)

    def getMaxAbsLeafnodePred(self):
        return self.getOrDefault(self.maxAbsLeafnodePred)

    def getMaxDeltaStep(self):
        return self.getOrDefault(self.maxDeltaStep)

    def getScoreTreeInterval(self):
        return self.getOrDefault(self.scoreTreeInterval)

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
        return self._set(quietMode=value)

    def setNtrees(self, value):
        return self._set(ntrees=value)

    def setMaxDepth(self, value):
        return self._set(maxDepth=value)

    def setMinRows(self, value):
        return self._set(minRows=value)

    def setMinChildWeight(self, value):
        return self._set(minChildWeight=value)

    def setLearnRate(self, value):
        return self._set(learnRate=value)

    def setEta(self, value):
        return self._set(eta=value)

    def setSampleRate(self, value):
        return self._set(sampleRate=value)

    def setSubsample(self, value):
        return self._set(subsample=value)

    def setColSampleRate(self, value):
        return self._set(colSampleRate=value)

    def setColSampleByLevel(self, value):
        return self._set(colSampleByLevel=value)

    def setColSampleRatePerTree(self, value):
        return self._set(colSampleRatePerTree=value)

    def setColSampleByTree(self, value):
        return self._set(colSampleByTree=value)

    def setMaxAbsLeafnodePred(self, value):
        return self._set(maxAbsLeafnodePred=value)

    def setMaxDeltaStep(self, value):
        return self._set(maxDeltaStep=value)

    def setScoreTreeInterval(self, value):
        return self._set(scoreTreeInterval=value)

    def setMinSplitImprovement(self, value):
        return self._set(minSplitImprovement=value)

    def setGamma(self, value):
        return self._set(gamma=value)

    def setNthread(self, value):
        return self._set(nthread=value)

    def setMaxBins(self, value):
        return self._set(maxBins=value)

    def setMaxLeaves(self, value):
        return self._set(maxLeaves=value)

    def setMinSumHessianInLeaf(self, value):
        return self._set(minSumHessianInLeaf=value)

    def setMinDataInLeaf(self, value):
        return self._set(minDataInLeaf=value)

    def setTreeMethod(self, value):
        return self._set(treeMethod=value)

    def setGrowPolicy(self, value):
        return self._set(growPolicy=value)

    def setBooster(self, value):
        return self._set(booster=value)

    def setDmatrixType(self, value):
        return self._set(dmatrixType=value)

    def setRegLambda(self, value):
        return self._set(regLambda=value)

    def setRegAlpha(self, value):
        return self._set(regAlpha=value)

    def setSampleType(self, value):
        return self._set(sampleType=value)

    def setNormalizeType(self, value):
        return self._set(normalizeType=value)

    def setRateDrop(self, value):
        return self._set(rateDrop=value)

    def setOneDrop(self, value):
        return self._set(oneDrop=value)

    def setSkipDrop(self, value):
        return self._set(skipDrop=value)

    def setGpuId(self, value):
        return self._set(gpuId=value)

    def setBackend(self, value):
        return self._set(backend=value)
