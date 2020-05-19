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

from ai.h2o.sparkling.ml.params.H2OSharedTreeParams import H2OSharedTreeParams
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from ai.h2o.sparkling.ml.params.HasMonotoneConstraints import HasMonotoneConstraints
from ai.h2o.sparkling.ml.params.HasQuantileAlpha import HasQuantileAlpha



class H2OGBMParams(H2OSharedTreeParams, HasMonotoneConstraints, HasQuantileAlpha):
    ##
    # Param definitions
    ##
    learnRate = Param(
        Params._dummy(),
        "learnRate",
        "Learning rate (from 0.0 to 1.0)",
        H2OTypeConverters.toFloat())

    learnRateAnnealing = Param(
        Params._dummy(),
        "learnRateAnnealing",
        "Scale the learning rate by this factor after each tree (e.g., 0.99 or 0.999)",
        H2OTypeConverters.toFloat())

    colSampleRate = Param(
        Params._dummy(),
        "colSampleRate",
        "Column sample rate (from 0.0 to 1.0)",
        H2OTypeConverters.toFloat())

    maxAbsLeafnodePred = Param(
        Params._dummy(),
        "maxAbsLeafnodePred",
        "Maximum absolute value of a leaf node prediction",
        H2OTypeConverters.toFloat())

    predNoiseBandwidth = Param(
        Params._dummy(),
        "predNoiseBandwidth",
        "Bandwidth (sigma) of Gaussian multiplicative noise ~N(1,sigma) for tree node predictions",
        H2OTypeConverters.toFloat())

    classSamplingFactors = Param(
        Params._dummy(),
        "classSamplingFactors",
        "Desired over/under-sampling ratios per class (in lexicographic order). If not specified, sampling factors "
        "will be automatically computed to obtain class balance during training. Requires balance_classes.",
        H2OTypeConverters.toNullableListFloat())

    checkConstantResponse = Param(
        Params._dummy(),
        "checkConstantResponse",
        "Check if response column is constant. If enabled, then an exception is thrown if the response column "
        "is a constant value.If disabled, then model will train regardless of the response column being a constant "
        "value or not.",
        H2OTypeConverters.toBoolean())

    customDistributionFunc = Param(
        Params._dummy(),
        "customDistributionFunc",
        "Reference to custom distribution, format: `language:keyName=funcName`",
        H2OTypeConverters.toNullableString())

    customMetricFunc = Param(
        Params._dummy(),
        "customMetricFunc",
        "Reference to custom evaluation function, format: `language:keyName=funcName`",
        H2OTypeConverters.toNullableString())

    maxRuntimeSecs = Param(
        Params._dummy(),
        "maxRuntimeSecs",
        "Maximum allowed runtime in seconds for model training. Use 0 to disable.",
        H2OTypeConverters.toFloat())

    foldAssignment = Param(
        Params._dummy(),
        "foldAssignment",
        "Cross-validation fold assignment scheme, if fold_column is not specified. The 'Stratified' option will "
        "stratify the folds based on the response variable, for classification problems.",
        H2OTypeConverters.toEnumString("hex.Model$Parameters$FoldAssignmentScheme"))

    exportCheckpointsDir = Param(
        Params._dummy(),
        "exportCheckpointsDir",
        "Automatically export generated models to this directory.",
        H2OTypeConverters.toNullableString())

    maxAfterBalanceSize = Param(
        Params._dummy(),
        "maxAfterBalanceSize",
        "Maximum relative size of the training data after balancing class counts (can be less than 1.0). "
        "Requires balance_classes.",
        H2OTypeConverters.toFloat())

    calibrateModel = Param(
        Params._dummy(),
        "calibrateModel",
        "Use Platt Scaling to calculate calibrated class probabilities. Calibration can provide more accurate "
        "estimates of class probabilities.",
        H2OTypeConverters.toBoolean())

    ignoredCols = Param(
        Params._dummy(),
        "ignoredCols",
        "Names of columns to ignore for training.",
        H2OTypeConverters.toNullableListString())

    ignoreConstCols = Param(
        Params._dummy(),
        "ignoreConstCols",
        "Ignore constant columns.",
        H2OTypeConverters.toBoolean())

    balanceClasses = Param(
        Params._dummy(),
        "balanceClasses",
        "Balance training data class counts via over/under-sampling (for imbalanced data).",
        H2OTypeConverters.toBoolean())

    huberAlpha = Param(
        Params._dummy(),
        "huberAlpha",
        "Desired quantile for Huber/M-regression (threshold between quadratic and linear loss,"
        " must be between 0 and 1).",
        H2OTypeConverters.toFloat())

    tweediePower = Param(
        Params._dummy(),
        "tweediePower",
        "Tweedie power for Tweedie regression, must be between 1 and 2.",
        H2OTypeConverters.toFloat())

    scoreEachIteration = Param(
        Params._dummy(),
        "scoreEachIteration",
        "Whether to score during each iteration of model training.",
        H2OTypeConverters.toBoolean())

    categoricalEncoding = Param(
        Params._dummy(),
        "categoricalEncoding",
        "Encoding scheme for categorical features",
        H2OTypeConverters.toEnumString("hex.Model$Parameters$CategoricalEncodingScheme"))

    maxCategoricalLevels = Param(
        Params._dummy(),
        "maxCategoricalLevels",
        "For every categorical feature, only use this many most frequent categorical levels for model training. "
        "Only used for categorical_encoding == EnumLimited.",
        H2OTypeConverters.toInt())

    keepCrossValidationModels = Param(
        Params._dummy(),
        "keepCrossValidationModels",
        "Whether to keep the cross-validation models.",
        H2OTypeConverters.toBoolean())

    balanceClasses = Param(
        Params._dummy(),
        "balanceClasses",
        "Balance training data class counts via over/under-sampling (for imbalanced data).",
        H2OTypeConverters.toBoolean())

    ##
    # Getters
    ##
    def getLearnRate(self):
        return self.getOrDefault(self.learnRate)

    def getLearnRateAnnealing(self):
        return self.getOrDefault(self.learnRateAnnealing)

    def getColSampleRate(self):
        return self.getOrDefault(self.colSampleRate)

    def getMaxAbsLeafnodePred(self):
        return self.getOrDefault(self.maxAbsLeafnodePred)

    def getPredNoiseBandwidth(self):
        return self.getOrDefault(self.predNoiseBandwidth)

    def getClassSamplingFactors(self):
        return self.getOrDefault(self.classSamplingFactors)

    def getCheckConstantResponse(self):
        return self.getOrDefault(self.checkConstantResponse)

    def getCustomDistributionFunc(self):
        return self.getOrDefault(self.customDistributionFunc)

    def getCustomMetricFunc(self):
        return self.getOrDefault(self.customMetricFunc)

    def getMaxRuntimeSecs(self):
        return self.getOrDefault(self.maxRuntimeSecs)

    def getFoldAssignment(self):
        return self.getOrDefault(self.foldAssignment)

    def getExportCheckpointsDir(self):
        return self.getOrDefault(self.exportCheckpointsDir)

    def getMaxAfterBalanceSize(self):
        return self.getOrDefault(self.maxAfterBalanceSize)

    def getCalibrateModel(self):
        return self.getOrDefault(self.calibrateModel)

    def getIgnoredCols(self):
        return self.getOrDefault(self.ignoredCols)

    def getIgnoreConstCols(self):
        return self.getOrDefault(self.ignoreConstCols)

    def getBalanceClasses(self):
        return self.getOrDefault(self.balanceClasses)

    def getHuberAlpha(self):
        return self.getOrDefault(self.huberAlpha)

    def getTweediePower(self):
        return self.getOrDefault(self.tweediePower)

    def getScoreEachIteration(self):
        return self.getOrDefault(self.scoreEachIteration)

    def getCategoricalEncoding(self):
        return self.getOrDefault(self.categoricalEncoding)

    def getMaxCategoricalLevels(self):
        return self.getOrDefault(self.maxCategoricalLevels)

    def getKeepCrossValidationModels(self):
        return self.getOrDefault(self.keepCrossValidationModels)

    def getBalanceClasses(self):
        return self.getOrDefault(self.balanceClasses)

    ##
    # Setters
    ##
    def setLearnRate(self, value):
        return self._set(learnRate=value)

    def setLearnRateAnnealing(self, value):
        return self._set(learnRateAnnealing=value)

    def setColSampleRate(self, value):
        return self._set(colSampleRate=value)

    def setMaxAbsLeafnodePred(self, value):
        return self._set(maxAbsLeafnodePred=value)

    def setPredNoiseBandwidth(self, value):
        return self._set(predNoiseBandwidth=value)

    def setClassSamplingFactors(self, value):
        return self._set(classSamplingFactors=value)

    def setCheckConstantResponse(self, value):
        return self._set(checkConstantResponse=value)

    def setCustomDistributionFunc(self, value):
        return self._set(customDistributionFunc=value)

    def setCustomMetricFunc(self, value):
        return self._set(customMetricFunc=value)

    def setMaxRuntimeSecs(self, value):
        return self._set(maxRuntimeSecs=value)

    def setFoldAssignment(self, value):
        return self._set(foldAssignment=value)

    def setExportCheckpointsDir(self, value):
        return self._set(exportCheckpointsDir=value)

    def setMaxAfterBalanceSize(self, value):
        return self._set(maxAfterBalanceSize=value)

    def setCalibrateModel(self, value):
        return self._set(calibrateModel=value)

    def setIgnoredCols(self, value):
        return self._set(ignoredCol=value)

    def setIgnoreConstCols(self, value):
        return self._set(ignoreConstCol=value)

    def setBalanceClasses(self, value):
        return self._set(balanceClasses=value)

    def setHuberAlpha(self, value):
        return self._set(huberAlpha=value)

    def setTweediePower(self, value):
        return self._set(tweediePower=value)

    def setScoreEachIteration(self, value):
        return self._set(scoreEachIteration=value)

    def setCategoricalEncoding(self, value):
        return self._set(categoricalEncoding=value)

    def setMaxCategoricalLevels(self, value):
        return self._set(maxCategoricalLevels=value)

    def setKeepCrossValidationModels(self, value):
        return self._set(keepCrossValidationModels=value)

    def setBalanceClasses(self, value):
        return self._set(balanceClasses=value)
