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

from ai.h2o.sparkling.ml.params.H2OSharedTreeParams import H2OSharedTreeParams
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class H2ODRFParams(H2OSharedTreeParams):
    ##
    # Param definitions
    ##
    binomialDoubleTrees = Param(
        Params._dummy(),
        "binomialDoubleTrees",
        "In case of binary classification, build 2 times more trees (one per class) - can lead "
        "to higher accuracy.",
        H2OTypeConverters.toBoolean())

    mtries = Param(
        Params._dummy(),
        "mtries",
        "Number of variables randomly sampled as candidates at each split. If set to -1, defaults "
        "to sqrt{p} for classification and p/3 for regression (where p is the # of predictors",
        H2OTypeConverters.toInt())

    customDistributionFunc = Param(
        Params._dummy(),
        "customDistributionFunc",
        "Reference to custom distribution, format: `language:keyName=funcName`",
        H2OTypeConverters.toNullableString())

    maxRuntimeSecs = Param(
        Params._dummy(),
        "maxRuntimeSecs",
        "Maximum allowed runtime in seconds for model training. Use 0 to disable.",
        H2OTypeConverters.toFloat())

    exportCheckpointsDir = Param(
        Params._dummy(),
        "exportCheckpointsDir",
        "Automatically export generated models to this directory.",
        H2OTypeConverters.toNullableString())

    classSamplingFactors = Param(
        Params._dummy(),
        "classSamplingFactors",
        "Desired over/under-sampling ratios per class (in lexicographic order). If not specified, sampling factors "
        "will be automatically computed to obtain class balance during training. Requires balance_classes.",
        H2OTypeConverters.toNullableListFloat())

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

    quantileAlpha = Param(
        Params._dummy(),
        "quantileAlpha",
        "Desired quantile for Quantile regression, must be between 0 and 1.",
        H2OTypeConverters.toFloat())

    customMetricFunc = Param(
        Params._dummy(),
        "customMetricFunc",
        "Reference to custom evaluation function, format: `language:keyName=funcName`",
        H2OTypeConverters.toNullableString())

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

    scoreEachIteration = Param(
        Params._dummy(),
        "scoreEachIteration",
        "Whether to score during each iteration of model training.",
        H2OTypeConverters.toBoolean())

    maxCategoricalLevels = Param(
        Params._dummy(),
        "maxCategoricalLevels",
        "For every categorical feature, only use this many most frequent categorical levels for model training. "
        "Only used for categorical_encoding == EnumLimited.",
        H2OTypeConverters.toInt())

    maxAfterBalanceSize = Param(
        Params._dummy(),
        "maxAfterBalanceSize",
        "Maximum relative size of the training data after balancing class counts (can be less than 1.0). "
        "Requires balance_classes.",
        H2OTypeConverters.toFloat())

    balanceClasses = Param(
        Params._dummy(),
        "balanceClasses",
        "Balance training data class counts via over/under-sampling (for imbalanced data).",
        H2OTypeConverters.toBoolean())

    foldAssignment = Param(
        Params._dummy(),
        "foldAssignment",
        "Cross-validation fold assignment scheme, if fold_column is not specified. The 'Stratified' option will "
        "stratify the folds based on the response variable, for classification problems.",
        H2OTypeConverters.toEnumString("hex.Model$Parameters$FoldAssignmentScheme"))

    categoricalEncoding = Param(
        Params._dummy(),
        "categoricalEncoding",
        "Encoding scheme for categorical features",
        H2OTypeConverters.toEnumString("hex.Model$Parameters$CategoricalEncodingScheme"))

    ##
    # Getters
    ##
    def getBinomialDoubleTrees(self):
        return self.getOrDefault(self.binomialDoubleTrees)

    def getMtries(self):
        return self.getOrDefault(self.mtries)

    def getCustomDistributionFunc(self):
        return self.getOrDefault(self.customDistributionFunc)

    def getMaxRuntimeSecs(self):
        return self.getOrDefault(self.maxRuntimeSecs)

    def getExportCheckpointsDir(self):
        return self.getOrDefault(self.exportCheckpointsDir)

    def getClassSamplingFactors(self):
        return self.getOrDefault(self.classSamplingFactors)

    def getHuberAlpha(self):
        return self.getOrDefault(self.huberAlpha)

    def getTweediePower(self):
        return self.getOrDefault(self.tweediePower)

    def getQuantileAlpha(self):
        return self.getOrDefault(self.quantileAlpha)

    def getCustomMetricFunc(self):
        return self.getOrDefault(self.customMetricFunc)

    def getIgnoredCols(self):
        return self.getOrDefault(self.ignoredCols)

    def getIgnoreConstCols(self):
        return self.getOrDefault(self.ignoreConstCols)

    def getScoreEachIteration(self):
        return self.getOrDefault(self.scoreEachIteration)

    def getMaxCategoricalLevels(self):
        return self.getOrDefault(self.maxCategoricalLevels)

    def getMaxAfterBalanceSize(self):
        return self.getOrDefault(self.maxAfterBalanceSize)

    def getBalanceClasses(self):
        return self.getOrDefault(self.balanceClasses)

    def getFoldAssignment(self):
        return self.getOrDefault(self.foldAssignment)

    def getCategoricalEncoding(self):
        return self.getOrDefault(self.categoricalEncoding)

    ##
    # Setters
    ##
    def setBinomialDoubleTrees(self, value):
        return self._set(binomialDoubleTrees=value)

    def setMtries(self, value):
        return self._set(mtries=value)

    def setCustomDistributionFunc(self, value):
        return self._set(customDistributionFunc=value)

    def setMaxRuntimeSecs(self, value):
        return self._set(maxRuntimeSecs=value)

    def setExportCheckpointsDir(self, value):
        return self._set(exportCheckpointsDir=value)

    def setClassSamplingFactors(self, value):
        return self._set(classSamplingFactors=value)

    def setHuberAlpha(self, value):
        return self._set(huberAlpha=value)

    def setTweediePower(self, value):
        return self._set(tweediePower=value)

    def setQuantileAlpha(self, value):
        return self._set(quantileAlpha=value)

    def setCustomMetricFunc(self, value):
        return self.getOrDefault(customMetricFunc=value)

    def setIgnoredCols(self, value):
        return self._set(ignoredCol=value)

    def setIgnoreConstCols(self, value):
        return self._set(ignoreConstCol=value)

    def setScoreEachIteration(self, value):
        return self._set(scoreEachIteration=value)

    def setMaxCategoricalLevels(self, value):
        return self._set(maxCategoricalLevels=value)

    def setMaxAfterBalanceSize(self, value):
        return self._set(maxAfterBalanceSize=value)

    def setBalanceClasses(self, value):
        return self._set(balanceClasses=value)

    def setFoldAssignment(self, value):
        return self._set(foldAssignment=value)

    def setCategoricalEncoding(self, value):
        return self._set(categoricalEncoding=value)
