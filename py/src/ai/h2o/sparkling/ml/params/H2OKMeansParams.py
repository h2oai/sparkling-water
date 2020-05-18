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

from ai.h2o.sparkling.ml.params.H2OAlgoUnsupervisedParams import H2OAlgoUnsupervisedParams
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters


class H2OKMeansParams(H2OAlgoUnsupervisedParams):
    maxIterations = Param(
        Params._dummy(),
        "maxIterations",
        "Maximum number of KMeans iterations to find the centroids.",
        H2OTypeConverters.toInt())

    standardize = Param(
        Params._dummy(),
        "standardize",
        "Standardize the numeric columns to have a mean of zero and unit variance.",
        H2OTypeConverters.toBoolean())

    init = Param(
        Params._dummy(),
        "init",
        "Initialization mode for finding the initial cluster centers.",
        H2OTypeConverters.toEnumString("hex.kmeans.KMeans$Initialization"))

    userPoints = Param(
        Params._dummy(),
        "userPoints",
        "This option enables to specify array of points, where each point represents coordinates of "
        "an initial cluster center. The user-specified points must have the same number of columns "
        "as the training observations. The number of rows must equal the number of clusters.",
        H2OTypeConverters.toNullableListListFloat())

    estimateK = Param(
        Params._dummy(),
        "estimateK",
        "If enabled, the algorithm tries to identify optimal number of clusters, up to k clusters.",
        H2OTypeConverters.toBoolean())

    k = Param(
        Params._dummy(),
        "k",
        "Number of clusters to generate.",
        H2OTypeConverters.toInt())

    quantileAlpha = Param(
        Params._dummy(),
        "quantileAlpha",
        "Desired quantile for Quantile regression, must be between 0 and 1.",
        H2OTypeConverters.toFloat())

    tweediePower = Param(
        Params._dummy(),
        "tweediePower",
        "Tweedie power for Tweedie regression, must be between 1 and 2.",
        H2OTypeConverters.toFloat())

    maxCategoricalLevels = Param(
        Params._dummy(),
        "maxCategoricalLevels",
        "For every categorical feature, only use this many most frequent categorical levels for model training. "
        "Only used for categorical_encoding == EnumLimited.",
        H2OTypeConverters.toInt())

    ignoredCols = Param(
        Params._dummy(),
        "ignoredCols",
        "Names of columns to ignore for training.",
        H2OTypeConverters.toNullableListString())

    #
    # Getters
    #
    def getMaxIterations(self):
        return self.getOrDefault(self.maxIterations)

    def getStandardize(self):
        return self.getOrDefault(self.standardize)

    def getInit(self):
        return self.getOrDefault(self.init)

    def getUserPoints(self):
        return self.getOrDefault(self.userPoints)

    def getEstimateK(self):
        return self.getOrDefault(self.estimateK)

    def getK(self):
        return self.getOrDefault(self.k)

    def getQuantileAlpha(self):
        return self.getOrDefault(self.quantileAlpha)

    def getTweediePower(self):
        return self.getOrDefault(self.tweediePower)

    def getMaxCategoricalLevels(self):
        return self.getOrDefault(self.maxCategoricalLevels)

    def getIgnoredCols(self):
        return self.getOrDefault(self.ignoredCols)

    #
    # Setters
    #
    def setMaxIterations(self, value):
        return self._set(maxIterations=value)

    def setStandardize(self, value):
        return self._set(standardize=value)

    def setInit(self, value):
        return self._set(init=value)

    def setUserPoints(self, value):
        return self._set(userPoints=value)

    def setEstimateK(self, value):
        return self._set(estimateK=value)

    def setK(self, value):
        return self._set(k=value)

    def setQuantileAlpha(self, value):
        return self._set(quantileAlpha=value)

    def setTweediePower(self, value):
        return self._set(tweediePower=value)

    def setMaxCategoricalLevels(self, value):
        return self._set(maxCategoricalLevels=value)

    def setIgnoredCols(self, value):
        return self._set(ignoredCol=value)
