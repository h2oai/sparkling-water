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

from ai.h2o.sparkling.ml.params.H2OCommonSupervisedParams import H2OCommonSupervisedParams
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from ai.h2o.sparkling.ml.params.HasMonotoneConstraints import HasMonotoneConstraints


class H2OAutoMLParams(H2OCommonSupervisedParams, HasMonotoneConstraints):
    ##
    # Param definitions
    ##
    ignoredCols = Param(
        Params._dummy(),
        "ignoredCols",
        "Ignored column names",
        H2OTypeConverters.toListString())

    includeAlgos = Param(
        Params._dummy(),
        "includeAlgos",
        "Algorithms to include when using automl",
        H2OTypeConverters.toEnumListString("ai.h2o.automl.Algo"))

    excludeAlgos = Param(
        Params._dummy(),
        "excludeAlgos",
        "Algorithms to exclude when using automl",
        H2OTypeConverters.toEnumListString("ai.h2o.automl.Algo"))

    projectName = Param(
        Params._dummy(),
        "projectName",
        "identifier for models that should be grouped together in the leaderboard "
        "(e.g., airlines and iris)",
        H2OTypeConverters.toNullableString())

    maxRuntimeSecs = Param(
        Params._dummy(),
        "maxRuntimeSecs",
        "Maximum time in seconds for automl to be running",
        H2OTypeConverters.toFloat())

    stoppingRounds = Param(
        Params._dummy(),
        "stoppingRounds",
        "Stopping rounds",
        H2OTypeConverters.toInt())

    stoppingTolerance = Param(
        Params._dummy(),
        "stoppingTolerance",
        "Stopping tolerance",
        H2OTypeConverters.toFloat())

    stoppingMetric = Param(
        Params._dummy(),
        "stoppingMetric",
        "Stopping metric",
        H2OTypeConverters.toEnumString("hex.ScoreKeeper$StoppingMetric"))

    sortMetric = Param(
        Params._dummy(),
        "sortMetric",
        "Sort metric for the AutoML leaderboard",
        H2OTypeConverters.toEnumString("ai.h2o.sparkling.ml.algos.H2OAutoMLSortMetric"))

    balanceClasses = Param(
        Params._dummy(),
        "balanceClasses",
        "Balance classes",
        H2OTypeConverters.toBoolean())

    classSamplingFactors = Param(
        Params._dummy(),
        "classSamplingFactors",
        "Class sampling factors",
        H2OTypeConverters.toNullableListFloat())

    maxAfterBalanceSize = Param(
        Params._dummy(),
        "maxAfterBalanceSize",
        "Max after balance size",
        H2OTypeConverters.toFloat())

    keepCrossValidationPredictions = Param(
        Params._dummy(),
        "keepCrossValidationPredictions",
        "Keep cross validation predictions",
        H2OTypeConverters.toBoolean())

    keepCrossValidationModels = Param(
        Params._dummy(),
        "keepCrossValidationModels",
        "Keep cross validation models",
        H2OTypeConverters.toBoolean())

    maxModels = Param(
        Params._dummy(),
        "maxModels",
        "Max models to train in AutoML",
        H2OTypeConverters.toInt())

    ##
    # Getters
    ##
    def getIgnoredCols(self):
        return self.getOrDefault(self.ignoredCols)

    def getTryMutations(self):
        return self.getOrDefault(self.tryMutations)

    def getExcludeAlgos(self):
        return self.getOrDefault(self.excludeAlgos)

    def getIncludeAlgos(self):
        return self.getOrDefault(self.includeAlgos)

    def getProjectName(self):
        return self.getOrDefault(self.projectName)

    def getMaxRuntimeSecs(self):
        return self.getOrDefault(self.maxRuntimeSecs)

    def getStoppingRounds(self):
        return self.getOrDefault(self.stoppingRounds)

    def getStoppingTolerance(self):
        return self.getOrDefault(self.stoppingTolerance)

    def getStoppingMetric(self):
        return self.getOrDefault(self.stoppingMetric)

    def getSortMetric(self):
        return self.getOrDefault(self.sortMetric)

    def getBalanceClasses(self):
        return self.getOrDefault(self.balanceClasses)

    def getClassSamplingFactors(self):
        return self.getOrDefault(self.classSamplingFactors)

    def getMaxAfterBalanceSize(self):
        return self.getOrDefault(self.maxAfterBalanceSize)

    def getKeepCrossValidationPredictions(self):
        return self.getOrDefault(self.keepCrossValidationPredictions)

    def getKeepCrossValidationModels(self):
        return self.getOrDefault(self.keepCrossValidationModels)

    def getMaxModels(self):
        return self.getOrDefault(self.maxModels)

    ##
    # Setters
    ##
    def setIgnoredCols(self, value):
        return self._set(ignoredCols=value)

    def setTryMutations(self, value):
        return self._set(tryMutations=value)

    def setIncludeAlgos(self, value):
        return self._set(includeAlgos=value)

    def setExcludeAlgos(self, value):
        return self._set(excludeAlgos=value)

    def setProjectName(self, value):
        return self._set(projectName=value)

    def setMaxRuntimeSecs(self, value):
        return self._set(maxRuntimeSecs=value)

    def setStoppingRounds(self, value):
        return self._set(stoppingRounds=value)

    def setStoppingTolerance(self, value):
        return self._set(stoppingTolerance=value)

    def setStoppingMetric(self, value):
        return self._set(stoppingMetric=value)

    def setSortMetric(self, value):
        return self._set(sortMetric=value)

    def setBalanceClasses(self, value):
        return self._set(balanceClasses=value)

    def setClassSamplingFactors(self, value):
        return self._set(classSamplingFactors=value)

    def setMaxAfterBalanceSize(self, value):
        return self._set(maxAfterBalanceSize=value)

    def setKeepCrossValidationPredictions(self, value):
        return self._set(keepCrossValidationPredictions=value)

    def setKeepCrossValidationModels(self, value):
        return self._set(keepCrossValidationModels=value)

    def setMaxModels(self, value):
        return self._set(maxModels=value)
