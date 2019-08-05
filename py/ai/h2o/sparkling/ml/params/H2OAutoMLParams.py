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

from ai.h2o.sparkling.ml.params.H2OCommonSupervisedParams import H2OCommonSupervisedParams
from ai.h2o.sparkling.ml.utils import getValidatedEnumValue, getValidatedEnumValues, getDoubleArrayFromIntArray


class H2OAutoMLParams(H2OCommonSupervisedParams):

    ##
    # Param definitions
    ##
    ignoredCols = Param(Params._dummy(), "ignoredCols", "Ignored column names")
    includeAlgos = Param(Params._dummy(), "includeAlgos", "Algorithms to include when using automl")
    excludeAlgos = Param(Params._dummy(), "excludeAlgos", "Algorithms to exclude when using automl")
    projectName = Param(Params._dummy(), "projectName", "identifier for models that should be grouped together in the leaderboard" +
                        " (e.g., airlines and iris)")
    maxRuntimeSecs = Param(Params._dummy(), "maxRuntimeSecs", "Maximum time in seconds for automl to be running")
    stoppingRounds = Param(Params._dummy(), "stoppingRounds", "Stopping rounds")
    stoppingTolerance = Param(Params._dummy(), "stoppingTolerance", "Stopping tolerance")
    stoppingMetric = Param(Params._dummy(), "stoppingMetric", "Stopping metric")
    sortMetric = Param(Params._dummy(), "sortMetric", "Sort metric for the AutoML leaderboard")
    balanceClasses = Param(Params._dummy(), "balanceClasses", "Balance classes")
    classSamplingFactors = Param(Params._dummy(), "classSamplingFactors", "Class sampling factors")
    maxAfterBalanceSize = Param(Params._dummy(), "maxAfterBalanceSize", "Max after balance size")
    keepCrossValidationPredictions = Param(Params._dummy(), "keepCrossValidationPredictions", "Keep cross validation predictions")
    keepCrossValidationModels = Param(Params._dummy(), "keepCrossValidationModels", "Keep cross validation models")
    maxModels = Param(Params._dummy(), "maxModels", "Max models to train in AutoML")

    ##
    # Getters
    ##
    def getIgnoredCols(self):
        return self.getOrDefault(self.ignoredCols)

    def getTryMutations(self):
        return self.getOrDefault(self.tryMutations)

    def getExcludeAlgos(self):
        # Convert Java Array[String] to Python
        algos = self.getOrDefault(self.excludeAlgos)
        if algos is None:
            return None
        else:
            return [algo for algo in algos]

    def getIncludeAlgos(self):
        # Convert Java Array[String] to Python
        algos = self.getOrDefault(self.includeAlgos)
        if algos is None:
            return None
        else:
            return [algo for algo in algos]

    def getProjectName(self):
        return self.getOrDefault(self.projectName)

    def getLoss(self):
        return self.getOrDefault(self.loss)

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
        assert_is_type(value, [str])
        return self._set(ignoredCols=value)

    def setTryMutations(self, value):
        assert_is_type(value, bool)
        return self._set(tryMutations=value)

    def setIncludeAlgos(self, value):
        validated = getValidatedEnumValues(self.__getAutomlAlgoEnum(), value, nullEnabled=True)
        return self._set(includeAlgos=validated)

    def setExcludeAlgos(self, value):
        validated = getValidatedEnumValues(self.__getAutomlAlgoEnum(), value, nullEnabled=True)
        return self._set(excludeAlgos=validated)

    def __getAutomlAlgoEnum(self):
        return "ai.h2o.automl.Algo"

    def setProjectName(self, value):
        assert_is_type(value, None, str)
        return self._set(projectName=value)

    def setLoss(self, value):
        assert_is_type(value, "AUTO")
        return self._set(loss=value)

    def setMaxRuntimeSecs(self, value):
        assert_is_type(value, int, float)
        return self._set(maxRuntimeSecs=float(value))

    def setStoppingRounds(self, value):
        assert_is_type(value, int)
        return self._set(stoppingRounds=value)

    def setStoppingTolerance(self, value):
        assert_is_type(value, int, float)
        return self._set(stoppingTolerance=float(value))

    def setStoppingMetric(self, value):
        validated = getValidatedEnumValue(self.__getStoppingMetricEnum(), value)
        return self._set(stoppingMetric=validated)

    def __getStoppingMetricEnum(self):
        return "hex.ScoreKeeper$StoppingMetric"

    def setSortMetric(self, value):
        validated = getValidatedEnumValue(self.__getSortMetricEnum(), value)
        return self._set(sortMetric=validated)

    def __getSortMetricEnum(self):
        return "ai.h2o.sparkling.ml.algos.H2OAutoMLSortMetric"

    def setBalanceClasses(self, value):
        assert_is_type(value, bool)
        return self._set(balanceClasses=value)

    def setClassSamplingFactors(self, value):
        assert_is_type(value, [int, float])
        return self._set(classSamplingFactors=getDoubleArrayFromIntArray(array))

    def setMaxAfterBalanceSize(self, value):
        assert_is_type(value, int, float)
        return self._set(maxAfterBalanceSize=float(value))

    def setKeepCrossValidationPredictions(self, value):
        assert_is_type(value, bool)
        return self._set(keepCrossValidationPredictions=value)

    def setKeepCrossValidationModels(self, value):
        assert_is_type(value, bool)
        return self._set(keepCrossValidationModels=value)

    def setMaxModels(self, value):
        assert_is_type(value, int)
        return self._set(maxModels=value)
