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

from ai.h2o.sparkling.ml.params.H2OCommonSupervisedParams import H2OCommonSupervisedParams
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class H2OGridSearchParams(H2OCommonSupervisedParams):
    ##
    # Param definitions
    ##
    algo = Param(
        Params._dummy(),
        "algo",
        "Algo to run grid search on",
        H2OTypeConverters.toH2OGridSearchSupportedAlgo())

    hyperParameters = Param(
        Params._dummy(),
        "hyperParameters",
        "Grid Search Hyper Params map",
        H2OTypeConverters.toDictionaryWithAnyElements())

    strategy = Param(
        Params._dummy(),
        "strategy",
        "strategy",
        H2OTypeConverters.toEnumString("hex.grid.HyperSpaceSearchCriteria$Strategy"))

    maxRuntimeSecs = Param(
        Params._dummy(),
        "maxRuntimeSecs",
        "maxRuntimeSecs",
        H2OTypeConverters.toFloat())

    maxModels = Param(
        Params._dummy(),
        "maxModels",
        "maxModels",
        H2OTypeConverters.toInt())

    stoppingRounds = Param(
        Params._dummy(),
        "stoppingRounds",
        "stoppingRounds",
        H2OTypeConverters.toInt())

    stoppingTolerance = Param(
        Params._dummy(),
        "stoppingTolerance",
        "stoppingTolerance",
        H2OTypeConverters.toFloat())

    stoppingMetric = Param(
        Params._dummy(),
        "stoppingMetric",
        "stoppingMetric",
        H2OTypeConverters.toEnumString("hex.ScoreKeeper$StoppingMetric"))

    selectBestModelBy = Param(
        Params._dummy(),
        "selectBestModelBy",
        "Specifies the metric which is used for comparing and sorting the models returned by the grid.",
        H2OTypeConverters.toEnumString("ai.h2o.sparkling.ml.algos.H2OGridSearchMetric"))

    parallelism = Param(
        Params._dummy(),
        "parallelism",
        """Level of model-building parallelism, the possible values are:
           0 -> H2O selects parallelism level based on cluster configuration, such as number of cores
           1 -> Sequential model building, no parallelism
           n>1 -> n models will be built in parallel if possible""",
        H2OTypeConverters.toInt())

    ##
    # Getters
    ##
    def getAlgo(self):
        javaAlgo = self.getOrDefault(self.algo)
        algoName = javaAlgo.parameters().algoName()
        if algoName == "GBM":
            from ai.h2o.sparkling.ml.algos import H2OGBM
            algo = H2OGBM()
        elif algoName == "DeepLearning":
            from ai.h2o.sparkling.ml.algos import H2ODeepLearning
            algo = H2ODeepLearning()
        elif algoName == "XGBoost":
            from ai.h2o.sparkling.ml.algos import H2OXGBoost
            algo = H2OXGBoost()
        elif algoName == "GLM":
            from ai.h2o.sparkling.ml.algos import H2OGLM
            algo = H2OGLM()
        elif algoName == "DRF":
            from ai.h2o.sparkling.ml.algos import H2ODRF
            algo = H2ODRF()
        else:
            raise ValueError('Unsupported algorithm for H2OGridSearch')

        algo._resetUid(javaAlgo.uid())
        algo._java_obj = javaAlgo
        algo._transfer_params_from_java()
        return algo

    def getHyperParameters(self):
        return self.getOrDefault(self.hyperParameters)

    def getStrategy(self):
        return self.getOrDefault(self.strategy)

    def getMaxRuntimeSecs(self):
        return self.getOrDefault(self.maxRuntimeSecs)

    def getMaxModels(self):
        return self.getOrDefault(self.maxModels)

    def getStoppingRounds(self):
        return self.getOrDefault(self.stoppingRounds)

    def getStoppingTolerance(self):
        return self.getOrDefault(self.stoppingTolerance)

    def getStoppingMetric(self):
        return self.getOrDefault(self.stoppingMetric)

    def getSelectBestModelBy(self):
        return self.getOrDefault(self.selectBestModelBy)

    def getParallelism(self):
        return self.getOrDefault(self.parallelism)

    ##
    # Setters
    ##
    def setAlgo(self, value):
        return self._set(algo=value)

    def setHyperParameters(self, value):
        return self._set(hyperParameters=value)

    def setStrategy(self, value):
        return self._set(strategy=value)

    def setMaxRuntimeSecs(self, value):
        return self._set(maxRuntimeSecs=value)

    def setMaxModels(self, value):
        return self._set(maxModels=value)

    def setStoppingRounds(self, value):
        return self._set(stoppingRounds=value)

    def setStoppingTolerance(self, value):
        return self._set(stoppingTolerance=value)

    def setStoppingMetric(self, value):
        return self._set(stoppingMetric=value)

    def setSelectBestModelBy(self, value):
        return self._set(selectBestModelBy=value)

    def setParallelism(self, value):
        return self._set(parallelism=value)
