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

from ai.h2o.sparkling.ml.params.H2OGridSearchCartesianCriteriaParams import H2OGridSearchCartesianCriteriaParams
from ai.h2o.sparkling.ml.params.H2OGridSearchCommonCriteriaParams import H2OGridSearchCommonCriteriaParams
from ai.h2o.sparkling.ml.params.H2OGridSearchRandomDiscreteCriteriaParams import \
    H2OGridSearchRandomDiscreteCriteriaParams
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class H2OGridSearchParams(
    H2OGridSearchRandomDiscreteCriteriaParams,
    H2OGridSearchCartesianCriteriaParams,
    H2OGridSearchCommonCriteriaParams):
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

    selectBestModelBy = Param(
        Params._dummy(),
        "selectBestModelBy",
        "Specifies the metric which is used for comparing and sorting the models returned by the grid.",
        H2OTypeConverters.toEnumString("ai.h2o.sparkling.ml.internals.H2OMetric"))

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
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is None:
            return None
        algoName = javaAlgo.getClass().getSimpleName()
        if algoName.endswith("Classifier"):
            import ai.h2o.sparkling.ml.algos.classification
            algo = getattr(ai.h2o.sparkling.ml.algos.classification, algoName)()
        elif algoName.endswith("Regressor"):
            import ai.h2o.sparkling.ml.algos.regression
            algo = getattr(ai.h2o.sparkling.ml.algos.regression, algoName)()
        else:
            import ai.h2o.sparkling.ml.algos
            algo = getattr(ai.h2o.sparkling.ml.algos, algoName)()
        algo._resetUid(javaAlgo.uid())
        algo._java_obj = javaAlgo
        algo._transfer_params_from_java()
        return algo

    def getHyperParameters(self):
        return self.getOrDefault(self.hyperParameters)

    def getSelectBestModelBy(self):
        return self.getOrDefault(self.selectBestModelBy)

    def getParallelism(self):
        return self.getOrDefault(self.parallelism)

    ##
    # Setters
    ##
    def setAlgo(self, value):
        self._set(algo=value)
        self._transfer_params_to_java()
        return self

    def setHyperParameters(self, value):
        return self._set(hyperParameters=value)

    def setSelectBestModelBy(self, value):
        return self._set(selectBestModelBy=value)

    def setParallelism(self, value):
        return self._set(parallelism=value)
