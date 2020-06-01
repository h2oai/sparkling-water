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

from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *

import warnings


class H2OGridSearchParams(Params):
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
        H2OTypeConverters.toEnumString("ai.h2o.sparkling.ml.internals.H2OMetric"))

    parallelism = Param(
        Params._dummy(),
        "parallelism",
        """Level of model-building parallelism, the possible values are:
           0 -> H2O selects parallelism level based on cluster configuration, such as number of cores
           1 -> Sequential model building, no parallelism
           n>1 -> n models will be built in parallel if possible""",
        H2OTypeConverters.toInt())

    seed = Param(
        Params._dummy(),
        "seed",
        "Used to specify seed to reproduce the model run",
        H2OTypeConverters.toInt())

    ##
    # Getters
    ##
    def getAlgo(self):
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is None:
            return None
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

    def getSeed(self):
        return self.getOrDefault(self.seed)

    def getLabelCol(self):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'getLabelCol' method of a given algorithm instead.", DeprecationWarning)
        return self.getOrDefault(self.algo).getLabelCol()

    def getOffsetCol(self):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'getOffsetCol' method of a given algorithm instead.", DeprecationWarning)
        return self.getOrDefault(self.algo).getOffsetCol()

    def getFoldCol(self):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'getFoldCol' method of a given algorithm instead.", DeprecationWarning)
        return self.getOrDefault(self.algo).getFoldCol()

    def getWeightCol(self):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'getWeightCol' method of a given algorithm instead.", DeprecationWarning)
        return self.getOrDefault(self.algo).getWeightCol()

    def getSplitRatio(self):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'getSplitRatio' method of a given algorithm instead.", DeprecationWarning)
        return self.getOrDefault(self.algo).getSplitRatio()

    def getNfolds(self):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'getNfolds' method of a given algorithm instead.", DeprecationWarning)
        return self.getOrDefault(self.algo).getNfolds()

    def getAllStringColumnsToCategorical(self):
        warnings.warn("The 'getAllStringColumnsToCategorical' method has been deprecated without replacement."
                      "The method will be removed in the version 3.32.", DeprecationWarning)
        return False

    def getColumnsToCategorical(self):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'getColumnsToCategorical' method of a given algorithm instead.", DeprecationWarning)
        return H2OTypeConverters.toNullableListString()(self.getOrDefault(self.algo).getColumnsToCategorical())

    def getPredictionCol(self):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'getPredictionCol' method of a given algorithm instead.", DeprecationWarning)
        return self.getOrDefault(self.algo).getPredictionCol()

    def getDetailedPredictionCol(self):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'getDetailedPredictionCol' method of a given algorithm instead.", DeprecationWarning)
        return self.getOrDefault(self.algo).getDetailedPredictionCol()

    def getWithDetailedPredictionCol(self):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'getWithDetailedPredictionCol' method of a given algorithm instead.", DeprecationWarning)
        return self.getOrDefault(self.algo).getWithDetailedPredictionCol()

    def getFeaturesCols(self):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'getFeaturesCols' method of a given algorithm instead.", DeprecationWarning)
        return H2OTypeConverters.toNullableListString()(self.getOrDefault(self.algo).getFeaturesCols())

    def getConvertUnknownCategoricalLevelsToNa(self):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'getConvertUnknownCategoricalLevelsToNa' method of a given algorithm instead.", DeprecationWarning)
        return self.getOrDefault(self.algo).getConvertUnknownCategoricalLevelsToNa()

    def getConvertInvalidNumbersToNa(self):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'getConvertInvalidNumbersToNa' method of a given algorithm instead.", DeprecationWarning)
        return self.getOrDefault(self.algo).getConvertInvalidNumbersToNa()

    def getNamedMojoOutputColumns(self):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'getNamedMojoOutputColumns' method of a given algorithm instead.", DeprecationWarning)
        return self.getOrDefault(self.algo).getNamedMojoOutputColumns()

    ##
    # Setters
    ##
    propagateValuesToAlgorithm = {}

    def setAlgo(self, value):
        value._set(**self.propagateValuesToAlgorithm)
        result = self._set(algo=value)
        value._transfer_params_to_java()
        self._transfer_params_to_java()
        return result

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

    def setSeed(self, value):
        return self._set(seed=value)

    def setLabelCol(self, value):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'setLabelCol' method of a given algorithm instead.", DeprecationWarning)
        self.propagateValuesToAlgorithm["labelCol"] = value
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is not None:
            javaAlgo.setLabelCol(value)
        return self

    def setOffsetCol(self, value):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'setOffsetCol' method of a given algorithm instead.", DeprecationWarning)
        self.propagateValuesToAlgorithm["offsetCol"] = value
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is not None:
            javaAlgo.setOffsetCol(value)
        return self

    def setFoldCol(self, value):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'setFoldCol' method of a given algorithm instead.", DeprecationWarning)
        self.propagateValuesToAlgorithm["foldCol"] = value
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is not None:
            javaAlgo.setFoldCol(value)
        return self

    def setWeightCol(self, value):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'setWeightCol' method of a given algorithm instead.", DeprecationWarning)
        self.propagateValuesToAlgorithm["weightCol"] = value
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is not None:
            javaAlgo.setWeightCol(value)
        return self

    def setSplitRatio(self, value):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'setSplitRatio' method of a given algorithm instead.", DeprecationWarning)
        self.propagateValuesToAlgorithm["splitRatio"] = value
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is not None:
            javaAlgo.setSplitRatio(value)
        return self

    def setNfolds(self, value):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'setNfolds' method of a given algorithm instead.", DeprecationWarning)
        self.propagateValuesToAlgorithm["nfolds"] = value
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is not None:
            javaAlgo.setNfolds(value)
        return self

    def setAllStringColumnsToCategorical(self, value):
        warnings.warn("The 'setAllStringColumnsToCategorical' method has been deprecated without replacement."
                      "The method will be removed in the version 3.32.", DeprecationWarning)
        return self

    def setColumnsToCategorical(self, value, *args):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'setColumnsToCategorical' method of a given algorithm instead.", DeprecationWarning)
        finalValue = [value, ] + list(args)
        self.propagateValuesToAlgorithm["columnsToCategorical"] = finalValue
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is not None:
            javaAlgo.setColumnsToCategorical(finalValue)
        return self

    def setPredictionCol(self, value):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'setPredictionCol' method of a given algorithm instead.", DeprecationWarning)
        self.propagateValuesToAlgorithm["predictionCol"] = value
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is not None:
            javaAlgo.setPredictionCol(value)
        return self

    def setDetailedPredictionCol(self, value):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'setDetailedPredictionCol' method of a given algorithm instead.", DeprecationWarning)
        self.propagateValuesToAlgorithm["detailedPredictionCol"] = value
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is not None:
            javaAlgo.setDetailedPredictionCol(value)
        return self

    def setWithDetailedPredictionCol(self, value):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'setWithDetailedPredictionCol' method of a given algorithm instead.", DeprecationWarning)
        self.propagateValuesToAlgorithm["withDetailedPredictionCol"] = value
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is not None:
            javaAlgo.setWithDetailedPredictionCol(value)
        return self

    def setFeaturesCols(self, value):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'setFeaturesCols' method of a given algorithm instead.", DeprecationWarning)
        self.propagateValuesToAlgorithm["featuresCols"] = value
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is not None:
            javaAlgo.setFeaturesCols(value)
        return self

    def setConvertUnknownCategoricalLevelsToNa(self, value):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'setConvertUnknownCategoricalLevelsToNa' method of a given algorithm instead.", DeprecationWarning)
        self.propagateValuesToAlgorithm["convertUnknownCategoricalLevelsToNa"] = value
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is not None:
            javaAlgo.setConvertUnknownCategoricalLevelsToNa(value)
        return self

    def setConvertInvalidNumbersToNa(self, value):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'setConvertInvalidNumbersToNa' method of a given algorithm instead.", DeprecationWarning)
        self.propagateValuesToAlgorithm["convertInvalidNumbersToNa"] = value
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is not None:
            javaAlgo.setConvertInvalidNumbersToNa(value)
        return self

    def setNamedMojoOutputColumns(self, value):
        warnings.warn("The method will be removed in the version 3.32. "
                      "Use 'setNamedMojoOutputColumns' method of a given algorithm instead.", DeprecationWarning)
        self.propagateValuesToAlgorithm["namedMojoOutputColumns"] = value
        javaAlgo = self._java_obj.getAlgo()
        if javaAlgo is not None:
            javaAlgo.setNamedMojoOutputColumns(value)
        return self
