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


class H2OBaseMOJOParams(Params):
    predictionCol = Param(
        Params._dummy(),
        "predictionCol",
        "Prediction column name",
        H2OTypeConverters.toString())

    detailedPredictionCol = Param(
        Params._dummy(),
        "detailedPredictionCol",
        "Column containing additional prediction details, its content depends on the model type.",
        H2OTypeConverters.toString())

    withDetailedPredictionCol = Param(
        Params._dummy(),
        "withDetailedPredictionCol",
        "Enables or disables generating additional prediction column, but with more details",
        H2OTypeConverters.toBoolean())

    withContributions = Param(
        Params._dummy(),
        "withContributions",
        "Enables or disables generating a sub-column of detailedPredictionCol containing Shapley values.",
        H2OTypeConverters.toBoolean())

    featuresCols = Param(
        Params._dummy(),
        "featuresCols",
        "Name of feature columns",
        H2OTypeConverters.toListString())

    convertUnknownCategoricalLevelsToNa = Param(
        Params._dummy(),
        "convertUnknownCategoricalLevelsToNa",
        "If set to 'true', the model converts unknown categorical levels to NA during making predictions.",
        H2OTypeConverters.toBoolean())

    convertInvalidNumbersToNa = Param(
        Params._dummy(),
        "convertInvalidNumbersToNa",
        "If set to 'true', the model converts invalid numbers to NA during making predictions.",
        H2OTypeConverters.toBoolean())

    namedMojoOutputColumns = Param(
        Params._dummy(),
        "namedMojoOutputColumns",
        "Mojo Output is not stored in the array but in the properly named columns",
        H2OTypeConverters.toBoolean())

    withLeafNodeAssignments = Param(
        Params._dummy(),
        "withLeafNodeAssignments",
        "Enables or disables computation of leaf node assignments.",
        H2OTypeConverters.toBoolean())

    withStageResults = Param(
        Params._dummy(),
        "withStageResults",
        "Enables or disables computation of stage results.",
        H2OTypeConverters.toBoolean())

    ##
    # Getters
    ##
    def getPredictionCol(self):
        return self.getOrDefault(self.predictionCol)

    def getDetailedPredictionCol(self):
        return self.getOrDefault(self.detailedPredictionCol)

    def getWithDetailedPredictionCol(self):
        warnings.warn("The method will be removed without a replacement in the version 3.36."
                      "Detailed prediction columns is always enabled.", DeprecationWarning)
        return True

    def getWithContributions(self):
        return self.getOrDefault(self.withContributions)

    def getFeaturesCols(self):
        return self.getOrDefault(self.featuresCols)

    def getConvertUnknownCategoricalLevelsToNa(self):
        return self.getOrDefault(self.convertUnknownCategoricalLevelsToNa)

    def getConvertInvalidNumbersToNa(self):
        return self.getOrDefault(self.convertInvalidNumbersToNa)

    def getNamedMojoOutputColumns(self):
        return self.getOrDefault(self.namedMojoOutputColumns)

    def getWithLeafNodeAssignments(self):
        return self.getOrDefault(self.withLeafNodeAssignments)

    def getWithStageResults(self):
        return self.getOrDefault(self.withStageResults)
