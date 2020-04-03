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

import warnings

from h2o.utils.typechecks import assert_is_type
from pyspark.ml.param import *

from ai.h2o.sparkling.ml.params.H2OMOJOAlgoSharedParams import H2OMOJOAlgoSharedParams
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters


class H2OCommonParams(H2OMOJOAlgoSharedParams):
    foldCol = Param(
        Params._dummy(),
        "foldCol",
        "Fold column name",
        H2OTypeConverters.toNullableString())

    weightCol = Param(
        Params._dummy(),
        "weightCol",
        "Weight column name",
        H2OTypeConverters.toNullableString())

    splitRatio = Param(
        Params._dummy(),
        "splitRatio",
        "Accepts values in range [0, 1.0] which determine how large part of dataset is used for training"
        " and for validation. For example, 0.8 -> 80% training 20% validation.",
        H2OTypeConverters.toFloat())

    seed = Param(
        Params._dummy(),
        "seed",
        "Used to specify seed to reproduce the model run",
        H2OTypeConverters.toInt())

    nfolds = Param(
        Params._dummy(),
        "nfolds",
        "Number of fold columns",
        H2OTypeConverters.toInt())

    columnsToCategorical = Param(
        Params._dummy(),
        "columnsToCategorical",
        "List of columns to convert to categorical before modelling",
        H2OTypeConverters.toListString())

    ##
    # Getters
    ##
    def getFoldCol(self):
        return self.getOrDefault(self.foldCol)

    def getWeightCol(self):
        return self.getOrDefault(self.weightCol)

    def getSplitRatio(self):
        return self.getOrDefault(self.splitRatio)

    def getSeed(self):
        return self.getOrDefault(self.seed)

    def getNfolds(self):
        return self.getOrDefault(self.nfolds)

    def getAllStringColumnsToCategorical(self):
        warnings.warn("The 'getAllStringColumnsToCategorical' method has been deprecated without replacement."
                      "The method will be removed in the version 3.32.", DeprecationWarning)
        return False

    def getColumnsToCategorical(self):
        return self.getOrDefault(self.columnsToCategorical)

    ##
    # Setters
    ##
    def setFoldCol(self, value):
        return self._set(foldCol=value)

    def setWeightCol(self, value):
        return self._set(weightCol=value)

    def setSplitRatio(self, value):
        return self._set(splitRatio=value)

    def setSeed(self, value):
        return self._set(seed=value)

    def setNfolds(self, value):
        return self._set(nfolds=value)

    def setAllStringColumnsToCategorical(self, value):
        warnings.warn("The 'setAllStringColumnsToCategorical' method has been deprecated without replacement."
                      "The method will be removed in the version 3.32.", DeprecationWarning)
        return self

    def setColumnsToCategorical(self, value, *args):
        assert_is_type(value, [str], str)

        if isinstance(value, str):
            prepared_array = [value]
        else:
            prepared_array = value

        for arg in args:
            prepared_array.append(arg)

        return self._set(columnsToCategorical=value)

    # Setters for parameters which are defined on MOJO as well
    def setPredictionCol(self, value):
        return self._set(predictionCol=value)

    def setDetailedPredictionCol(self, value):
        return self._set(detailedPredictionCol=value)

    def setWithDetailedPredictionCol(self, value):
        return self._set(withDetailedPredictionCol=value)

    def setFeaturesCols(self, value):
        return self._set(featuresCols=value)

    def setConvertUnknownCategoricalLevelsToNa(self, value):
        return self._set(convertUnknownCategoricalLevelsToNa=value)

    def setConvertInvalidNumbersToNa(self, value):
        return self._set(convertInvalidNumbersToNa=value)

    def setNamedMojoOutputColumns(self, value):
        return self._set(namedMojoOutputColumns=value)
