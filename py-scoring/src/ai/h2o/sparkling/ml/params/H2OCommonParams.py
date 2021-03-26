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

from ai.h2o.sparkling.ml.params.H2OBaseMOJOParams import H2OBaseMOJOParams
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from h2o.utils.typechecks import assert_is_type
from pyspark.ml.param import *
import warnings


class H2OCommonParams(H2OBaseMOJOParams):
    ##
    # Param definitions
    ##
    validationDataFrame = Param(
        Params._dummy(),
        "validationDataFrame",
        "A data frame dedicated for a validation of the trained model. If the parameters is not set," +
        "a validation frame created via the 'splitRatio' parameter.",
        H2OTypeConverters.toNullableDataFrame())

    splitRatio = Param(
        Params._dummy(),
        "splitRatio",
        "Accepts values in range [0, 1.0] which determine how large part of dataset is used for training"
        " and for validation. For example, 0.8 -> 80% training 20% validation.",
        H2OTypeConverters.toFloat())

    columnsToCategorical = Param(
        Params._dummy(),
        "columnsToCategorical",
        "List of columns to convert to categorical before modelling",
        H2OTypeConverters.toListString())

    ##
    # Getters
    ##
    def getValidationDataFrame(self):
        return self.getOrDefault(self.validationDataFrame)

    def getSplitRatio(self):
        return self.getOrDefault(self.splitRatio)

    def getColumnsToCategorical(self):
        return self.getOrDefault(self.columnsToCategorical)

    ##
    # Setters
    ##
    def setValidationDataFrame(self, value):
        return self._set(validationDataFrame=value)

    def setSplitRatio(self, value):
        return self._set(splitRatio=value)

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
        warnings.warn("The method will be removed without a replacement in the version 3.36."
                      "Detailed prediction columns is enabled by default.", DeprecationWarning)
        return self

    def setFeaturesCols(self, value):
        return self._set(featuresCols=value)

    def setConvertUnknownCategoricalLevelsToNa(self, value):
        return self._set(convertUnknownCategoricalLevelsToNa=value)

    def setConvertInvalidNumbersToNa(self, value):
        return self._set(convertInvalidNumbersToNa=value)

    def setNamedMojoOutputColumns(self, value):
        return self._set(namedMojoOutputColumns=value)

    def setWithContributions(self, value):
        return self._set(withContributions=value)

    def setWithLeafNodeAssignments(self, value):
        return self._set(withLeafNodeAssignments=value)

    def setWithStageResults(self, value):
        return self._set(withStageResults=value)
