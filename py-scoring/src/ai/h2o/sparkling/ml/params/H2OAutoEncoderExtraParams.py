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
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from ai.h2o.sparkling.ml.params.HasInputCols import HasInputCols
from ai.h2o.sparkling.ml.params.HasOutputCol import HasOutputCol


class H2OAutoEncoderExtraParams(HasInputCols, HasOutputCol):
    originalCol = Param(
        Params._dummy(),
        "originalCol",
        "Original column name. This column contains input values to the neural network of auto encoder.",
        H2OTypeConverters.toNullableString())

    withOriginalCol = Param(
        Params._dummy(),
        "withOriginalCol",
        "A flag identifying whether a column with input values to the neural network will be produced or not.",
        H2OTypeConverters.toBoolean())

    mseCol = Param(
        Params._dummy(),
        "mseCol",
        "MSE column name. This column contains mean square error calculated from original and output values.",
        H2OTypeConverters.toNullableString())

    withMSECol = Param(
        Params._dummy(),
        "withMSECol",
        "A flag identifying whether a column with mean square error will be produced or not.",
        H2OTypeConverters.toBoolean())

    def getOriginalCol(self):
        return self.getOrDefault(self.originalCol)

    def getWithOriginalCol(self):
        return self.getOrDefault(self.withOriginalCol)

    def getMSECol(self):
        return self.getOrDefault(self.mseCol)

    def getWithMSECol(self):
        return self.getOrDefault(self.withMSECol)

    def setOriginalCol(self, value):
        return self._set(originalCol=value)

    def setWithOriginalCol(self, value):
        return self._set(withOriginalCol=value)

    def setMSECol(self, value):
        return self._set(mseCol=value)

    def setWithMSECol(self, value):
        return self._set(withMSECol=value)
