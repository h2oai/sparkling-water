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
from ai.h2o.sparkling.ml.params.H2ODimReductionExtraParams import H2ODimReductionExtraParams


class H2OGLRMExtraParams(H2ODimReductionExtraParams):
    reconstructedCol = Param(
        Params._dummy(),
        "reconstructedCol",
        "Reconstructed column name. This column contains reconstructed input values (A_hat=X*Y instead of just X).",
        H2OTypeConverters.toNullableString())

    withReconstructedCol = Param(
        Params._dummy(),
        "withReconstructedCol",
        "A flag identifying whether a column with reconstructed input values will be produced or not.",
        H2OTypeConverters.toBoolean())

    maxScoringIterations = Param(
        Params._dummy(),
        "maxScoringIterations",
        "The maximum number of iterations used in MOJO scoring to update X.",
        H2OTypeConverters.toInt())

    def getReconstructedCol(self):
        return self.getOrDefault(self.reconstructedCol)

    def getWithReconstructedCol(self):
        return self.getOrDefault(self.withReconstructedCol)

    def getMaxScoringIterations(self):
        return self.getOrDefault(self.maxScoringIterations)

    def setReconstructedCol(self, value):
        return self._set(reconstructedCol=value)

    def setWithReconstructedCol(self, value):
        return self._set(withReconstructedCol=value)

    def setMaxScoringIterations(self, value):
        return self._set(maxScoringIterations=value)
