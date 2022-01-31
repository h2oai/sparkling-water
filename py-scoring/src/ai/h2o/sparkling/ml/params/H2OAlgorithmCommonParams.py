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

from ai.h2o.sparkling.ml.params.H2OCommonParams import H2OCommonParams
from ai.h2o.sparkling.ml.params.H2OAlgorithmMOJOParams import H2OAlgorithmMOJOParams
from pyspark.ml.param import *
import warnings


class H2OAlgorithmCommonParams(H2OCommonParams, H2OAlgorithmMOJOParams):
    # Setters for parameters which are defined on MOJO as well
    def setPredictionCol(self, value):
        return self._set(predictionCol=value)

    def setDetailedPredictionCol(self, value):
        return self._set(detailedPredictionCol=value)

    def setFeaturesCols(self, value):
        return self._set(featuresCols=value)

    def setNamedMojoOutputColumns(self, value):
        warnings.warn("The method will be removed without replacement in the version 3.40."
                      "Named output columns will be always used.", DeprecationWarning)
        return self._set(namedMojoOutputColumns=value)

    def setWithContributions(self, value):
        return self._set(withContributions=value)

    def setWithLeafNodeAssignments(self, value):
        return self._set(withLeafNodeAssignments=value)

    def setWithStageResults(self, value):
        return self._set(withStageResults=value)
