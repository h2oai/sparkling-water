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
from pyspark.ml.wrapper import JavaWrapper


class H2OMOJOSettings(JavaWrapper):

    def __init__(self,
                 predictionCol="prediction",
                 detailedPredictionCol="detailed_prediction",
                 withDetailedPredictionCol=True,
                 convertUnknownCategoricalLevelsToNa=False,
                 convertInvalidNumbersToNa=False,
                 namedMojoOutputColumns=True,
                 withContributions=False,
                 withLeafNodeAssignments=False,
                 withStageResults=False):
        self._java_obj = None

        assert isinstance(predictionCol, str)
        assert isinstance(detailedPredictionCol, str)
        assert isinstance(convertUnknownCategoricalLevelsToNa, bool)
        assert isinstance(convertInvalidNumbersToNa, bool)
        assert isinstance(namedMojoOutputColumns, bool)
        assert isinstance(withContributions, bool)
        assert isinstance(withLeafNodeAssignments, bool)
        assert isinstance(withStageResults, bool)
        self.predictionCol = predictionCol
        self.detailedPredictionCol = detailedPredictionCol
        self.convertUnknownCategoricalLevelsToNa = convertUnknownCategoricalLevelsToNa
        self.convertInvalidNumbersToNa = convertInvalidNumbersToNa
        self.namedMojoOutputColumns = namedMojoOutputColumns
        self.withContributions = withContributions
        self.withLeafNodeAssignments = withLeafNodeAssignments
        self.withStageResults = withStageResults

    def toJavaObject(self):
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.models.H2OMOJOSettings",
                                            self.predictionCol,
                                            self.detailedPredictionCol,
                                            True,
                                            self.convertUnknownCategoricalLevelsToNa,
                                            self.convertInvalidNumbersToNa,
                                            self.namedMojoOutputColumns,
                                            self.withContributions,
                                            self.withLeafNodeAssignments,
                                            self.withStageResults)
        return self._java_obj

    @staticmethod
    def default():
        return H2OMOJOSettings()
