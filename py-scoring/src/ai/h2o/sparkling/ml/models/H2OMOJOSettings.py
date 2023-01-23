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
                 convertUnknownCategoricalLevelsToNa=False,
                 convertInvalidNumbersToNa=False,
                 withContributions=False,
                 withInternalContributions=False,
                 withPredictionInterval=False,
                 withLeafNodeAssignments=False,
                 withStageResults=False,
                 dataFrameSerializer="ai.h2o.sparkling.utils.JSONDataFrameSerializer",
                 scoringBulkSize=1000):
        self._java_obj = None

        assert isinstance(predictionCol, str)
        assert isinstance(detailedPredictionCol, str)
        assert isinstance(convertUnknownCategoricalLevelsToNa, bool)
        assert isinstance(convertInvalidNumbersToNa, bool)
        assert isinstance(withContributions, bool)
        assert isinstance(withInternalContributions, bool)
        assert isinstance(withPredictionInterval, bool)
        assert isinstance(withLeafNodeAssignments, bool)
        assert isinstance(withStageResults, bool)
        assert isinstance(dataFrameSerializer, str)
        assert isinstance(scoringBulkSize, int)
        self.predictionCol = predictionCol
        self.detailedPredictionCol = detailedPredictionCol
        self.convertUnknownCategoricalLevelsToNa = convertUnknownCategoricalLevelsToNa
        self.convertInvalidNumbersToNa = convertInvalidNumbersToNa
        self.withContributions = withContributions
        self.withInternalContributions = withInternalContributions
        self.withPredictionInterval = withPredictionInterval
        self.withLeafNodeAssignments = withLeafNodeAssignments
        self.withStageResults = withStageResults
        self.dataFrameSerializer = dataFrameSerializer
        self.scoringBulkSize = scoringBulkSize

    def toJavaObject(self):
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.models.H2OMOJOSettings",
                                            self.predictionCol,
                                            self.detailedPredictionCol,
                                            self.convertUnknownCategoricalLevelsToNa,
                                            self.convertInvalidNumbersToNa,
                                            self.withContributions,
                                            self.withInternalContributions,
                                            self.withPredictionInterval,
                                            self.withLeafNodeAssignments,
                                            self.withStageResults,
                                            self.dataFrameSerializer,
                                            self.scoringBulkSize)
        return self._java_obj

    @staticmethod
    def default():
        return H2OMOJOSettings()
