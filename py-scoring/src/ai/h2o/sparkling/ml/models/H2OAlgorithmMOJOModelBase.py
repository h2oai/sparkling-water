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
from ai.h2o.sparkling.ml.models.H2OMOJOModelBase import H2OMOJOModelBase

import warnings


class H2OAlgorithmMOJOModelBase(H2OMOJOModelBase):

    ##
    # Getters
    ##
    def getPredictionCol(self):
        return self._java_obj.getPredictionCol()

    def getDetailedPredictionCol(self):
        return self._java_obj.getDetailedPredictionCol()

    def getFeaturesCols(self):
        return list(self._java_obj.getFeaturesCols())

    def getWithContributions(self):
        return self._java_obj.getWithContributions()

    def getWithLeafNodeAssignments(self):
        return self._java_obj.getWithLeafNodeAssignments()

    def getWithStageResults(self):
        return self._java_obj.getWithStageResults()
