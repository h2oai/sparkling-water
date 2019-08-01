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

from h2o.utils.typechecks import assert_is_type
from pyspark.ml.param import *

from ai.h2o.sparkling.ml.params.H2OSharedTreeParams import H2OSharedTreeParams


class H2OGBMParams(H2OSharedTreeParams):

    ##
    # Param definitions
    ##
    learnRate = Param(Params._dummy(), "learnRate", "Learning rate (from 0.0 to 1.0)")
    learnRateAnnealing = Param(Params._dummy(), "learnRateAnnealing", "Scale the learning rate by this factor after each tree (e.g., 0.99 or 0.999)")
    colSampleRate = Param(Params._dummy(), "colSampleRate", "Column sample rate (from 0.0 to 1.0)")
    maxAbsLeafnodePred = Param(Params._dummy(), "maxAbsLeafnodePred", "Maximum absolute value of a leaf node prediction")
    predNoiseBandwidth = Param(Params._dummy(), "predNoiseBandwidth", "Bandwidth (sigma) of Gaussian multiplicative noise ~N(1,sigma) for tree node predictions")

    ##
    # Getters
    ##
    def getLearnRate(self):
        return self.getOrDefault(self.learnRate)

    def getLearnRateAnnealing(self):
        return self.getOrDefault(self.learnRateAnnealing)

    def getColSampleRate(self):
        return self.getOrDefault(self.colSampleRate)

    def getMaxAbsLeafnodePred(self):
        return self.getOrDefault(self.maxAbsLeafnodePred)

    def getPredNoiseBandwidth(self):
        return self.getOrDefault(self.predNoiseBandwidth)

    ##
    # Setters
    ##
    def setLearnRate(self, value):
        assert_is_type(value, int, float)
        return self._set(learnRate=float(value))

    def setLearnRateAnnealing(self, value):
        assert_is_type(value, int, float)
        return self._set(learnRateAnnealing=float(value))

    def setColSampleRate(self, value):
        assert_is_type(value, int, float)
        return self._set(colSampleRate=float(value))

    def setMaxAbsLeafnodePred(self, value):
        assert_is_type(value, int, float)
        return self._set(maxAbsLeafnodePred=float(value))

    def setPredNoiseBandwidth(self, value):
        assert_is_type(value, int, float)
        return self._set(predNoiseBandwidth=float(value))
