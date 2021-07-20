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
from ai.h2o.sparkling.ml.params.H2ODimReductionExtraParamsOnMOJO import H2ODimReductionExtraParamsOnMOJO


class H2OGLRMExtraParamsOnMOJO(H2ODimReductionExtraParamsOnMOJO):
    def getReconstructedCol(self):
        return self._java_obj.getReconstructedCol()

    def getWithReconstructedlCol(self):
        return self._java_obj.getWithReconstructedCol()

    def getMaxScoringIterations(self):
        return self._java_obj.getMaxScoringIterations()

    def setReconstructedCol(self, value):
        self._java_obj.setReconstructedCol(value)
        return self

    def setWithReconstructedCol(self, value):
        self._java_obj.setWithReconstructedCol(value)
        return self

    def setMaxScoringIterations(self, value):
        self._java_obj.setMaxScoringIterations(value)
        return self
