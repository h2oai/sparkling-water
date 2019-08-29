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

from ai.h2o.sparkling.ml.params.H2OSharedTreeParams import H2OSharedTreeParams
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class H2ODRFParams(H2OSharedTreeParams):
    ##
    # Param definitions
    ##
    binomialDoubleTrees = Param(
        Params._dummy(),
        "binomialDoubleTrees",
        "In case of binary classification, build 2 times more trees (one per class) - can lead "
        "to higher accuracy.",
        H2OTypeConverters.toBoolean())

    mtries = Param(
        Params._dummy(),
        "mtries",
        "Number of variables randomly sampled as candidates at each split. If set to -1, defaults "
        "to sqrt{p} for classification and p/3 for regression (where p is the # of predictors",
        H2OTypeConverters.toInt())

    ##
    # Getters
    ##
    def getBinomialDoubleTrees(self):
        return self.getOrDefault(self.binomialDoubleTrees)

    def getMtries(self):
        return self.getOrDefault(self.mtries)

    ##
    # Setters
    ##
    def setBinomialDoubleTrees(self, value):
        return self._set(binomialDoubleTrees=value)

    def setMtries(self, value):
        return self._set(mtries=value)
