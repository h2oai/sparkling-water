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

from ai.h2o.sparkling.ml.params.H2OAlgoUnsupervisedParams import H2OAlgoUnsupervisedParams
from ai.h2o.sparkling.ml.utils import getValidatedEnumValue, getDoubleArrayArrayFromIntArrayArray


class H2OKMeansParams(H2OAlgoUnsupervisedParams):
    maxIterations = Param(Params._dummy(), "maxIterations",
                          "Maximum number of KMeans iterations to find the centroids.")
    standardize = Param(Params._dummy(), "standardize",
                        "Standardize the numeric columns to have a mean of zero and unit variance.")
    init = Param(Params._dummy(), "init", "Initialization mode for finding the initial cluster centers.")
    userPoints = Param(Params._dummy(), "userPoints", "This option enables" +
                       " to specify array of points, where each point represents coordinates of an initial cluster center. The user-specified" +
                       " points must have the same number of columns as the training observations. The number of rows must equal" +
                       " the number of clusters.")
    estimateK = Param(Params._dummy(), "estimateK",
                      "If enabled, the algorithm tries to identify optimal number of clusters, up to k clusters.")
    k = Param(Params._dummy(), "k", "Number of clusters to generate.")

    #
    # Getters
    #
    def getMaxIterations(self):
        return self.getOrDefault(self.maxIterations)

    def getStandardize(self):
        return self.getOrDefault(self.standardize)

    def getInit(self):
        return self.getOrDefault(self.init)

    def getUserPoints(self):
        return self.getOrDefault(self.userPoints)

    def getEstimateK(self):
        return self.getOrDefault(self.estimateK)

    def getK(self):
        return self.getOrDefault(self.k)

    #
    # Setters
    #
    def setMaxIterations(self, value):
        assert_is_type(value, int)
        return self._set(maxIterations=value)

    def setStandardize(self, value):
        assert_is_type(value, bool)
        return self._set(standardize=value)

    def setInit(self, value):
        validated = getValidatedEnumValue(self.__getInitEnum(), value)
        return self._set(init=validated)

    def setUserPoints(self, value):
        assert_is_type(value, [[int, float]])
        return self._set(userPoints=getDoubleArrayArrayFromIntArrayArray(value))

    def setEstimateK(self, value):
        assert_is_type(value, bool)
        return self._set(estimateK=value)

    def setK(self, value):
        assert_is_type(value, int)
        return self._set(k=value)

    def __getInitEnum(self):
        return "hex.kmeans.KMeans$Initialization"
