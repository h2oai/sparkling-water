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
from py4j.java_gateway import JavaObject
from pyspark.ml.param import *

from ai.h2o.sparkling.ml.params.H2OCommonSupervisedParams import H2OCommonSupervisedParams
from ai.h2o.sparkling.ml.utils import getValidatedEnumValue


class H2OGridSearchParams(H2OCommonSupervisedParams):

    ##
    # Param definitions
    ##
    algo = Param(Params._dummy(), "algo", "Algo to run grid search on")
    hyperParameters = Param(Params._dummy(), "hyperParameters", "Grid Search Hyper Params map")
    strategy = Param(Params._dummy(), "strategy", "strategy")
    maxRuntimeSecs = Param(Params._dummy(), "maxRuntimeSecs", "maxRuntimeSecs")
    maxModels = Param(Params._dummy(), "maxModels", "maxModels")
    stoppingRounds = Param(Params._dummy(), "stoppingRounds", "stoppingRounds")
    stoppingTolerance = Param(Params._dummy(), "stoppingTolerance", "stoppingTolerance")
    stoppingMetric = Param(Params._dummy(), "stoppingMetric", "stoppingMetric")
    selectBestModelBy = Param(Params._dummy(), "selectBestModelBy", "selectBestModelBy")
    selectBestModelDecreasing = Param(Params._dummy(), "selectBestModelDecreasing", "selectBestModelDecreasing")

    ##
    # Getters
    ##
    def getAlgoParams(self):
        return self._java_obj.getAlgoParams()

    def getHyperParameters(self):
        params = self.getOrDefault(self.hyperParameters)
        if isinstance(params, JavaObject):
            keys = [k for k in params.keySet().toArray()]
            map = {}
            for k in keys:
                map[k] = [v for v in params.get(k)]
            return map
        else:
            return params

    def getStrategy(self):
        return self.getOrDefault(self.strategy)

    def getMaxRuntimeSecs(self):
        return self.getOrDefault(self.maxRuntimeSecs)

    def getMaxModels(self):
        return self.getOrDefault(self.maxModels)

    def getStoppingRounds(self):
        return self.getOrDefault(self.stoppingRounds)

    def getStoppingTolerance(self):
        return self.getOrDefault(self.stoppingTolerance)

    def getStoppingMetric(self):
        return self.getOrDefault(self.stoppingMetric)

    def getSelectBestModelBy(self):
        return self.getOrDefault(self.selectBestModelBy)

    def getSelectBestModelDecreasing(self):
        return self.getOrDefault(self.selectBestModelDecreasing)

    ##
    # Setters
    ##
    def setAlgo(self, value):
        assert_is_type(value, object)
        self._java_obj.setAlgo(value._java_obj)
        return self

    def setHyperParameters(self, value):
        assert_is_type(value, None, {str : [object]})
        return self._set(hyperParameters=value)

    def setStrategy(self, value):
        validated = getValidatedEnumValue(self.__getStrategyEnum(), value)
        return self._set(link=validated)

    def __getStrategyEnum(self):
        return "hex.grid.HyperSpaceSearchCriteria$Strategy"

    def setMaxRuntimeSecs(self, value):
        assert_is_type(value, int, float)
        return self._set(maxRuntimeSecs=float(value))

    def setMaxModels(self, value):
        assert_is_type(value, int)
        return self._set(maxModels=value)

    def setStoppingRounds(self, value):
        assert_is_type(value, int)
        return self._set(stoppingRounds=value)

    def setStoppingTolerance(self, value):
        assert_is_type(value, int, float)
        return self._set(stoppingTolerance=float(value))

    def setStoppingMetric(self, value):
        validated = getValidatedEnumValue(self.__getStoppingMetricEnum(), value)
        return self._set(stoppingMetric=validated)

    def __getStoppingMetricEnum(self):
        return "hex.ScoreKeeper$StoppingMetric"

    def setSelectBestModelBy(self, value):
        validated = getValidatedEnumValue(self.__getSelectBestModelByEnum(), value)
        return self._set(selectBestModelBy=validated)

    def __getSelectBestModelByEnum(self):
        return "org.apache.spark.ml.h2o.algos.H2OGridSearchMetric"

    def setSelectBestModelDecreasing(self, value):
        assert_is_type(value, bool)
        return self._set(selectBestModelDecreasing=value)
