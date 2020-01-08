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


class HasStoppingCriteria(Params):
    stoppingRounds = Param(
        Params._dummy(),
        "stoppingRounds",
        "Stopping Rounds",
        H2OTypeConverters.toInt())

    stoppingMetric = Param(
        Params._dummy(),
        "stoppingMetric",
        "Stopping Metric",
        H2OTypeConverters.toEnumString("hex.ScoreKeeper$StoppingMetric"))

    stoppingTolerance = Param(
        Params._dummy(),
        "stoppingTolerance",
        "Stopping Tolerance",
        H2OTypeConverters.toFloat())

    def getStoppingRounds(self):
        return self.getOrDefault(self.stoppingRounds)

    def getStoppingMetric(self):
        return self.getOrDefault(self.stoppingMetric)

    def getStoppingTolerance(self):
        return self.getOrDefault(self.stoppingTolerance)

    def setStoppingRounds(self, value):
        return self._set(stoppingRounds=value)

    def setStoppingMetric(self, value):
        return self._set(stoppingMetric=value)

    def setStoppingTolerance(self, value):
        return self._set(stoppingTolerance=value)
