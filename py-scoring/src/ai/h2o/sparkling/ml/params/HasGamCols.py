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


class HasGamCols(Params):
    gamCols = Param(
        Params._dummy(),
        "gamCols",
        "Arrays of predictor column names for gam for smoothers using single or multiple predictors "
        "like {{'c1'},{'c2','c3'},{'c4'},...}",
        H2OTypeConverters.toNullableListListString())

    def getGamCols(self):
        return self.getOrDefault(self.gamCols)

    def setGamCols(self, value):
        return self._set(gamCols=self._convertGamCols(value))

    def _toStringArray(self, value):
        if isinstance(value, list):
            return value
        else:
            return [str(value)]

    def _convertGamCols(self, value):
        if isinstance(value, list):
            return [self._toStringArray(item) for item in value]
        else:
            return value

    def _updateInitKwargs(self, kwargs):
        if 'gamCols' in kwargs:
            kwargs['gamCols'] = self._convertGamCols(kwargs['gamCols'])
        return kwargs
