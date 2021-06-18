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

from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class H2OBaseMOJOParams(Params):
    convertUnknownCategoricalLevelsToNa = Param(
        Params._dummy(),
        "convertUnknownCategoricalLevelsToNa",
        "If set to 'true', the model converts unknown categorical levels to NA during making predictions.",
        H2OTypeConverters.toBoolean())

    convertInvalidNumbersToNa = Param(
        Params._dummy(),
        "convertInvalidNumbersToNa",
        "If set to 'true', the model converts invalid numbers to NA during making predictions.",
        H2OTypeConverters.toBoolean())

    ##
    # Getters
    ##
    def getConvertUnknownCategoricalLevelsToNa(self):
        return self.getOrDefault(self.convertUnknownCategoricalLevelsToNa)

    def getConvertInvalidNumbersToNa(self):
        return self.getOrDefault(self.convertInvalidNumbersToNa)
