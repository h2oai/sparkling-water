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


class HasPreProcessing(Params):
    preProcessing = Param(
        Params._dummy(),
        "preProcessing",
        "The list of pre-processing steps to run. Only 'TargetEncoding' is currently supported.",
        H2OTypeConverters.toNullableListEnumString("ai.h2o.automl.preprocessing.PreprocessingStepDefinition$Type"))

    def getPreProcessing(self):
        return self.getOrDefault(self.preProcessing)

    def setPreProcessing(self, value):
        return self._set(preProcessing=value)
