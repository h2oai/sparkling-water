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

from ai.h2o.sparkling.ml.utils import getValidatedEnumValue


class H2OAlgoCommonParams:
    ##
    # Param definitions
    ##
    modelId = Param(Params._dummy(), "modelId", "An unique identifier of a trained model. If the id already exists, a number will be appended to ensure uniqueness.")
    keepCrossValidationPredictions = Param(Params._dummy(), "keepCrossValidationPredictions", "Whether to keep the predictions of the cross-validation models")
    keepCrossValidationFoldAssignment = Param(Params._dummy(), "keepCrossValidationFoldAssignment", "Whether to keep the cross-validation fold assignment")
    parallelizeCrossValidation = Param(Params._dummy(), "parallelizeCrossValidation", "Allow parallel training of cross-validation models")
    distribution = Param(Params._dummy(), "distribution", "Distribution function")

    ##
    # Getters
    ##
    def getModelId(self):
        return self.getOrDefault(self.modelId)

    def getKeepCrossValidationPredictions(self):
        return self.getOrDefault(self.keepCrossValidationPredictions)

    def getKeepCrossValidationFoldAssignment(self):
        return self.getOrDefault(self.keepCrossValidationFoldAssignment)

    def getParallelizeCrossValidation(self):
        return self.getOrDefault(self.parallelizeCrossValidation)

    def getDistribution(self):
        return self.getOrDefault(self.distribution)

    ##
    # Setters
    ##
    def setModelId(self, value):
        assert_is_type(value, None, str)
        return self._set(modelId=value)

    def setKeepCrossValidationPredictions(self, value):
        assert_is_type(value, bool)
        return self._set(keepCrossValidationPredictions=value)

    def setKeepCrossValidationFoldAssignment(self, value):
        assert_is_type(value, bool)
        return self._set(keepCrossValidationFoldAssignment=value)

    def setParallelizeCrossValidation(self, value):
        assert_is_type(value, bool)
        return self._set(parallelizeCrossValidation=value)

    def setDistribution(self, value):
        validated = getValidatedEnumValue(self.__getDistributionEnum(), value)
        return self._set(distribution=validated)

    def __getDistributionEnum(self):
        return "hex.genmodel.utils.DistributionFamily"
