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

from ai.h2o.sparkling.ml.models.H2OMOJOModelBase import H2OMOJOModelBase
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class H2OMOJOModelParams(H2OMOJOModelBase):

    def getModelDetails(self):
        return self._java_obj.getModelDetails()

    def getDomainValues(self):
        return H2OTypeConverters.scalaMapStringDictStringToStringDictString(self._java_obj.getDomainValues())

    def getTrainingMetrics(self):
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getTrainingMetrics())

    def getValidationMetrics(self):
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getValidationMetrics())

    def getCrossValidationMetrics(self):
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getCrossValidationMetrics())

    def getCurrentMetrics(self):
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getCurrentMetrics())

    def getTrainingParams(self):
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getTrainingParams())

    def getModelCategory(self):
        return self._java_obj.getModelCategory()

    def getScoringHistory(self):
        return H2OTypeConverters.scalaToPythonDataFrame(self._java_obj.getScoringHistory())

    def getFeatureImportances(self):
        return H2OTypeConverters.scalaToPythonDataFrame(self._java_obj.getFeatureImportances())


class HasOffsetCol:

    def getOffsetCol(self):
        return self._java_obj.getOffsetCol()


class HasNtrees:

    def getNtrees(self):
        return self._java_obj.getNtrees()


class H2OUnsupervisedMOJOModelParams(H2OMOJOModelParams):
    pass


class H2OSupervisedMOJOModelParams(H2OMOJOModelParams, HasOffsetCol):
    pass


class H2OTreeBasedUnsupervisedMOJOModelParams(H2OUnsupervisedMOJOModelParams, HasNtrees):
    pass


class H2OTreeBasedSupervisedMOJOModelParams(H2OSupervisedMOJOModelParams, HasNtrees):
    pass
