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
from ai.h2o.sparkling.ml.models.H2OAlgorithmMOJOModelBase import H2OAlgorithmMOJOModelBase
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from ai.h2o.sparkling.ml.metrics.H2OMetricsFactory import H2OMetricsFactory
from pyspark.ml.param import *


class H2OMOJOModelParams:

    def getModelDetails(self):
        return self._java_obj.getModelDetails()

    def getDomainValues(self):
        return H2OTypeConverters.scalaMapStringDictStringToStringDictString(self._java_obj.getDomainValues())

    def getTrainingMetrics(self):
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getTrainingMetrics())

    def getTrainingMetricsObject(self):
        return H2OMetricsFactory.fromJavaObject(self._java_obj.getTrainingMetricsObject())

    def getValidationMetrics(self):
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getValidationMetrics())

    def getValidationMetricsObject(self):
        return H2OMetricsFactory.fromJavaObject(self._java_obj.getValidationMetricsObject())

    def getCrossValidationMetrics(self):
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getCrossValidationMetrics())

    def getCrossValidationMetricsObject(self):
        return H2OMetricsFactory.fromJavaObject(self._java_obj.getCrossValidationMetricsObject())

    def getCurrentMetrics(self):
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getCurrentMetrics())

    def getCurrentMetricsObject(self):
        return H2OMetricsFactory.fromJavaObject(self._java_obj.getCurrentMetricsObject())

    def getCrossValidationMetricsSummary(self):
        return H2OTypeConverters.scalaToPythonDataFrame(self._java_obj.getCrossValidationMetricsSummary())

    def getTrainingParams(self):
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getTrainingParams())

    def getModelCategory(self):
        return self._java_obj.getModelCategory()

    def getScoringHistory(self):
        return H2OTypeConverters.scalaToPythonDataFrame(self._java_obj.getScoringHistory())

    def getFeatureImportances(self):
        return H2OTypeConverters.scalaToPythonDataFrame(self._java_obj.getFeatureImportances())


class H2OAlgorithmMOJOModelParams(H2OMOJOModelParams, H2OAlgorithmMOJOModelBase):
    pass


class H2OFeatureMOJOModelParams(H2OMOJOModelParams, H2OMOJOModelBase):
    pass


class HasOffsetCol:

    def getOffsetCol(self):
        return self._java_obj.getOffsetCol()


class HasNtrees:

    def getNtrees(self):
        return self._java_obj.getNtrees()


class H2OUnsupervisedMOJOModelParams(H2OAlgorithmMOJOModelParams):
    pass


class H2OSupervisedMOJOModelParams(H2OAlgorithmMOJOModelParams, HasOffsetCol):
    pass


class H2OTreeBasedUnsupervisedMOJOModelParams(H2OUnsupervisedMOJOModelParams, HasNtrees):
    pass


class H2OTreeBasedSupervisedMOJOModelParams(H2OSupervisedMOJOModelParams, HasNtrees):
    pass
