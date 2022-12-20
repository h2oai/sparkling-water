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
from ai.h2o.sparkling.H2ODataFrameConverters import H2ODataFrameConverters
from ai.h2o.sparkling.ml.metrics.H2OMetricsFactory import H2OMetricsFactory
from pyspark.ml.param import *


class H2OMOJOModelParams:

    def getModelDetails(self):
        return self._java_obj.getModelDetails()

    def getModelSummary(self):
        return H2ODataFrameConverters.scalaToPythonDataFrame(self._java_obj.getModelSummary())

    def getDomainValues(self):
        return H2OTypeConverters.scalaMapStringDictStringToStringDictString(self._java_obj.getDomainValues())

    def getTrainingMetrics(self):
        """
        :return: A map of all metrics of the float type calculated on the training dataset.
        """
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getTrainingMetrics())

    def getTrainingMetricsObject(self):
        """
        :return: An object holding all metrics of the float type and also more complex performance information
        calculated on the training dataset.
        """
        return H2OMetricsFactory.fromJavaObject(self._java_obj.getTrainingMetricsObject())

    def getValidationMetrics(self):
        """
        :return: A map of all metrics of the float type calculated on the validation dataset.
        """
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getValidationMetrics())

    def getValidationMetricsObject(self):
        """
        :return: An object holding all metrics of the float type and also more complex performance information
        calculated on the validation dataset.
        """
        return H2OMetricsFactory.fromJavaObject(self._java_obj.getValidationMetricsObject())

    def getCrossValidationMetrics(self):
        """
        :return: A map of all combined cross-validation holdout metrics of the float type.
        """
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getCrossValidationMetrics())

    def getCrossValidationMetricsObject(self):
        """
        :return: An object holding all metrics of the Double type and also more complex performance information
        combined from cross-validation holdouts.
        """
        return H2OMetricsFactory.fromJavaObject(self._java_obj.getCrossValidationMetricsObject())

    def getCurrentMetrics(self):
        """
        :return: A map of all metrics of the Double type. If the nfolds parameter was set, the metrics were combined
        from cross-validation holdouts. If cross validations wasn't enabled, the metrics were calculated from
        a validation dataset. If the validation dataset wasn't available, the metrics were calculated from
        the training dataset.
        """
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getCurrentMetrics())

    def getCurrentMetricsObject(self):
        """
        :return: An object holding all metrics of the Double type and also more complex performance information.
        If the nfolds parameter was set, the object was combined from cross-validation holdouts. If cross validations
        wasn't enabled, the object was calculated from a validation dataset. If the validation dataset wasn't available,
        the object was calculated from the training dataset.
        """
        return H2OMetricsFactory.fromJavaObject(self._java_obj.getCurrentMetricsObject())

    def getCrossValidationMetricsSummary(self):
        """
        :return: A data frame with information about performance of individual folds according to various model metrics.
        """
        return H2ODataFrameConverters.scalaToPythonDataFrame(self._java_obj.getCrossValidationMetricsSummary())

    def getTrainingParams(self):
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getTrainingParams())

    def getModelCategory(self):
        return self._java_obj.getModelCategory()

    def getScoringHistory(self):
        return H2ODataFrameConverters.scalaToPythonDataFrame(self._java_obj.getScoringHistory())

    def getCrossValidationModelsScoringHistory(self):
        return H2ODataFrameConverters.scalaDfArrayToPythonDfArray(self._java_obj.getCrossValidationModelsScoringHistory())

    def getFeatureImportances(self):
        return H2ODataFrameConverters.scalaToPythonDataFrame(self._java_obj.getFeatureImportances())

    def getStartTime(self):
        """:return: Start time in milliseconds"""
        return self._java_obj.getStartTime()

    def getEndTime(self):
        """:return: End time in milliseconds"""
        return self._java_obj.getEndTime()

    def getRunTime(self):
        """:return: Runtime in milliseconds"""
        return self._java_obj.getRunTime()

    def getDefaultThreshold(self):
        """:return: Default threshold used for predictions"""
        return self._java_obj.getDefaultThreshold()

    def getMojoFileName(self):
        return self._java_obj.mojoFileName()

    def getCoefficients(self):
        return H2ODataFrameConverters.scalaToPythonDataFrame(self._java_obj.getCoefficients())


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
