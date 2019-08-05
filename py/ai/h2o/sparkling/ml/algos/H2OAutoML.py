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

import random
import string
from pyspark import keyword_only
from pyspark.ml.util import JavaMLWritable, JavaMLReadable
from pyspark.ml.wrapper import JavaEstimator
from pyspark.sql.dataframe import DataFrame

from ai.h2o.sparkling import Initializer
from ai.h2o.sparkling.ml.models import H2OMOJOModel
from ai.h2o.sparkling.ml.params import H2OAutoMLParams
from ai.h2o.sparkling.ml.utils import set_double_values, validateEnumValue, validateEnumValues
from ai.h2o.sparkling.sparkSpecifics import get_input_kwargs


class H2OAutoML(H2OAutoMLParams, JavaEstimator, JavaMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, featuresCols=[], labelCol="label", allStringColumnsToCategorical=True, columnsToCategorical=[], splitRatio=1.0, foldCol=None,
                 weightCol=None, ignoredCols=[], includeAlgos=None, excludeAlgos=None, projectName=None, maxRuntimeSecs=3600.0, stoppingRounds=3,
                 stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5, convertUnknownCategoricalLevelsToNa=True, seed=-1,
                 sortMetric="AUTO", balanceClasses=False, classSamplingFactors=None, maxAfterBalanceSize=5.0,
                 keepCrossValidationPredictions=True, keepCrossValidationModels=True, maxModels=0,
                 predictionCol="prediction", detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                 convertInvalidNumbersToNa=False, **deprecatedArgs):
        Initializer.load_sparkling_jar()
        super(H2OAutoML, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.H2OAutoML", self.uid)

        self._setDefault(featuresCols=[], labelCol="label", allStringColumnsToCategorical=True, columnsToCategorical=[], splitRatio=1.0, foldCol=None,
                         weightCol=None, ignoredCols=[], includeAlgos=None, excludeAlgos=None, projectName=None, maxRuntimeSecs=3600.0, stoppingRounds=3,
                         stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5,
                         convertUnknownCategoricalLevelsToNa=True, seed=-1, sortMetric="AUTO", balanceClasses=False,
                         classSamplingFactors=None, maxAfterBalanceSize=5.0, keepCrossValidationPredictions=True,
                         keepCrossValidationModels=True, maxModels=0, predictionCol="prediction", detailedPredictionCol="detailed_prediction",
                         withDetailedPredictionCol=False, convertInvalidNumbersToNa=False)
        kwargs = get_input_kwargs(self)

        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCols=[], labelCol="label", allStringColumnsToCategorical=True, columnsToCategorical=[], splitRatio=1.0, foldCol=None,
                  weightCol=None, ignoredCols=[], includeAlgos=None, excludeAlgos=None, projectName=None, maxRuntimeSecs=3600.0, stoppingRounds=3,
                  stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5, convertUnknownCategoricalLevelsToNa=True, seed=-1,
                  sortMetric="AUTO", balanceClasses=False, classSamplingFactors=None, maxAfterBalanceSize=5.0, keepCrossValidationPredictions=True,
                  keepCrossValidationModels=True, maxModels=0, predictionCol="prediction", detailedPredictionCol="detailed_prediction",
                  withDetailedPredictionCol=False, convertInvalidNumbersToNa=False, **deprecatedArgs):

        kwargs = get_input_kwargs(self)

        validateEnumValues(self._H2OAutoMLParams__getAutomlAlgoEnum(), kwargs, "includeAlgos", nullEnabled=True)
        validateEnumValues(self._H2OAutoMLParams__getAutomlAlgoEnum(), kwargs, "excludeAlgos", nullEnabled=True)
        validateEnumValue(self._H2OAutoMLParams__getStoppingMetricEnum(), kwargs, "stoppingMetric")
        validateEnumValue(self._H2OAutoMLParams__getSortMetricEnum(), kwargs, "sortMetric")

        if "projectName" in kwargs and kwargs["projectName"] is None:
            kwargs["projectName"] = ''.join(random.choice(string.ascii_letters) for i in range(30))

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["maxRuntimeSecs", "stoppingTolerance", "splitRatio", "maxAfterBalanceSize"]
        set_double_values(kwargs, double_types)
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)

    def leaderboard(self):
        leaderboard_java = self._java_obj.leaderboard()
        if leaderboard_java.isDefined():
            return DataFrame(leaderboard_java.get(), self._hc._sql_context)
        else:
            return None
