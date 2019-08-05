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

from pyspark import keyword_only
from pyspark.ml.util import JavaMLWritable, JavaMLReadable
from pyspark.ml.wrapper import JavaEstimator
from pyspark.sql.dataframe import DataFrame

from ai.h2o.sparkling import Initializer
from ai.h2o.sparkling.ml.models import H2OMOJOModel
from ai.h2o.sparkling.ml.params import H2OGridSearchParams
from ai.h2o.sparkling.ml.utils import set_double_values, validateEnumValue
from ai.h2o.sparkling.sparkSpecifics import get_input_kwargs


class H2OGridSearch(H2OGridSearchParams, JavaEstimator, JavaMLReadable, JavaMLWritable):
    @keyword_only
    def __init__(self, featuresCols=[], algo=None, splitRatio=1.0, hyperParameters={}, labelCol="label", weightCol=None, allStringColumnsToCategorical=True,
                 columnsToCategorical=[], strategy="Cartesian", maxRuntimeSecs=0.0, maxModels=0, seed=-1,
                 stoppingRounds=0, stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=0, selectBestModelBy="AUTO",
                 selectBestModelDecreasing=True, foldCol=None, convertUnknownCategoricalLevelsToNa=True,
                 predictionCol="prediction", detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                 convertInvalidNumbersToNa=False, calculateContributions=False, **deprecatedArgs):
        Initializer.load_sparkling_jar()
        super(H2OGridSearch, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.H2OGridSearch", self.uid)

        self._setDefault(featuresCols=[], algo=None, splitRatio=1.0, hyperParameters={}, labelCol="label", weightCol=None, allStringColumnsToCategorical=True,
                         columnsToCategorical=[], strategy="Cartesian",
                         maxRuntimeSecs=0.0, maxModels=0, seed=-1,
                         stoppingRounds=0, stoppingTolerance=0.001,
                         stoppingMetric="AUTO", nfolds=0,
                         selectBestModelBy="AUTO", selectBestModelDecreasing=True, foldCol=None,
                         convertUnknownCategoricalLevelsToNa=True, predictionCol="prediction",
                         detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                         convertInvalidNumbersToNa=False, calculateContributions=False)
        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCols=[], algo=None, splitRatio=1.0, hyperParameters={}, labelCol="label", weightCol=None, allStringColumnsToCategorical=True,
                  columnsToCategorical=[], strategy="Cartesian", maxRuntimeSecs=0.0, maxModels=0, seed=-1,
                  stoppingRounds=0, stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=0, selectBestModelBy="AUTO",
                  selectBestModelDecreasing=True, foldCol=None, convertUnknownCategoricalLevelsToNa=True,
                  predictionCol="prediction", detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                  convertInvalidNumbersToNa=False, calculateContributions=False, **deprecatedArgs):
        kwargs = get_input_kwargs(self)

        validateEnumValue(self._H2OGridSearchParams__getStrategyEnum(), kwargs, "strategy")
        validateEnumValue(self._H2OGridSearchParams__getStoppingMetricEnum(), kwargs, "stoppingMetric")
        validateEnumValue(self._H2OGridSearchParams__getSelectBestModelByEnum(), kwargs, "selectBestModelBy")

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["splitRatio", "stoppingTolerance", "maxRuntimeSecs"]
        set_double_values(kwargs, double_types)
        if "algo" in kwargs and kwargs["algo"] is not None:
            tmp = kwargs["algo"]
            del kwargs['algo']
            self._java_obj.setAlgo(tmp._java_obj)

        return self._set(**kwargs)

    def get_grid_models(self):
        return [H2OMOJOModel(m) for m in self._java_obj.getGridModels()]

    def get_grid_models_params(self):
        return DataFrame(self._java_obj.getGridModelsParams(),  self._hc._sql_context)

    def get_grid_models_metrics(self):
        return DataFrame(self._java_obj.getGridModelsMetrics(),  self._hc._sql_context)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)
