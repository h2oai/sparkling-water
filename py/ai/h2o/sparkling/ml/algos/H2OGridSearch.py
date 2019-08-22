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
from pyspark.sql.dataframe import DataFrame

from ai.h2o.sparkling import Initializer
from ai.h2o.sparkling.ml.Utils import Utils
from ai.h2o.sparkling.ml.algos.H2OAlgoBase import H2OAlgoBase
from ai.h2o.sparkling.ml.models import H2OMOJOModel
from ai.h2o.sparkling.ml.params import H2OGridSearchParams


class H2OGridSearch(H2OGridSearchParams, H2OAlgoBase):

    @keyword_only
    def __init__(self,
                 algo=None,
                 hyperParameters={},
                 strategy="Cartesian",
                 maxRuntimeSecs=0.0,
                 maxModels=0,
                 stoppingRounds=0,
                 stoppingTolerance=0.001,
                 stoppingMetric="AUTO",
                 selectBestModelBy="AUTO",
                 selectBestModelDecreasing=True,
                 labelCol="label",
                 foldCol=None,
                 weightCol=None,
                 splitRatio=1.0,
                 seed=-1,
                 nfolds=0,
                 allStringColumnsToCategorical=True,
                 columnsToCategorical=[],
                 predictionCol="prediction",
                 detailedPredictionCol="detailed_prediction",
                 withDetailedPredictionCol=False,
                 featuresCols=[],
                 convertUnknownCategoricalLevelsToNa=False,
                 convertInvalidNumbersToNa=False):
        Initializer.load_sparkling_jar()
        super(H2OGridSearch, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.H2OGridSearch", self.uid)
        self._setDefaultValuesFromJava(["algoParams"])
        kwargs = Utils.getInputKwargs(self)

        if "algo" in kwargs and kwargs["algo"] is not None:
            tmp = kwargs["algo"]
            del kwargs['algo']
            self._java_obj.setAlgo(tmp._java_obj)

        self._set(**kwargs)

    def get_grid_models(self):
        return [H2OMOJOModel(m) for m in self._java_obj.getGridModels()]

    def get_grid_models_params(self):
        return DataFrame(self._java_obj.getGridModelsParams(), self._hc._sql_context)

    def get_grid_models_metrics(self):
        return DataFrame(self._java_obj.getGridModelsMetrics(), self._hc._sql_context)
