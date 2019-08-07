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

from ai.h2o.sparkling import Initializer
from ai.h2o.sparkling.ml.algos.H2OAutoMLExtensions import H2OAutoMLExtensions
from ai.h2o.sparkling.ml.models import H2OMOJOModel
from ai.h2o.sparkling.ml.params import H2OAutoMLParams
from ai.h2o.sparkling.sparkSpecifics import get_input_kwargs


class H2OAutoML(H2OAutoMLParams, H2OAutoMLExtensions, JavaEstimator, JavaMLReadable, JavaMLWritable):

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

        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)
