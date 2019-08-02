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
from pyspark.ml.param import *
from pyspark.ml.param import *
from pyspark.ml.util import JavaMLWritable, JavaMLReadable
from pyspark.ml.wrapper import JavaEstimator

from ai.h2o.sparkling import Initializer
from ai.h2o.sparkling.ml.models import H2OMOJOModel
from ai.h2o.sparkling.ml.params import H2OKMeansParams
from ai.h2o.sparkling.ml.utils import set_double_values, validateEnumValue, getDoubleArrayArrayFromIntArrayArray
from ai.h2o.sparkling.sparkSpecifics import get_input_kwargs


class H2OKMeans(H2OKMeansParams, JavaEstimator, JavaMLReadable, JavaMLWritable):
    @keyword_only
    def __init__(self,
                 predictionCol="prediction",
                 detailedPredictionCol="detailed_prediction",
                 withDetailedPredictionCol=False,
                 featuresCols=[],
                 foldCol=None,
                 weightCol=None,
                 splitRatio=1.0,
                 seed=-1,
                 nfolds=0,
                 allStringColumnsToCategorical=True,
                 columnsToCategorical=[],
                 convertUnknownCategoricalLevelsToNa=False,
                 convertInvalidNumbersToNa=False,
                 modelId=None,
                 keepCrossValidationPredictions=False,
                 keepCrossValidationFoldAssignment=False,
                 parallelizeCrossValidation=True,
                 distribution="AUTO",
                 maxIterations=10,
                 standardize=True,
                 init="Furthest",
                 userPoints=None,
                 estimateK=False,
                 k=2,
                 **deprecatedArgs):
        Initializer.load_sparkling_jar()
        super(H2OKMeans, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.H2OKMeans", self.uid)

        self._setDefault(
            predictionCol="prediction",
            detailedPredictionCol="detailed_prediction",
            withDetailedPredictionCol=False,
            featuresCols=[],
            foldCol=None,
            weightCol=None,
            splitRatio=1.0,
            seed=-1,
            nfolds=0,
            allStringColumnsToCategorical=True,
            columnsToCategorical=[],
            convertUnknownCategoricalLevelsToNa=False,
            convertInvalidNumbersToNa=False,
            modelId=None,
            keepCrossValidationPredictions=False,
            keepCrossValidationFoldAssignment=False,
            parallelizeCrossValidation=True,
            distribution="AUTO",
            maxIterations=10,
            standardize=True,
            init="Furthest",
            userPoints=None,
            estimateK=False,
            k=2)
        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self,
                  predictionCol="prediction",
                  detailedPredictionCol="detailed_prediction",
                  withDetailedPredictionCol=False,
                  featuresCols=[],
                  foldCol=None,
                  weightCol=None,
                  splitRatio=1.0,
                  seed=-1,
                  nfolds=0,
                  allStringColumnsToCategorical=True,
                  columnsToCategorical=[],
                  convertUnknownCategoricalLevelsToNa=False,
                  convertInvalidNumbersToNa=False,
                  modelId=None,
                  keepCrossValidationPredictions=False,
                  keepCrossValidationFoldAssignment=False,
                  parallelizeCrossValidation=True,
                  distribution="AUTO",
                  maxIterations=10,
                  standardize=True,
                  init="Furthest",
                  userPoints=None,
                  estimateK=False,
                  k=2,
                  **deprecatedArgs):
        kwargs = get_input_kwargs(self)

        validateEnumValue(self._H2OAlgoCommonParams__getDistributionEnum(), kwargs, "distribution")
        validateEnumValue(self._H2OKMeansParams__getInitEnum(), kwargs, "init")

        # We need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["splitRatio"]
        set_double_values(kwargs, double_types)

        if "userPoints" in kwargs:
            kwargs["userPoints"] = getDoubleArrayArrayFromIntArrayArray(kwargs["userPoints"])

        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)
