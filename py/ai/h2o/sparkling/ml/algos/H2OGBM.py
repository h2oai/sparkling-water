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
from pyspark.ml.util import _jvm
from pyspark.ml.wrapper import JavaEstimator

from ai.h2o.sparkling import Initializer
from ai.h2o.sparkling.ml.models import H2OMOJOModel
from ai.h2o.sparkling.ml.params import H2OGBMParams
from ai.h2o.sparkling.ml.utils import set_double_values, validateEnumValue, arrayToDoubleArray
from ai.h2o.sparkling.sparkSpecifics import get_input_kwargs

java_max_double_value = (2-2**(-52))*(2**1023)


class H2OGBM(H2OGBMParams, JavaEstimator, JavaMLReadable, JavaMLWritable):
    @keyword_only
    def __init__(self, modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                 nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                 seed=-1, distribution="AUTO", ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                 histogramType="AUTO", r2Stopping=java_max_double_value,
                 nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                 sampleRate=1.0, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                 learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=java_max_double_value,
                 predNoiseBandwidth=0.0, convertUnknownCategoricalLevelsToNa=False, foldCol=None, predictionCol="prediction",
                 detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                 convertInvalidNumbersToNa=False, **deprecatedArgs):
        Initializer.load_sparkling_jar()
        super(H2OGBM, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.H2OGBM", self.uid)

        self._setDefault(modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                         nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                         seed=-1, distribution="AUTO",
                         ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                         histogramType="AUTO",
                         r2Stopping=_jvm().Double.MAX_VALUE, nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                         sampleRate=1.0, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                         learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=_jvm().Double.MAX_VALUE,
                         predNoiseBandwidth=0.0, convertUnknownCategoricalLevelsToNa=False, foldCol=None,
                         predictionCol="prediction", detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                         convertInvalidNumbersToNa=False)
        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False,parallelizeCrossValidation=True,
                  seed=-1, distribution="AUTO", ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                  histogramType="AUTO", r2Stopping=java_max_double_value,
                  nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                  sampleRate=1.0, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                  learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=java_max_double_value,
                  predNoiseBandwidth=0.0, convertUnknownCategoricalLevelsToNa=False, foldCol=None, predictionCol="prediction",
                  detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                  convertInvalidNumbersToNa=False, **deprecatedArgs):
        kwargs = get_input_kwargs(self)

        validateEnumValue(self._H2OAlgoCommonParams__getDistributionEnum(), kwargs, "distribution")
        validateEnumValue(self._H2OSharedTreeParams__getHistogramTypeEnum(), kwargs, "histogramType")

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["minRows", "predNoiseBandwidth", "splitRatio", "learnRate", "colSampleRate", "learnRateAnnealing", "maxAbsLeafnodePred"
                                                                                                                           "minSplitImprovement", "r2Stopping", "sampleRate", "colSampleRateChangePerLevel", "colSampleRatePerTree"]
        set_double_values(kwargs, double_types)

        # We need to also map all doubles in the arrays
        arrayToDoubleArray("sampleRatePerClass", kwargs)

        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)
