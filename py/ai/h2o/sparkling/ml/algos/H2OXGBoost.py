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
from ai.h2o.sparkling.ml.models import H2OMOJOModel
from ai.h2o.sparkling.ml.params import H2OXGBoostParams
from ai.h2o.sparkling.ml.utils import set_double_values, validateEnumValue
from ai.h2o.sparkling.sparkSpecifics import get_input_kwargs


class H2OXGBoost(H2OXGBoostParams, JavaEstimator, JavaMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                 nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                 seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False, quietMode=True,
                 ntrees=50, nEstimators=0, maxDepth=6, minRows=1.0, minChildWeight=1.0, learnRate=0.3, eta=0.3, learnRateAnnealing=1.0,
                 sampleRate=1.0, subsample=1.0, colSampleRate=1.0, colSampleByLevel=1.0, colSampleRatePerTree=1.0, colsampleBytree=1.0,
                 maxAbsLeafnodePred=0.0, maxDeltaStep=0.0, scoreTreeInterval=0, initialScoreInterval=4000, scoreInterval=4000,
                 minSplitImprovement=0.0, gamma=0.0, nthread=-1, maxBins=256, maxLeaves=0,
                 minSumHessianInLeaf=100.0, minDataInLeaf=0.0, treeMethod="auto", growPolicy="depthwise",
                 booster="gbtree", dmatrixType="auto", regLambda=0.0, regAlpha=0.0, sampleType="uniform",
                 normalizeType="tree", rateDrop=0.0, oneDrop=False, skipDrop=0.0, gpuId=0, backend="auto",
                 foldCol=None, predictionCol="prediction", detailedPredictionCol="detailed_prediction",
                 withDetailedPredictionCol=False, convertInvalidNumbersToNa=False, **deprecatedArgs):
        Initializer.load_sparkling_jar()
        super(H2OXGBoost, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.H2OXGBoost", self.uid)

        self._setDefault(modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                         nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                         seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False,
                         quietMode=True, ntrees=50, nEstimators=0, maxDepth=6, minRows=1.0, minChildWeight=1.0,
                         learnRate=0.3, eta=0.3, learnRateAnnealing=1.0, sampleRate=1.0, subsample=1.0, colSampleRate=1.0, colSampleByLevel=1.0,
                         colSampleRatePerTree=1.0, colsampleBytree=1.0, maxAbsLeafnodePred=0.0, maxDeltaStep=0.0, scoreTreeInterval=0,
                         initialScoreInterval=4000, scoreInterval=4000, minSplitImprovement=0.0, gamma=0.0, nthread=-1, maxBins=256, maxLeaves=0,
                         minSumHessianInLeaf=100.0, minDataInLeaf=0.0,
                         treeMethod="auto", growPolicy="depthwise", booster="gbtree", dmatrixType="auto", regLambda=0.0, regAlpha=0.0,
                         sampleType="uniform", normalizeType="tree", rateDrop=0.0, oneDrop=False, skipDrop=0.0, gpuId=0,
                         backend="auto", foldCol=None, predictionCol="prediction", detailedPredictionCol="detailed_prediction",
                         withDetailedPredictionCol=False, convertInvalidNumbersToNa=False)

        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                  seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False, quietMode=True,
                  ntrees=50, nEstimators=0, maxDepth=6, minRows=1.0, minChildWeight=1.0, learnRate=0.3, eta=0.3, learnRateAnnealing=1.0,
                  sampleRate=1.0, subsample=1.0, colSampleRate=1.0, colSampleByLevel=1.0, colSampleRatePerTree=1.0, colsampleBytree=1.0,
                  maxAbsLeafnodePred=0.0, maxDeltaStep=0.0, scoreTreeInterval=0, initialScoreInterval=4000, scoreInterval=4000,
                  minSplitImprovement=0.0, gamma=0.0, nthread=-1, maxBins=256, maxLeaves=0,
                  minSumHessianInLeaf=100.0, minDataInLeaf=0.0, treeMethod="auto", growPolicy="depthwise",
                  booster="gbtree", dmatrixType="auto", regLambda=0.0, regAlpha=0.0, sampleType="uniform",
                  normalizeType="tree", rateDrop=0.0, oneDrop=False, skipDrop=0.0, gpuId=0, backend="auto",
                  foldCol=None, predictionCol="prediction", detailedPredictionCol="detailed_prediction",
                  withDetailedPredictionCol=False, convertInvalidNumbersToNa=False, **deprecatedArgs):
        kwargs = get_input_kwargs(self)

        validateEnumValue(self._H2OAlgoCommonParams__getDistributionEnum(), kwargs, "distribution")
        validateEnumValue(self._H2OXGBoostParams__getTreeMethodEnum(), kwargs, "treeMethod")
        validateEnumValue(self._H2OXGBoostParams__getGrowPolicyEnum(), kwargs, "growPolicy")
        validateEnumValue(self._H2OXGBoostParams__getBoosterEnum(), kwargs, "booster")
        validateEnumValue(self._H2OXGBoostParams__getDmatrixTypeEnum(), kwargs, "dmatrixType")
        validateEnumValue(self._H2OXGBoostParams__getSampleTypeEnum(), kwargs, "sampleType")
        validateEnumValue(self._H2OXGBoostParams__getNormalizeTypeEnum(), kwargs, "normalizeType")
        validateEnumValue(self._H2OXGBoostParams__getBackendEnum(), kwargs, "backend")


        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["splitRatio", "minRows", "minChildWeight", "learnRate", "eta", "learnRateAnnealing"
                                                                                       "sampleRate", "subsample", "colSampleRate", "colSampleByLevel", "colSampleRatePerTree",
                        "colsampleBytree", "maxAbsLeafnodePred", "maxDeltaStep", "minSplitImprovement", "gamma",
                        "minSumHessianInLeaf", "minDataInLeaf", "regLambda", "regAlpha", "rateDrop", "skipDrop"]
        set_double_values(kwargs, double_types)
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)
