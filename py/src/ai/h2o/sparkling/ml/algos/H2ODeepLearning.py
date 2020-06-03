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

from ai.h2o.sparkling import Initializer
from ai.h2o.sparkling.ml.Utils import Utils
from ai.h2o.sparkling.ml.algos.H2OSupervisedAlgoBase import H2OSupervisedAlgoBase
from ai.h2o.sparkling.ml.params import H2ODeepLearningParams

import sys


class H2ODeepLearning(H2ODeepLearningParams, H2OSupervisedAlgoBase):

    @keyword_only
    def __init__(self,
                 epochs=10.0,
                 l1=0.0,
                 l2=0.0,
                 hidden=[200, 200],
                 reproducible=False,
                 activation="Rectifier",
                 quantileAlpha=0.5,
                 modelId=None,
                 keepCrossValidationPredictions=False,
                 keepCrossValidationFoldAssignment=False,
                 keepCrossValidationModels=True,
                 parallelizeCrossValidation=True,
                 distribution="AUTO",
                 labelCol="label",
                 foldCol=None,
                 weightCol=None,
                 offsetCol=None,
                 splitRatio=1.0,
                 seed=-1,
                 nfolds=0,
                 columnsToCategorical=[],
                 predictionCol="prediction",
                 detailedPredictionCol="detailed_prediction",
                 withDetailedPredictionCol=False,
                 featuresCols=[],
                 convertUnknownCategoricalLevelsToNa=False,
                 convertInvalidNumbersToNa=False,
                 namedMojoOutputColumns=True,
                 stoppingRounds=5,
                 stoppingMetric="AUTO",
                 stoppingTolerance=0.001,
                 inputDropoutRatio=0.0,
                 shuffleTrainingData=False,
                 rateDecay=1.0,
                 singleNodeMode=False,
                 ignoredCols=None,
                 ignoreConstCols=True,
                 hiddenDropoutRatios=None,
                 useAllFactorLevels=True,
                 missingValuesHandling="MeanImputation",
                 maxCategoricalFeatures=2147483647,
                 fastMode=True,
                 sparse=False,
                 scoreTrainingSamples=10000,
                 adaptiveRate=True,
                 initialWeightScale=1.0,
                 customMetricFunc=None,
                 customDistributionFunc=None,
                 autoencoder=False,
                 classificationStop=0.0,
                 standardize=True,
                 targetRatioCommToComp=0.05,
                 classSamplingFactors=None,
                 elasticAveragingMovingRate=0.9,
                 quietMode=False,
                 scoreValidationSampling="Uniform",
                 epsilon=.00000001,
                 trainSamplesPerIteration=-2,
                 diagnostics=True,
                 momentumStable=0.0,
                 rate=0.005,
                 regressionStop=0.000001,
                 initialWeightDistribution="UniformAdaptive",
                 sparsityBeta=0.0,
                 variableImportances=True,
                 loss="Automatic",
                 rateAnnealing=0.000001,
                 scoreDutyCycle=0.1,
                 maxRuntimeSecs=0.0,
                 exportCheckpointsDir=None,
                 nesterovAcceleratedGradient=True,
                 momentumRamp=1000000.0,
                 rho=0.99,
                 scoreInterval=5.0,
                 balanceClasses=False,
                 elasticAveraging=False,
                 averageActivation=0.0,
                 forceLoadBalance=True,
                 categoricalEncoding="AUTO",
                 momentumStart=0.0,
                 maxAfterBalanceSize=5.0,
                 tweediePower=1.5,
                 huberAlpha=0.9,
                 overwriteWithBestModel=True,
                 scoreEachIteration=False,
                 exportWeightsAndBiases=False,
                 foldAssignment="AUTO",
                 maxW2=sys.float_info.max,
                 elasticAveragingRegularization=0.001,
                 replicateTrainingData=True,
                 miniBatchSize=1,
                 scoreValidationSamples=0,
                 maxCategoricalLevels=10,
                 withContributions=False):
        Initializer.load_sparkling_jar()
        super(H2ODeepLearning, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.H2ODeepLearning", self.uid)
        self._setDefaultValuesFromJava()
        kwargs = Utils.getInputKwargs(self)
        self._set(**kwargs)
