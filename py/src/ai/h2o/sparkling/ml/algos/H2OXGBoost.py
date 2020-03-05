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
from ai.h2o.sparkling.ml.algos.H2OTreeBasedSupervisedAlgoBase import H2OTreeBasedSupervisedAlgoBase
from ai.h2o.sparkling.ml.params import H2OXGBoostParams


class H2OXGBoost(H2OXGBoostParams, H2OTreeBasedSupervisedAlgoBase):

    @keyword_only
    def __init__(self,
                 quietMode=True,
                 ntrees=50,
                 maxDepth=6,
                 minRows=1.0,
                 minChildWeight=1.0,
                 learnRate=0.3,
                 eta=0.3,
                 sampleRate=1.0,
                 subsample=1.0,
                 colSampleRate=1.0,
                 colSampleByLevel=1.0,
                 colSampleRatePerTree=1.0,
                 colSampleByTree=1.0,
                 maxAbsLeafnodePred=0.0,
                 maxDeltaStep=0.0,
                 scoreTreeInterval=0,
                 minSplitImprovement=0.0,
                 gamma=0.0,
                 nthread=-1,
                 maxBins=256,
                 maxLeaves=0,
                 minSumHessianInLeaf=100.0,
                 minDataInLeaf=0.0,
                 treeMethod="auto",
                 growPolicy="depthwise",
                 booster="gbtree",
                 dmatrixType="auto",
                 regLambda=0.0,
                 regAlpha=0.0,
                 sampleType="uniform",
                 normalizeType="tree",
                 rateDrop=0.0,
                 oneDrop=False,
                 skipDrop=0.0,
                 gpuId=0,
                 backend="auto",
                 modelId=None,
                 keepCrossValidationPredictions=False,
                 keepCrossValidationFoldAssignment=False,
                 parallelizeCrossValidation=True,
                 distribution="AUTO",
                 labelCol="label",
                 foldCol=None,
                 weightCol=None,
                 offsetCol=None,
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
                 convertInvalidNumbersToNa=False,
                 namedMojoOutputColumns=True,
                 monotoneConstraints={},
                 stoppingRounds=0,
                 stoppingMetric="AUTO",
                 stoppingTolerance=0.001,
                 **DeprecatedParams):
        Initializer.load_sparkling_jar()
        super(H2OXGBoost, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.H2OXGBoost", self.uid)
        self._setDefaultValuesFromJava()
        kwargs = Utils.getInputKwargs(self)
        self._set(**kwargs)
