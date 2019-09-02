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
from ai.h2o.sparkling.ml.algos.H2OAlgoBase import H2OAlgoBase
from ai.h2o.sparkling.ml.params import H2OGBMParams

javaMaxDoubleValue = (2 - 2 ** (-52)) * (2 ** 1023)


class H2OGBM(H2OGBMParams, H2OAlgoBase):

    @keyword_only
    def __init__(self,
                 learnRate=0.1,
                 learnRateAnnealing=1.0,
                 colSampleRate=1.0,
                 maxAbsLeafnodePred=javaMaxDoubleValue,
                 predNoiseBandwidth=0.0,
                 ntrees=50,
                 maxDepth=5,
                 minRows=10.0,
                 nbins=20,
                 nbinsCats=1024,
                 minSplitImprovement=1e-5,
                 histogramType="AUTO",
                 r2Stopping=javaMaxDoubleValue,
                 nbinsTopLevel=1 << 10,
                 buildTreeOneNode=False,
                 scoreTreeInterval=0,
                 sampleRate=1.0,
                 sampleRatePerClass=None,
                 colSampleRateChangePerLevel=1.0,
                 colSampleRatePerTree=1.0,
                 modelId=None,
                 keepCrossValidationPredictions=False,
                 keepCrossValidationFoldAssignment=False,
                 parallelizeCrossValidation=True,
                 distribution="AUTO",
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
                 convertInvalidNumbersToNa=False,
                 namedMojoOutputColumns=True):
        Initializer.load_sparkling_jar()
        super(H2OGBM, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.H2OGBM", self.uid)
        self._setDefaultValuesFromJava()
        kwargs = Utils.getInputKwargs(self)
        self._set(**kwargs)
