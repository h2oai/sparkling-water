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
from ai.h2o.sparkling.ml.params import H2ODRFParams

class H2ODRF(H2ODRFParams, H2OTreeBasedSupervisedAlgoBase):

    @keyword_only
    def __init__(self,
                 binomialDoubleTrees=False,
                 mtries=-1,
                 ntrees=50,
                 maxDepth=20,
                 minRows=1,
                 nbins=20,
                 nbinsCats=1024,
                 minSplitImprovement=1e-5,
                 histogramType="AUTO",
                 r2Stopping=Utils.javaDoubleMaxValue,
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
                 stoppingRounds=0,
                 stoppingMetric="AUTO",
                 stoppingTolerance=0.001):
        Initializer.load_sparkling_jar()
        super(H2ODRF, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.H2ODRF", self.uid)
        self._setDefaultValuesFromJava()
        kwargs = Utils.getInputKwargs(self)
        self._set(**kwargs)
