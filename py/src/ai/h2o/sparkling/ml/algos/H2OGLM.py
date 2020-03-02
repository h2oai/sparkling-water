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
from ai.h2o.sparkling.ml.params import H2OGLMParams


class H2OGLM(H2OGLMParams, H2OSupervisedAlgoBase):

    @keyword_only
    def __init__(self,
                 standardize=True,
                 family="gaussian",
                 link="family_default",
                 solver="AUTO",
                 tweedieVariancePower=0.0,
                 tweedieLinkPower=0.0,
                 alphaValue=None,
                 lambdaValue=None,
                 missingValuesHandling="MeanImputation",
                 prior=-1.0,
                 lambdaSearch=False,
                 nlambdas=-1,
                 nonNegative=False,
                 lambdaMinRatio=-1.0,
                 maxIterations=-1,
                 intercept=True,
                 betaEpsilon=1e-4,
                 objectiveEpsilon=-1.0,
                 gradientEpsilon=-1.0,
                 objReg=-1.0,
                 computePValues=False,
                 removeCollinearCols=False,
                 interactions=None,
                 interactionPairs=None,
                 earlyStopping=True,
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
                 **DeprecatedParams):
        Initializer.load_sparkling_jar()
        super(H2OGLM, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.H2OGLM", self.uid)
        self._setDefaultValuesFromJava()
        kwargs = Utils.getInputKwargs(self)
        self._set(**kwargs)
