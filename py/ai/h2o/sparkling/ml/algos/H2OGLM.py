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
from ai.h2o.sparkling.ml.params import H2OGLMParams
from ai.h2o.sparkling.ml.utils import set_double_values, validateEnumValue, arrayToDoubleArray
from ai.h2o.sparkling.sparkSpecifics import get_input_kwargs


class H2OGLM(H2OGLMParams, JavaEstimator, JavaMLReadable, JavaMLWritable):
    @keyword_only
    def __init__(self, modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                 nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                 seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False,
                 standardize=True, family="gaussian", link="family_default", solver="AUTO", tweedieVariancePower=0.0,
                 tweedieLinkPower=0.0, alpha=None, lambda_=None, missingValuesHandling="MeanImputation",
                 prior=-1.0, lambdaSearch=False, nlambdas=-1, nonNegative=False, exactLambdas=False,
                 lambdaMinRatio=-1.0,maxIterations=-1, intercept=True, betaEpsilon=1e-4, objectiveEpsilon=-1.0,
                 gradientEpsilon=-1.0, objReg=-1.0, computePValues=False, removeCollinearCols=False,
                 interactions=None, interactionPairs=None, earlyStopping=True, foldCol=None,
                 predictionCol="prediction", detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                 convertInvalidNumbersToNa=False, calculateContributions=False, **deprecatedArgs):
        Initializer.load_sparkling_jar()
        super(H2OGLM, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.H2OGLM", self.uid)

        self._setDefault(modelId=None, splitRatio=1.0, labelCol="label", weightCol=None,  featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                         nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                         seed=-1, distribution="AUTO",
                         convertUnknownCategoricalLevelsToNa=False,
                         standardize=True, family="gaussian",
                         link="family_default",
                         solver="AUTO", tweedieVariancePower=0.0,
                         tweedieLinkPower=0.0, alpha=None, lambda_=None,
                         missingValuesHandling="MeanImputation",
                         prior=-1.0, lambdaSearch=False, nlambdas=-1, nonNegative=False, exactLambdas=False,
                         lambdaMinRatio=-1.0,maxIterations=-1, intercept=True, betaEpsilon=1e-4, objectiveEpsilon=-1.0,
                         gradientEpsilon=-1.0, objReg=-1.0, computePValues=False, removeCollinearCols=False,
                         interactions=None, interactionPairs=None, earlyStopping=True, foldCol=None,
                         predictionCol="prediction", detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                         convertInvalidNumbersToNa=False, calculateContributions=False)
        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False,parallelizeCrossValidation=True,
                  seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False,
                  standardize=True, family="gaussian", link="family_default", solver="AUTO", tweedieVariancePower=0.0,
                  tweedieLinkPower=0.0, alpha=None, lambda_=None, missingValuesHandling="MeanImputation",
                  prior=-1.0, lambdaSearch=False, nlambdas=-1, nonNegative=False, exactLambdas=False,
                  lambdaMinRatio=-1.0,maxIterations=-1, intercept=True, betaEpsilon=1e-4, objectiveEpsilon=-1.0,
                  gradientEpsilon=-1.0, objReg=-1.0, computePValues=False, removeCollinearCols=False,
                  interactions=None, interactionPairs=None, earlyStopping=True, foldCol=None,
                  predictionCol="prediction", detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                  convertInvalidNumbersToNa=False, calculateContributions=False, **deprecatedArgs):
        kwargs = get_input_kwargs(self)

        validateEnumValue(self._H2OAlgoCommonParams__getDistributionEnum(), kwargs, "distribution")
        validateEnumValue(self._H2OGLMParams__getFamilyEnum(), kwargs, "family")
        validateEnumValue(self._H2OGLMParams__getLinkEnum(), kwargs, "link")
        validateEnumValue(self._H2OGLMParams__getSolverEnum(), kwargs, "solver")
        validateEnumValue(self._H2OGLMParams__getMissingValuesHandlingEnum(), kwargs, "missingValuesHandling")

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["splitRatio", "tweedieVariancePower", "tweedieLinkPower", "prior", "lambdaMinRatio",
                        "betaEpsilon", "objectiveEpsilon", "gradientEpsilon", "objReg"]
        set_double_values(kwargs, double_types)

        # We need to also map all doubles in the arrays
        arrayToDoubleArray("alpha", kwargs)
        arrayToDoubleArray("lambda_", kwargs)

        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)
