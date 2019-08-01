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

from h2o.utils.typechecks import assert_is_type
from pyspark.ml.param import *

from ai.h2o.sparkling.ml.params.H2OAlgoSupervisedParams import H2OAlgoSupervisedParams
from ai.h2o.sparkling.ml.utils import getValidatedEnumValue, getDoubleArrayFromIntArray


class H2OGLMParams(H2OAlgoSupervisedParams):

    ##
    # Param definitions
    ##
    standardize = Param(Params._dummy(), "standardize", "standardize")
    family = Param(Params._dummy(), "family", "family")
    link = Param(Params._dummy(), "link", "link")
    solver = Param(Params._dummy(), "solver", "solver")
    tweedieVariancePower = Param(Params._dummy(), "tweedieVariancePower", "Tweedie variance power")
    tweedieLinkPower = Param(Params._dummy(), "tweedieLinkPower", "Tweedie link power")
    alpha = Param(Params._dummy(), "alpha", "alpha")
    lambda_ = Param(Params._dummy(), "lambda_", "lambda")
    missingValuesHandling = Param(Params._dummy(), "missingValuesHandling", "missingValuesHandling")
    prior = Param(Params._dummy(), "prior", "prior")
    lambdaSearch = Param(Params._dummy(), "lambdaSearch", "lambda search")
    nlambdas = Param(Params._dummy(), "nlambdas", "nlambdas")
    nonNegative = Param(Params._dummy(), "nonNegative", "nonNegative")
    exactLambdas = Param(Params._dummy(), "exactLambdas", "exact lambdas")
    lambdaMinRatio = Param(Params._dummy(), "lambdaMinRatio", "lambdaMinRatio")
    maxIterations = Param(Params._dummy(), "maxIterations", "maxIterations")
    intercept = Param(Params._dummy(), "intercept", "intercept")
    betaEpsilon = Param(Params._dummy(), "betaEpsilon", "betaEpsilon")
    objectiveEpsilon = Param(Params._dummy(), "objectiveEpsilon", "objectiveEpsilon")
    gradientEpsilon = Param(Params._dummy(), "gradientEpsilon", "gradientEpsilon")
    objReg = Param(Params._dummy(), "objReg", "objReg")
    computePValues = Param(Params._dummy(), "computePValues", "computePValues")
    removeCollinearCols = Param(Params._dummy(), "removeCollinearCols", "removeCollinearCols")
    interactions = Param(Params._dummy(), "interactions", "interactions")
    interactionPairs = Param(Params._dummy(), "interactionPairs", "interactionPairs")
    earlyStopping = Param(Params._dummy(), "earlyStopping", "earlyStopping")

    ##
    # Getters
    ##
    def getStandardize(self):
        return self.getOrDefault(self.standardize)

    def getFamily(self):
        return self.getOrDefault(self.family)

    def getLink(self):
        return self.getOrDefault(self.link)

    def getSolver(self):
        return self.getOrDefault(self.solver)

    def getTweedieVariancePower(self):
        return self.getOrDefault(self.tweedieVariancePower)

    def getTweedieLinkPower(self):
        return self.getOrDefault(self.tweedieLinkPower)

    def getAlpha(self):
        return self.getOrDefault(self.alpha)

    def getLambda(self):
        return self.getOrDefault(self.lambda_)

    def getMissingValuesHandling(self):
        return self.getOrDefault(self.missingValuesHandling)

    def getPrior(self):
        return self.getOrDefault(self.prior)

    def getLambdaSearch(self):
        return self.getOrDefault(self.lambdaSearch)

    def getNlambdas(self):
        return self.getOrDefault(self.nlambdas)

    def getNonNegative(self):
        return self.getOrDefault(self.nonNegative)

    def getExactLambdas(self):
        return self.getOrDefault(self.exactLambdas)

    def getLambdaMinRatio(self):
        return self.getOrDefault(self.lambdaMinRatio)

    def getMaxIterations(self):
        return self.getOrDefault(self.maxIterations)

    def getIntercept(self):
        return self.getOrDefault(self.intercept)

    def getBetaEpsilon(self):
        return self.getOrDefault(self.betaEpsilon)

    def getObjectiveEpsilon(self):
        return self.getOrDefault(self.objectiveEpsilon)

    def getGradientEpsilon(self):
        return self.getOrDefault(self.gradientEpsilon)

    def getObjReg(self):
        return self.getOrDefault(self.objReg)

    def getComputePValues(self):
        return self.getOrDefault(self.computePValues)

    def getRemoveCollinearCols(self):
        return self.getOrDefault(self.removeCollinearCols)

    def getInteractions(self):
        return self.getOrDefault(self.interactions)

    def getInteractionPairs(self):
        return self.getOrDefault(self.interactionPairs)

    def getEarlyStopping(self):
        return self.getOrDefault(self.earlyStopping)


    ##
    # Setters
    ##
    def setStandardize(self, value):
        assert_is_type(value, bool)
        return self._set(standardize=value)

    def setFamily(self, value):
        validated = getValidatedEnumValue(self.__getFamilyEnum(), value)
        return self._set(family=validated)

    def __getFamilyEnum(self):
        return "hex.glm.GLMModel$GLMParameters$Family"

    def setLink(self, value):
        validated = getValidatedEnumValue(self.__getLinkEnum(), value)
        return self._set(link=validated)

    def __getLinkEnum(self):
        return "hex.glm.GLMModel$GLMParameters$Link"

    def setSolver(self, value):
        validated = getValidatedEnumValue(self.__getSolverEnum(), value)
        return self._set(solver=validated)

    def __getSolverEnum(self):
        return "hex.glm.GLMModel$GLMParameters$Solver"

    def setTweedieVariancePower(self, value):
        assert_is_type(value, int, float)
        return self._set(tweedieVariancePower=float(value))

    def setTweedieLinkPower(self, value):
        assert_is_type(value, int, float)
        return self._set(tweedieLinkPower=float(value))

    def setAlpha(self, value):
        assert_is_type(value, None, [int, float])
        return self._set(alpha=getDoubleArrayFromIntArray(value))

    def setLambda(self, value):
        assert_is_type(value, None, [int, float])
        return self._set(lambda_=getDoubleArrayFromIntArray(value))

    def setMissingValuesHandling(self, value):
        validated = getValidatedEnumValue(self.__getMissingValuesHandlingEnum(), value)
        return self._set(missingValuesHandling=validated)

    def __getMissingValuesHandlingEnum(self):
        return "hex.deeplearning.DeepLearningModel$DeepLearningParameters$MissingValuesHandling"

    def setPrior(self, value):
        assert_is_type(value, int, float)
        return self._set(prior=float(value))

    def setLambdaSearch(self, value):
        assert_is_type(value, bool)
        return self._set(lambdaSearch=value)

    def setNlambdas(self, value):
        assert_is_type(value, int)
        return self._set(nlambdas=value)

    def setNonNegative(self, value):
        assert_is_type(value, bool)
        return self._set(nonNegative=value)

    def setExactLambdas(self, value):
        assert_is_type(value, bool)
        return self._set(exactLambdas=value)

    def setLambdaMinRatio(self, value):
        assert_is_type(value, int, float)
        return self._set(lambdaMinRatio=float(value))

    def setMaxIterations(self, value):
        assert_is_type(value, int)
        return self._set(maxIterations=value)

    def setIntercept(self, value):
        assert_is_type(value, bool)
        return self._set(intercept=value)

    def setBetaEpsilon(self, value):
        assert_is_type(value, int, float)
        return self._set(betaEpsilon=float(value))

    def setObjectiveEpsilon(self, value):
        assert_is_type(value, int, float)
        return self._set(objectiveEpsilon=float(value))

    def setGradientEpsilon(self, value):
        assert_is_type(value, int, float)
        return self._set(gradientEpsilon=float(value))

    def setObjReg(self, value):
        assert_is_type(value, int, float)
        return self._set(objReg=float(value))

    def setComputePValues(self, value):
        assert_is_type(value, bool)
        return self._set(computePValues=value)

    def setRemoveCollinearCols(self, value):
        assert_is_type(value, bool)
        return self._set(removeCollinearCols=value)

    def setInteractions(self, value):
        assert_is_type(value, None, [str])
        return self._set(interactions=value)

    def setInteractionPairs(self, value):
        assert_is_type(value, None, [(str, str)])
        return self._set(interactionPairs=value)

    def setEarlyStopping(self, value):
        assert_is_type(value, bool)
        return self._set(earlyStopping=value)
