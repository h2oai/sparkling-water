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

from ai.h2o.sparkling.ml.params.H2OAlgoSupervisedParams import H2OAlgoSupervisedParams
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from h2o.utils.typechecks import assert_is_type
from pyspark.ml.param import *
import warnings

class H2OGLMParams(H2OAlgoSupervisedParams):
    ##
    # Param definitions
    ##
    standardize = Param(
        Params._dummy(),
        "standardize",
        "standardize",
        H2OTypeConverters.toBoolean())

    family = Param(
        Params._dummy(),
        "family",
        "family",
        H2OTypeConverters.toEnumString("hex.glm.GLMModel$GLMParameters$Family"))

    link = Param(
        Params._dummy(),
        "link",
        "link",
        H2OTypeConverters.toEnumString("hex.glm.GLMModel$GLMParameters$Link"))

    solver = Param(
        Params._dummy(),
        "solver",
        "solver",
        H2OTypeConverters.toEnumString("hex.glm.GLMModel$GLMParameters$Solver"))

    tweedieVariancePower = Param(
        Params._dummy(),
        "tweedieVariancePower",
        "Tweedie variance power",
        H2OTypeConverters.toFloat())

    tweedieLinkPower = Param(
        Params._dummy(),
        "tweedieLinkPower",
        "Tweedie link power",
        H2OTypeConverters.toFloat())

    alphaValue = Param(
        Params._dummy(),
        "alphaValue",
        "alphaValue",
        H2OTypeConverters.toNullableListFloat())

    lambdaValue = Param(
        Params._dummy(),
        "lambdaValue",
        "lambdaValue",
        H2OTypeConverters.toNullableListFloat())

    missingValuesHandling = Param(
        Params._dummy(),
        "missingValuesHandling",
        "missingValuesHandling",
        H2OTypeConverters.toEnumString(
            "hex.deeplearning.DeepLearningModel$DeepLearningParameters$MissingValuesHandling"))

    prior = Param(
        Params._dummy(),
        "prior",
        "prior",
        H2OTypeConverters.toFloat())

    lambdaSearch = Param(
        Params._dummy(),
        "lambdaSearch",
        "lambda search",
        H2OTypeConverters.toBoolean())

    nlambdas = Param(
        Params._dummy(),
        "nlambdas",
        "nlambdas",
        H2OTypeConverters.toInt())

    nonNegative = Param(
        Params._dummy(),
        "nonNegative",
        "nonNegative",
        H2OTypeConverters.toBoolean())

    lambdaMinRatio = Param(
        Params._dummy(),
        "lambdaMinRatio",
        "lambdaMinRatio",
        H2OTypeConverters.toFloat())

    maxIterations = Param(
        Params._dummy(),
        "maxIterations",
        "maxIterations",
        H2OTypeConverters.toInt())

    intercept = Param(
        Params._dummy(),
        "intercept",
        "intercept",
        H2OTypeConverters.toBoolean())

    betaEpsilon = Param(
        Params._dummy(),
        "betaEpsilon",
        "betaEpsilon",
        H2OTypeConverters.toFloat())

    objectiveEpsilon = Param(
        Params._dummy(),
        "objectiveEpsilon",
        "objectiveEpsilon",
        H2OTypeConverters.toFloat())

    gradientEpsilon = Param(
        Params._dummy(),
        "gradientEpsilon",
        "gradientEpsilon",
        H2OTypeConverters.toFloat())

    objReg = Param(
        Params._dummy(),
        "objReg",
        "objReg",
        H2OTypeConverters.toFloat())

    computePValues = Param(
        Params._dummy(),
        "computePValues",
        "computePValues",
        H2OTypeConverters.toBoolean())

    removeCollinearCols = Param(
        Params._dummy(),
        "removeCollinearCols",
        "removeCollinearCols",
        H2OTypeConverters.toBoolean())

    interactions = Param(
        Params._dummy(),
        "interactions",
        "interactions",
        H2OTypeConverters.toNullableListString())

    interactionPairs = Param(
        Params._dummy(),
        "interactionPairs",
        "interactionPairs")

    earlyStopping = Param(
        Params._dummy(),
        "earlyStopping",
        "earlyStopping",
        H2OTypeConverters.toBoolean())

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

    def getAlphaValue(self):
        return self.getOrDefault(self.alphaValue)

    def getLambdaValue(self):
        return self.getOrDefault(self.lambdaValue)

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
        warnings.warn("Method 'getExactLambdas' is deprecated and will be removed in the next major release.")
        return False

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
        return self._set(standardize=value)

    def setFamily(self, value):
        return self._set(family=value)

    def setLink(self, value):
        return self._set(link=value)

    def setSolver(self, value):
        return self._set(solver=value)

    def setTweedieVariancePower(self, value):
        return self._set(tweedieVariancePower=value)

    def setTweedieLinkPower(self, value):
        return self._set(tweedieLinkPower=value)

    def setAlphaValue(self, value):
        return self._set(alphaValue=value)

    def setLambdaValue(self, value):
        return self._set(lambdaValue=value)

    def setMissingValuesHandling(self, value):
        return self._set(missingValuesHandling=value)

    def setPrior(self, value):
        return self._set(prior=value)

    def setLambdaSearch(self, value):
        return self._set(lambdaSearch=value)

    def setNlambdas(self, value):
        return self._set(nlambdas=value)

    def setNonNegative(self, value):
        return self._set(nonNegative=value)

    def setExactLambdas(self, value):
        warnings.warn("Method 'setExactLambdas' is deprecated and will be removed in the next major release.")
        return self

    def setLambdaMinRatio(self, value):
        return self._set(lambdaMinRatio=value)

    def setMaxIterations(self, value):
        return self._set(maxIterations=value)

    def setIntercept(self, value):
        return self._set(intercept=value)

    def setBetaEpsilon(self, value):
        return self._set(betaEpsilon=value)

    def setObjectiveEpsilon(self, value):
        return self._set(objectiveEpsilon=value)

    def setGradientEpsilon(self, value):
        return self._set(gradientEpsilon=value)

    def setObjReg(self, value):
        return self._set(objReg=value)

    def setComputePValues(self, value):
        return self._set(computePValues=value)

    def setRemoveCollinearCols(self, value):
        return self._set(removeCollinearCols=value)

    def setInteractions(self, value):
        return self._set(interactions=value)

    def setInteractionPairs(self, value):
        assert_is_type(value, None, [(str, str)])
        return self._set(interactionPairs=value)

    def setEarlyStopping(self, value):
        return self._set(earlyStopping=value)
