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
from ai.h2o.sparkling.ml.params.HasQuantileAlpha import HasQuantileAlpha
from ai.h2o.sparkling.ml.params.HasStoppingCriteria import HasStoppingCriteria
from pyspark.ml.param import *


class H2ODeepLearningParams(H2OAlgoSupervisedParams, HasStoppingCriteria, HasQuantileAlpha):
    ##
    # Param definitions
    ##
    epochs = Param(
        Params._dummy(),
        "epochs",
        "The number of passes over the training dataset to be carried out",
        H2OTypeConverters.toFloat())

    l1 = Param(
        Params._dummy(),
        "l1",
        "A regularization method that constrains the absolute value of the weights and "
        "has the net effect of dropping some weights (setting them to zero) from a model "
        "to reduce complexity and avoid overfitting.",
        H2OTypeConverters.toFloat())

    l2 = Param(
        Params._dummy(),
        "l2",
        "A regularization method that constrains the sum of the squared weights. "
        "This method introduces bias into parameter estimates, but frequently "
        "produces substantial gains in modeling as estimate variance is reduced.",
        H2OTypeConverters.toFloat())

    hidden = Param(
        Params._dummy(),
        "hidden",
        "The number and size of each hidden layer in the model",
        H2OTypeConverters.toListInt())

    reproducible = Param(
        Params._dummy(),
        "reproducible",
        "Force reproducibility on small data (will be slow - only uses 1 thread)",
        H2OTypeConverters.toBoolean())

    activation = Param(
        Params._dummy(),
        "activation",
        "The activation function (non-linearity) applied on neurons of hidden layers.",
        H2OTypeConverters.toEnumString("hex.deeplearning.DeepLearningModel$DeepLearningParameters$Activation"))

    inputDropoutRatio = Param(
        Params._dummy(),
        "inputDropoutRatio",
        "Input layer dropout ratio (can improve generalization, try 0.1 or 0.2).",
        H2OTypeConverters.toFloat())

    shuffleTrainingData = Param(
        Params._dummy(),
        "shuffleTrainingData",
        "Enable shuffling of training data (recommended if training data is replicated and train_samples_per_iteration"
        " is close to #nodes x #rows, of if using balance_classes).",
        H2OTypeConverters.toBoolean())

    rateDecay = Param(
        Params._dummy(),
        "rateDecay",
        "Learning rate decay factor between layers (N-th layer: rate * rate_decay ^ (n - 1).",
        H2OTypeConverters.toFloat())

    singleNodeMode = Param(
        Params._dummy(),
        "singleNodeMode",
        "Run on a single node for fine-tuning of model parameters.",
        H2OTypeConverters.toBoolean())

    ignoredCols = Param(
        Params._dummy(),
        "ignoredCols",
        "Names of columns to ignore for training.",
        H2OTypeConverters.toNullableListString())

    ignoreConstCols = Param(
        Params._dummy(),
        "ignoreConstCols",
        "Ignore constant columns.",
        H2OTypeConverters.toBoolean())

    hiddenDropoutRatios = Param(
        Params._dummy(),
        "hiddenDropoutRatios",
        "Hidden layer dropout ratios (can improve generalization), specify one value per hidden layer,"
        " defaults to 0.5.",
        H2OTypeConverters.toNullableListFloat())

    useAllFactorLevels = Param(
        Params._dummy(),
        "useAllFactorLevels",
        "Use all factor levels of categorical variables. Otherwise, the first factor level is omitted"
        " (without loss of accuracy). Useful for variable importances and auto-enabled for autoencoder.",
        H2OTypeConverters.toBoolean())

    missingValuesHandling = Param(
        Params._dummy(),
        "missingValuesHandling",
        "Handling of missing values. Either MeanImputation or Skip.",
        H2OTypeConverters.toEnumString(
            "hex.deeplearning.DeepLearningModel$DeepLearningParameters$MissingValuesHandling"))

    maxCategoricalFeatures = Param(
        Params._dummy(),
        "maxCategoricalFeatures",
        "Max. number of categorical features, enforced via hashing. #Experimental",
        H2OTypeConverters.toInt())

    fastMode = Param(
        Params._dummy(),
        "fastMode",
        "Enable fast mode (minor approximation in back-propagation).",
        H2OTypeConverters.toBoolean())

    sparse = Param(
        Params._dummy(),
        "sparse",
        "Sparse data handling (more efficient for data with lots of 0 values).",
        H2OTypeConverters.toBoolean())

    scoreTrainingSamples = Param(
        Params._dummy(),
        "scoreTrainingSamples",
        "Number of training set samples for scoring (0 for all).",
        H2OTypeConverters.toInt())

    adaptiveRate = Param(
        Params._dummy(),
        "adaptiveRate",
        "Number of training set samples for scoring (0 for all).",
        H2OTypeConverters.toBoolean())

    maxCategoricalLevels = Param(
        Params._dummy(),
        "maxCategoricalLevels",
        "For every categorical feature, only use this many most frequent categorical levels for model training. "
        "Only used for categorical_encoding == EnumLimited.",
        H2OTypeConverters.toInt())

    initialWeightScale = Param(
        Params._dummy(),
        "initialWeightScale",
        "Uniform: -value...value, Normal: stddev.",
        H2OTypeConverters.toFloat())

    customMetricFunc = Param(
        Params._dummy(),
        "customMetricFunc",
        "Reference to custom evaluation function, format: `language:keyName=funcName`",
        H2OTypeConverters.toNullableString())

    autoencoder = Param(
        Params._dummy(),
        "autoencoder",
        "autoencoder",
        H2OTypeConverters.toBoolean())

    classificationStop = Param(
        Params._dummy(),
        "classificationStop",
        "Stopping criterion for classification error fraction on training data (-1 to disable).",
        H2OTypeConverters.toFloat())

    standardize = Param(
        Params._dummy(),
        "standardize",
        "If enabled, automatically standardize the data. "
        "If disabled, the user must provide properly scaled input data.",
        H2OTypeConverters.toBoolean())

    targetRatioCommToComp = Param(
        Params._dummy(),
        "targetRatioCommToComp",
        "Target ratio of communication overhead to computation. "
        "Only for multi-node operation and train_samples_per_iteration = -2 (auto-tuning).",
        H2OTypeConverters.toFloat())

    classSamplingFactors = Param(
        Params._dummy(),
        "classSamplingFactors",
        "Desired over/under-sampling ratios per class (in lexicographic order). If not specified, sampling factors "
        "will be automatically computed to obtain class balance during training. Requires balance_classes.",
        H2OTypeConverters.toNullableListFloat())

    elasticAveragingMovingRate = Param(
        Params._dummy(),
        "elasticAveragingMovingRate",
        "Elastic averaging moving rate (only if elastic averaging is enabled).",
        H2OTypeConverters.toFloat())

    quietMode = Param(
        Params._dummy(),
        "quietMode",
        "Enable quiet mode for less output to standard output.",
        H2OTypeConverters.toBoolean())

    scoreValidationSampling = Param(
        Params._dummy(),
        "scoreValidationSampling",
        "Method used to sample validation dataset for scoring.",
        H2OTypeConverters.toEnumString("hex.deeplearning.DeepLearningModel$DeepLearningParameters$Uniform"))

    rate = Param(
        Params._dummy(),
        "rate",
        "Learning rate (higher => less stable, lower => slower convergence).",
        H2OTypeConverters.toFloat())

    ##
    # Getters
    ##
    def getEpochs(self):
        return self.getOrDefault(self.epochs)

    def getL1(self):
        return self.getOrDefault(self.l1)

    def getL2(self):
        return self.getOrDefault(self.l2)

    def getHidden(self):
        return self.getOrDefault(self.hidden)

    def getReproducible(self):
        return self.getOrDefault(self.reproducible)

    def getActivation(self):
        return self.getOrDefault(self.activation)

    def getInputDropoutRatio(self):
        return self.getOrDefault(self.inputDropoutRatio)

    def getShuffleTrainingData(self):
        return self.getOrDefault(self.shuffleTrainingData)

    def getRateDecay(self):
        return self.getOrDefault(self.rateDecay)

    def getSingleNodeMode(self):
        return self.getOrDefault(self.singleNodeMode)

    def getIgnoredCols(self):
        return self.getOrDefault(self.ignoredCols)

    def getHiddenDropoutRatios(self):
        return self.getOrDefault(self.hiddenDropoutRatios)

    def getUseAllFactorLevels(self):
        return self.getOrDefault(self.useAllFactorLevels)

    def getMissingValuesHandling(self):
        return self.getOrDefault(self.missingValuesHandling)

    def getMaxCategoricalFeatures(self):
        return self.getOrDefault(self.maxCategoricalFeatures)

    def getIgnoreConstCols(self):
        return self.getOrDefault(self.ignoredConstCols)

    def getFastMode(self):
        return self.getOrDefault(self.fastMode)

    def getSparse(self):
        return self.getOrDefault(self.sparse)

    def getScoreTrainingSamples(self):
        return self.getOrDefault(self.scoreTrainingSamples)

    def getAdaptiveRate(self):
        return self.getOrDefault(self.adaptiveRate)

    def getMaxCategoricalLevels(self):
        return self.getOrDefault(self.maxCategoricalLevels)

    def getInitialWeightScale(self):
        return self.getOrDefault(self.initialWeightScale)

    def getCustomMetricFunc(self):
        return self.getOrDefault(self.customMetricFunc)

    def getAutoencoder(self):
        return self.getOrDefault(self.autoencoder)

    def getClassificationStop(self):
        return self.getOrDefault(self.classificationStop)

    def getStandardize(self):
        return self.getOrDefault(self.standardize)

    def getTargetRatioCommToComp(self):
        return self.getOrDefault(self.targetRatioCommToComp)

    def getClassSamplingFactors(self):
        return self.getOrDefault(self.classSamplingFactors)

    def getElasticAveragingMovingRate(self):
        return self.getOrDefault(self.elasticAveragingMovingRate)

    def getQuietMode(self):
        return self.getOrDefault(self.quietMode)

    def getScoreValidationSampling(self):
        return self.getOrDefault(self.scoreValidationSampling)

    def getRate(self):
        return self.getOrDefault(self.rate)

    ##
    # Setters
    ##
    def setEpochs(self, value):
        return self._set(epochs=value)

    def setL1(self, value):
        return self._set(l1=value)

    def setL2(self, value):
        return self._set(l2=value)

    def setHidden(self, value):
        return self._set(hidden=value)

    def setReproducible(self, value):
        return self._set(reproducible=value)

    def setActivation(self, value):
        return self._set(activation=value)

    def setInputDropoutRatio(self, value):
        return self._set(inputDropoutRatio=value)

    def setShuffleTrainingData(self, value):
        return self._set(shuffleTrainingData=value)

    def setRateDecay(self, value):
        return self._set(rateDecay=value)

    def setSingleNodeMode(self, value):
        return self._set(singleNodeMode=value)

    def setIgnoredCols(self, value):
        return self._set(ignoredCols=value)

    def setHiddenDropoutRatios(self, value):
        return self._set(hiddenDropoutRatios=value)

    def setUseAllFactorLevels(self, value):
        return self._set(useAllFactorLevels=value)

    def setMissingValuesHandling(self, value):
        return self._set(missingValuesHandling=value)

    def setMaxCategoricalFeatures(self, value):
        return self._set(maxCategoricalFeatures=value)

    def setIgnoreConstCols(self, value):
        return self._set(ignoreConstCols=value)

    def setFastMode(self, value):
        return self._set(fastMode=value)

    def setSparse(self, value):
        return self._set(sparse=value)

    def setScoreTrainingSamples(self, value):
        return self._set(scoreTrainingSamples=value)

    def setAdaptiveRate(self, value):
        return self._set(adaptiveRate=value)

    def setMaxCategoricalLevels(self, value):
        return self._set(maxCategoricalLevels=value)

    def setInitialWeightScale(self, value):
        return self._set(initialWeightScale=value)

    def setCustomMetricFunc(self, value):
        return self._set(customMetricFunc=value)

    def setAutoencoder(self, value):
        return self._set(autoencoder=value)

    def setClassificationStop(self, value):
        return self._set(classificationStop=value)

    def setStandardize(self, value):
        return self._set(standardize=value)

    def setTargetRatioCommToComp(self, value):
        return self._set(targetRatioCommToComp=value)

    def setClassSamplingFactors(self, value):
        return self._set(classSamplingFactors=value)

    def setElasticAveragingMovingRate(self, value):
        return self._set(elasticAveragingMovingRate=value)

    def setQuiteMode(self, value):
        return self._set(quiteMode=value)

    def setScoreValidationSampling(self, value):
        return self._set(scoreValidationSampling=value)

    def setRate(self, value):
        return self._set(rate=value)
