from pyspark.ml.param import *
from h2o.utils.typechecks import assert_is_type, Enum
from pysparkling.context import H2OContext
from pyspark.sql import SparkSession
from py4j.java_gateway import JavaObject
from py_sparkling.ml.util import get_correct_case_enum, get_enum_array_from_str_array
import warnings

class H2OAlgorithmParams(Params):
    ##
    # Param definitions
    ##
    ratio = Param(Params._dummy(), "ratio", "Ration of frame which is used for training")
    labelCol = Param(Params._dummy(), "labelCol", "Label column name")
    weightCol = Param(Params._dummy(), "weightCol", "Weight column name")
    featuresCols = Param(Params._dummy(), "featuresCols", "columns used as features")
    allStringColumnsToCategorical = Param(Params._dummy(), "allStringColumnsToCategorical", "Transform all strings columns to categorical")
    columnsToCategorical = Param(Params._dummy(), "columnsToCategorical", "List of columns to convert to categoricals before modelling")
    nfolds = Param(Params._dummy(), "nfolds", "Number of folds for K-fold cross-validation (0 to disable or >= 2)")
    foldCol = Param(Params._dummy(), "foldCol", "Fold column name")
    keepCrossValidationPredictions = Param(Params._dummy(), "keepCrossValidationPredictions", "Whether to keep the predictions of the cross-validation models")
    keepCrossValidationFoldAssignment = Param(Params._dummy(), "keepCrossValidationFoldAssignment", "Whether to keep the cross-validation fold assignment")
    parallelizeCrossValidation = Param(Params._dummy(), "parallelizeCrossValidation", "Allow parallel training of cross-validation models")
    seed = Param(Params._dummy(), "seed", "Seed for random numbers (affects sampling) - Note: only reproducible when running single threaded.")
    distribution = Param(Params._dummy(), "distribution", "Distribution function")
    convertUnknownCategoricalLevelsToNa = Param(Params._dummy(), "convertUnknownCategoricalLevelsToNa", "If set to 'true', the model converts unknown categorical levels to NA during making predictions.")

    ##
    # Getters
    ##
    def getRatio(self):
        return self.getOrDefault(self.ratio)

    def getPredictionCol(self):
        warnings.warn("The method 'getPredictionCol' is deprecated. Use 'getLabelCol' instead!")
        return self.getLabelCol()

    def getLabelCol(self):
        return self.getOrDefault(self.labelCol)

    def getWeightCol(self):
        return self.getOrDefault(self.weightCol)

    def getFeaturesCols(self):
        return self.getOrDefault(self.featuresCols)

    def getAllStringColumnsToCategorical(self):
        return self.getOrDefault(self.allStringColumnsToCategorical)

    def getColumnsToCategorical(self):
        return self.getOrDefault(self.columnsToCategorical)

    def getNfolds(self):
        return self.getOrDefault(self.nfolds)

    def getFoldCol(self):
        return self.getOrDefault(self.foldCol)

    def getKeepCrossValidationPredictions(self):
        return self.getOrDefault(self.keepCrossValidationPredictions)

    def getKeepCrossValidationFoldAssignment(self):
        return self.getOrDefault(self.keepCrossValidationFoldAssignment)

    def getParallelizeCrossValidation(self):
        return self.getOrDefault(self.parallelizeCrossValidation)

    def getSeed(self):
        return self.getOrDefault(self.seed)

    def getDistribution(self):
        # Convert Java Enum to String so we can represent it in Python
        return self.getOrDefault(self.distribution).toString()

    def getConvertUnknownCategoricalLevelsToNa(self):
        return self.getOrDefault(self.convertUnknownCategoricalLevelsToNa)

    ##
    # Setters
    ##
    def setRatio(self, value):
        assert_is_type(value, int, float)
        return self._set(ratio=value)

    def setPredictionCol(self, value):
        warnings.warn("The method 'setPredictionCol' is deprecated. Use 'setLabelCol' instead!")
        return self.setLabelCol(value)

    def setLabelCol(self, value):
        assert_is_type(value, str)
        return self._set(labelCol=value)

    def setWeightCol(self, value):
        assert_is_type(value, str)
        return self._set(weightCol=value)

    def setFeaturesCols(self, value):
        assert_is_type(value, [str])
        return self._set(featuresCols=value)

    def setAllStringColumnsToCategorical(self, value):
        assert_is_type(value, bool)
        return self._set(allStringColumnsToCategorical=value)

    def setColumnsToCategorical(self, value):
        assert_is_type(value, [str])
        return self._set(columnsToCategorical=value)

    def setNfolds(self, value):
        assert_is_type(value, int)
        return self._set(nfolds=value)

    def setFoldCol(self, value):
        assert_is_type(value, str, None)
        return self._set(foldCol=value)

    def setKeepCrossValidationPredictions(self, value):
        assert_is_type(value, bool)
        return self._set(keepCrossValidationPredictions=value)

    def setKeepCrossValidationFoldAssignment(self, value):
        assert_is_type(value, bool)
        return self._set(keepCrossValidationFoldAssignment=value)

    def setParallelizeCrossValidation(self, value):
        assert_is_type(value, bool)
        return self._set(parallelizeCrossValidation=value)

    def setSeed(self, value):
        assert_is_type(value, int)
        return self._set(seed=value)

    def setDistribution(self, value):
        assert_is_type(value, None, Enum("AUTO", "bernoulli", "quasibinomial", "modified_huber", "multinomial", "ordinal", "gaussian", "poisson", "gamma", "tweedie", "huber", "laplace", "quantile"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.genmodel.utils.DistributionFamily.values(), value)
        return self._set(distribution=jvm.hex.genmodel.utils.DistributionFamily.valueOf(correct_case_value))

    def setConvertUnknownCategoricalLevelsToNa(self, value):
        assert_is_type(value, bool)
        return self._set(convertUnknownCategoricalLevelsToNa=value)


class H2OSharedTreeParams(H2OAlgorithmParams):

    ##
    # Param definitions
    ##
    ntrees = Param(Params._dummy(), "ntrees", "Number of trees")
    maxDepth = Param(Params._dummy(), "maxDepth", "Maximum tree depth")
    minRows = Param(Params._dummy(), "minRows", "Fewest allowed (weighted) observations in a leaf")
    nbins = Param(Params._dummy(), "nbins", "For numerical columns (real/int), build a histogram of (at least) this many bins, then split at the best point")
    nbinsCats = Param(Params._dummy(), "nbinsCats", "For categorical columns (factors), build a histogram of this many bins, then split at the best point. Higher values can lead to more overfitting")
    minSplitImprovement = Param(Params._dummy(), "minSplitImprovement", "Minimum relative improvement in squared error reduction for a split to happen")
    histogramType = Param(Params._dummy(), "histogramType", "What type of histogram to use for finding optimal split points")
    r2Stopping = Param(Params._dummy(), "r2Stopping", "r2_stopping is no longer supported and will be ignored if set - please use stopping_rounds, stopping_metric and stopping_tolerance instead. Previous version of H2O would stop making trees when the R^2 metric equals or exceeds this")
    nbinsTopLevel = Param(Params._dummy(), "nbinsTopLevel", "For numerical columns (real/int), build a histogram of (at most) this many bins at the root level, then decrease by factor of two per level")
    buildTreeOneNode = Param(Params._dummy(), "buildTreeOneNode", "Run on one node only; no network overhead but fewer cpus used.  Suitable for small datasets.")
    scoreTreeInterval = Param(Params._dummy(), "scoreTreeInterval", "Score the model after every so many trees. Disabled if set to 0.")
    sampleRate = Param(Params._dummy(), "sampleRate", "Row sample rate per tree (from 0.0 to 1.0)")
    sampleRatePerClass = Param(Params._dummy(), "sampleRatePerClass", "A list of row sample rates per class (relative fraction for each class, from 0.0 to 1.0), for each tree")
    colSampleRateChangePerLevel = Param(Params._dummy(), "colSampleRateChangePerLevel", "Relative change of the column sampling rate for every level (from 0.0 to 2.0)")
    colSampleRatePerTree = Param(Params._dummy(), "colSampleRatePerTree", "Column sample rate per tree (from 0.0 to 1.0)")

    ##
    # Getters
    ##
    def getNtrees(self):
        return self.getOrDefault(self.ntrees)
    
    def getMaxDepth(self):
        return self.getOrDefault(self.maxDepth)
    
    def getMinRows(self):
        return self.getOrDefault(self.minRows)
    
    def getNbins(self):
        return self.getOrDefault(self.nbins)
    
    def getNbinsCats(self):
        return self.getOrDefault(self.nbinsCats)
    
    def getMinSplitImprovement(self):
        return self.getOrDefault(self.minSplitImprovement)
    
    def getHistogramType(self):
        # Convert Java Enum to String so we can represent it in Python
        return self.getOrDefault(self.histogramType).toString()
    
    def getR2Stopping(self):
        return self.getOrDefault(self.r2Stopping)

    def getNbinsTopLevel(self):
        return self.getOrDefault(self.nbinsTopLevel)

    def getBuildTreeOneNode(self):
        return self.getOrDefault(self.buildTreeOneNode)

    def getScoreTreeInterval(self):
        return self.getOrDefault(self.scoreTreeInterval)

    def getSampleRate(self):
        return self.getOrDefault(self.sampleRate)

    def getSampleRatePerClass(self):
        return self.getOrDefault(self.sampleRatePerClass)

    def getColSampleRateChangePerLevel(self):
        return self.getOrDefault(self.colSampleRateChangePerLevel)

    def getColSampleRatePerTree(self):
        return self.getOrDefault(self.colSampleRatePerTree)

    ##
    # Setters
    ##
    def setNtrees(self, value):
        assert_is_type(value, int)
        return self._set(ntrees=value)

    def setMaxDepth(self, value):
        assert_is_type(value, int)
        return self._set(maxDepth=value)

    def setMinRows(self, value):
        assert_is_type(value, int, float)
        return self._set(minRows=float(value))

    def setNbins(self, value):
        assert_is_type(value, int)
        return self._set(nbins=value)

    def setNbinsCats(self, value):
        assert_is_type(value, int)
        return self._set(nbinsCats=value)

    def setMinSplitImprovement(self, value):
        assert_is_type(value, int, float)
        return self._set(minSplitImprovement=value)

    def setHistogramType(self, value):
        assert_is_type(value, None, Enum("AUTO", "UniformAdaptive", "Random", "QuantilesGlobal", "RoundRobin"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.tree.SharedTreeModel.SharedTreeParameters.HistogramType.values(), value)
        return self._set(histogramType=jvm.hex.tree.SharedTreeModel.SharedTreeParameters.HistogramType.valueOf(correct_case_value))

    def setR2Stopping(self, value):
        assert_is_type(value, int, float)
        return self._set(r2Stopping=value)

    def setNbinsTopLevel(self, value):
        assert_is_type(value, int, float)
        return self._set(nbinsTopLevel=value)

    def setBuildTreeOneNode(self, value):
        assert_is_type(value, bool)
        return self._set(buildTreeOneNode=value)

    def setScoreTreeInterval(self, value):
        assert_is_type(value, int)
        return self._set(scoreTreeInterval=value)

    def setSampleRate(self, value):
        assert_is_type(value, int, float)
        return self._set(sampleRate=value)

    def setSampleRatePerClass(self, value):
        assert_is_type(value, None, [int, float])
        return self._set(sampleRatePerClass=value)

    def setColSampleRateChangePerLevel(self, value):
        assert_is_type(value, int, float)
        return self._set(colSampleRateChangePerLevel=value)

    def setColSampleRatePerTree(self, value):
        assert_is_type(value, int, float)
        return self._set(colSampleRatePerTree=value)


class H2OGBMParams(H2OSharedTreeParams):

    ##
    # Param definitions
    ##
    learnRate = Param(Params._dummy(), "learnRate", "Learning rate (from 0.0 to 1.0)")
    learnRateAnnealing = Param(Params._dummy(), "learnRateAnnealing", "Scale the learning rate by this factor after each tree (e.g., 0.99 or 0.999)")
    colSampleRate = Param(Params._dummy(), "colSampleRate", "Column sample rate (from 0.0 to 1.0)")
    maxAbsLeafnodePred = Param(Params._dummy(), "maxAbsLeafnodePred", "Maximum absolute value of a leaf node prediction")
    predNoiseBandwidth = Param(Params._dummy(), "predNoiseBandwidth", "Bandwidth (sigma) of Gaussian multiplicative noise ~N(1,sigma) for tree node predictions")

    ##
    # Getters
    ##
    def getLearnRate(self):
        return self.getOrDefault(self.learnRate)

    def getLearnRateAnnealing(self):
        return self.getOrDefault(self.learnRateAnnealing)

    def getColSampleRate(self):
        return self.getOrDefault(self.colSampleRate)

    def getMaxAbsLeafnodePred(self):
        return self.getOrDefault(self.maxAbsLeafnodePred)

    def getPredNoiseBandwidth(self):
        return self.getOrDefault(self.predNoiseBandwidth)

    ##
    # Setters
    ##
    def setLearnRate(self, value):
        assert_is_type(value, int, float)
        return self._set(learnRate=value)

    def setLearnRateAnnealing(self, value):
        assert_is_type(value, int, float)
        return self._set(learnRateAnnealing=value)

    def setColSampleRate(self, value):
        assert_is_type(value, int, float)
        return self._set(colSampleRate=value)

    def setMaxAbsLeafnodePred(self, value):
        assert_is_type(value, int, float)
        return self._set(maxAbsLeafnodePred=value)

    def setPredNoiseBandwidth(self, value):
        assert_is_type(value, int, float)
        return self._set(predNoiseBandwidth=value)


class H2ODeepLearningParams(H2OAlgorithmParams):

    ##
    # Param definitions
    ##
    epochs = Param(Params._dummy(), "epochs", "The number of passes over the training dataset to be carried out")

    l1 = Param(Params._dummy(), "l1", "A regularization method that constrains the absolute value of the weights and"
                                      " has the net effect of dropping some weights (setting them to zero) from"
                                      " a model to reduce complexity and avoid overfitting.")

    l2 = Param(Params._dummy(), "l2", "A regularization method that constrains the sum of the squared weights."
                                      " This method introduces bias into parameter estimates, but frequently"
                                      " produces substantial gains in modeling as estimate variance is reduced.")

    hidden = Param(Params._dummy(), "hidden", "The number and size of each hidden layer in the model")
    reproducible = Param(Params._dummy(), "reproducible", "Force reproducibility on small data (will be slow - only uses 1 thread)")

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

    ##
    # Setters
    ##
    def setEpochs(self, value):
        assert_is_type(value, int, float)
        return self._set(epochs=value)

    def setL1(self, value):
        assert_is_type(value, int, float)
        return self._set(l1=value)

    def setL2(self, value):
        assert_is_type(value, int, float)
        return self._set(l2=value)

    def setHidden(self, value):
        assert_is_type(value, [int])
        return self._set(hidden=value)

    def setReproducible(self, value):
        assert_is_type(value, bool)
        return self._set(reproducible=value)


class H2OAutoMLParams(Params):

    ##
    # Param definitions
    ##
    featuresCols = Param(Params._dummy(), "featuresCols", "columns used as features")
    labelCol = Param(Params._dummy(), "labelCol", "Label column name")
    allStringColumnsToCategorical = Param(Params._dummy(), "allStringColumnsToCategorical", "Transform all strings columns to categorical")
    columnsToCategorical = Param(Params._dummy(), "columnsToCategorical", "List of columns to convert to categoricals before modelling")
    ratio = Param(Params._dummy(), "ratio", "Ration of frame which is used for training")
    foldCol = Param(Params._dummy(), "foldCol", "Fold column name")
    weightCol = Param(Params._dummy(), "weightCol", "Weight column name")
    ignoredCols = Param(Params._dummy(), "ignoredCols", "Ignored column names")
    includeAlgos = Param(Params._dummy(), "includeAlgos", "Algorithms to include when using automl")
    excludeAlgos = Param(Params._dummy(), "excludeAlgos", "Algorithms to exclude when using automl")
    projectName = Param(Params._dummy(), "projectName", "identifier for models that should be grouped together in the leaderboard" +
                        " (e.g., airlines and iris)")
    maxRuntimeSecs = Param(Params._dummy(), "maxRuntimeSecs", "Maximum time in seconds for automl to be running")
    stoppingRounds = Param(Params._dummy(), "stoppingRounds", "Stopping rounds")
    stoppingTolerance = Param(Params._dummy(), "stoppingTolerance", "Stopping tolerance")
    stoppingMetric = Param(Params._dummy(), "stoppingMetric", "Stopping metric")
    nfolds = Param(Params._dummy(), "nfolds", "Cross-validation fold construction")
    convertUnknownCategoricalLevelsToNa = Param(Params._dummy(), "convertUnknownCategoricalLevelsToNa", "If set to 'true', the model converts unknown categorical levels to NA during making predictions.")
    seed = Param(Params._dummy(), "seed", "Seed for random numbers")
    sortMetric = Param(Params._dummy(), "sortMetric", "Sort metric for the AutoML leaderboard")
    balanceClasses = Param(Params._dummy(), "balanceClasses", "Balance classes")
    classSamplingFactors = Param(Params._dummy(), "classSamplingFactors", "Class sampling factors")
    maxAfterBalanceSize = Param(Params._dummy(), "maxAfterBalanceSize", "Max after balance size")
    keepCrossValidationPredictions = Param(Params._dummy(), "keepCrossValidationPredictions", "Keep cross validation predictions")
    keepCrossValidationModels = Param(Params._dummy(), "keepCrossValidationModels", "Keep cross validation models")
    maxModels = Param(Params._dummy(), "maxModels", "Max models to train in AutoML")
    ##
    # Getters
    ##
    def getFeaturesCols(self):
        return self.getOrDefault(self.featuresCols)

    def getPredictionCol(self):
        warnings.warn("The method 'getPredictionCol' is deprecated. Use 'getLabelCol' instead!")
        return self.getLabelCol()

    def getLabelCol(self):
        return self.getOrDefault(self.labelCol)

    def getAllStringColumnsToCategorical(self):
        return self.getOrDefault(self.allStringColumnsToCategorical)

    def getColumnsToCategorical(self):
        return self.getOrDefault(self.columnsToCategorical)

    def getRatio(self):
        return self.getOrDefault(self.ratio)

    def getFoldCol(self):
        return self.getOrDefault(self.foldCol)

    def getFoldColumn(self):
        warnings.warn("The method 'getFoldColumn' is deprecated. Use 'getFoldCol' instead!")
        return self.getFoldCol()

    def getWeightCol(self):
        return self.getOrDefault(self.weightCol)

    def getWeightsColumn(self):
        warnings.warn("The method 'getWeightsColumn' is deprecated. Use 'getWeightCol' instead!")
        return self.getWeightCol()

    def getIgnoredCols(self):
        return self.getOrDefault(self.ignoredCols)

    def getIgnoredColumns(self):
        warnings.warn("The method 'getIgnoredColumns' is deprecated. Use 'getIgnoredCols' instead!")
        return self.getIgnoredCols()

    def getTryMutations(self):
        return self.getOrDefault(self.tryMutations)

    def getExcludeAlgos(self):
        # Convert Java Enum to String so we can represent it in Python
        algos = self.getOrDefault(self.excludeAlgos)
        algos_str = []
        if algos is not None:
            for a in algos:
                algos_str.append(a)
        return algos_str

    def getIncludeAlgos(self):
        # Convert Java Enum to String so we can represent it in Python
        algos = self.getOrDefault(self.includeAlgos)
        algos_str = []
        if algos is not None:
            for a in algos:
                algos_str.append(a)
        return algos_str

    def getProjectName(self):
        return self.getOrDefault(self.projectName)

    def getLoss(self):
        return self.getOrDefault(self.loss)

    def getMaxRuntimeSecs(self):
        return self.getOrDefault(self.maxRuntimeSecs)

    def getStoppingRounds(self):
        return self.getOrDefault(self.stoppingRounds)

    def getStoppingTolerance(self):
        return self.getOrDefault(self.stoppingTolerance)

    def getStoppingMetric(self):
        # Convert Java Enum to String so we can represent it in Python
        return self.getOrDefault(self.stoppingMetric).toString()

    def getNfolds(self):
        return self.getOrDefault(self.nfolds)

    def getConvertUnknownCategoricalLevelsToNa(self):
        return self.getOrDefault(self.convertUnknownCategoricalLevelsToNa)

    def getSeed(self):
        return self.getOrDefault(self.seed)

    def getSortMetric(self):
        metric = self.getOrDefault(self.sortMetric)
        if metric is None:
            return "AUTO"
        else:
            return metric

    def getBalanceClasses(self):
        return self.getOrDefault(self.balanceClasses)

    def getClassSamplingFactors(self):
        return self.getOrDefault(self.classSamplingFactors)

    def getMaxAfterBalanceSize(self):
        return self.getOrDefault(self.maxAfterBalanceSize)

    def getKeepCrossValidationPredictions(self):
        return self.getOrDefault(self.keepCrossValidationPredictions)

    def getKeepCrossValidationModels(self):
        return self.getOrDefault(self.keepCrossValidationModels)

    def getMaxModels(self):
        return self.getOrDefault(self.maxModels)

    ##
    # Setters
    ##
    def setFeaturesCols(self, value):
        assert_is_type(value, [str])
        return self._set(featuresCols=value)

    def setPredictionCol(self, value):
        warnings.warn("The method 'setPredictionCol' is deprecated. Use 'setLabelCol' instead!")
        return self.setLabelCol(value)

    def setLabelCol(self, value):
        assert_is_type(value, str)
        return self._set(labelCol=value)

    def setAllStringColumnsToCategorical(self, value):
        assert_is_type(value, bool)
        return self._set(allStringColumnsToCategorical=value)

    def setColumnsToCategorical(self, value):
        assert_is_type(value, [str])
        return self._set(columnsToCategorical=value)

    def setRatio(self, value):
        assert_is_type(value, int, float)
        return self._set(ratio=value)

    def setFoldCol(self, value):
        assert_is_type(value, None, str)
        return self._set(foldCol=value)

    def setFoldColumn(self, value):
        warnings.warn("The method 'setFoldColumn' is deprecated. Use 'setFoldCol' instead!")
        return self.setFoldCol(value)

    def setWeightCol(self, value):
        assert_is_type(value, None, str)
        return self._set(weightCol=value)

    def setWeightsColumn(self, value):
        warnings.warn("The method 'setWeightsColumn' is deprecated. Use 'setWeightCol' instead!")
        return self.setWeightCol(value)

    def setIgnoredCols(self, value):
        assert_is_type(value, [str])
        return self._set(ignoredCols=value)

    def setIgnoredColumns(self, value):
        warnings.warn("The method 'setIgnoredColumns' is deprecated. Use 'setIgnoredCols' instead!")
        return self.setIgnoredCols(value)

    def setTryMutations(self, value):
        assert_is_type(value, bool)
        return self._set(tryMutations=value)

    def setIncludeAlgos(self, value):
        assert_is_type(value, None, [Enum("XGBoost", "GLM", "DRF", "GBM", "DeepLearning", "StackedEnsemble")])
        # H2O typechecks does not check for case sensitivity
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        java_enums = get_enum_array_from_str_array(value, jvm.ai.h2o.automl.Algo)
        return self._set(includeAlgos=java_enums)

    def setExcludeAlgos(self, value):
        assert_is_type(value, None, [Enum("XGBoost", "GLM", "DRF", "GBM", "DeepLearning", "StackedEnsemble")])
        # H2O typechecks does not check for case sensitivity
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        java_enums = get_enum_array_from_str_array(value, jvm.ai.h2o.automl.Algo)
        return self._set(excludeAlgos=java_enums)

    def setProjectName(self, value):
        assert_is_type(value, None, str)
        return self._set(projectName=value)

    def setLoss(self, value):
        assert_is_type(value, "AUTO")
        return self._set(loss=value)

    def setMaxRuntimeSecs(self, value):
        assert_is_type(value, int, float)
        return self._set(maxRuntimeSecs=value)

    def setStoppingRounds(self, value):
        assert_is_type(value, int)
        return self._set(stoppingRounds=value)

    def setStoppingTolerance(self, value):
        assert_is_type(value, int, float)
        return self._set(stoppingTolerance=value)

    def setStoppingMetric(self, value):
        # H2O typechecks does not check for case sensitivity
        assert_is_type(value, Enum("AUTO", "deviance", "logloss", "MSE", "RMSE", "MAE", "RMSLE", "AUC", "lift_top_group", "misclassification", "mean_per_class_error", "custom"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.ScoreKeeper.StoppingMetric.values(), value)
        return self._set(stoppingMetric=jvm.hex.ScoreKeeper.StoppingMetric.valueOf(correct_case_value))

    def setNfolds(self, value):
        assert_is_type(value, int)
        return self._set(nfolds=value)

    def setConvertUnknownCategoricalLevelsToNa(self, value):
        assert_is_type(value, bool)
        return self._set(convertUnknownCategoricalLevelsToNa=value)

    def setSeed(self, value):
        assert_is_type(value, int)
        return self._set(seed=value)

    def setSortMetric(self, value):
        assert_is_type(value, None, "AUTO", "deviance", "logloss", "MSE", "RMSE", "MAE", "RMSLE", "AUC" "mean_per_class_error")
        if value is "AUTO":
            return self._set(sortMetric=None)
        else:
            return self._set(sortMetric=value)

    def setBalanceClasses(self, value):
        assert_is_type(value, bool)
        return self._set(balanceClasses=value)

    def setClassSamplingFactors(self, value):
        assert_is_type(value, [int, float])
        return self._set(classSamplingFactors=value)

    def setMaxAfterBalanceSize(self, value):
        assert_is_type(value, int, float)
        return self._set(maxAfterBalanceSize=value)

    def setKeepCrossValidationPredictions(self, value):
        assert_is_type(value, bool)
        return self._set(keepCrossValidationPredictions=value)

    def setKeepCrossValidationModels(self, value):
        assert_is_type(value, bool)
        return self._set(keepCrossValidationModels=value)

    def setMaxModels(self, value):
        assert_is_type(value, int)
        return self._set(maxModels=value)


class H2OXGBoostParams(H2OAlgorithmParams):

    ##
    # Param definitions
    ##
    quietMode = Param(Params._dummy(), "quietMode", "Quiet mode")
    missingValuesHandling = Param(Params._dummy(), "missingValuesHandling", "Missing Values Handling")
    ntrees = Param(Params._dummy(), "ntrees", "Number of trees")
    nEstimators = Param(Params._dummy(), "nEstimators", "number of estimators")
    maxDepth = Param(Params._dummy(), "maxDepth", "Maximal depth")
    minRows = Param(Params._dummy(), "minRows", "Min rows")
    minChildWeight = Param(Params._dummy(), "minChildWeight", "minimal child weight")
    learnRate = Param(Params._dummy(), "learnRate", "learn rate")
    eta = Param(Params._dummy(), "eta", "eta")
    learnRateAnnealing = Param(Params._dummy(), "learnRateAnnealing", "Learn Rate Annealing")
    sampleRate = Param(Params._dummy(), "sampleRate", "Sample rate")
    subsample = Param(Params._dummy(), "subsample", "subsample")
    colSampleRate = Param(Params._dummy(), "colSampleRate", "col sample rate")
    colSampleByLevel = Param(Params._dummy(), "colSampleByLevel", "Col Sample By Level")
    colSampleRatePerTree = Param(Params._dummy(), "colSampleRatePerTree", "col samle rate")
    colsampleBytree = Param(Params._dummy(), "colsampleBytree", "col sample by tree")
    maxAbsLeafnodePred = Param(Params._dummy(), "maxAbsLeafnodePred", "max abs lead node prediction")
    maxDeltaStep = Param(Params._dummy(), "maxDeltaStep", "max delta step")
    scoreTreeInterval = Param(Params._dummy(), "scoreTreeInterval", "score tree interval")
    initialScoreInterval = Param(Params._dummy(), "initialScoreInterval", "Initial Score Interval")
    scoreInterval = Param(Params._dummy(), "scoreInterval", "Score Interval")
    minSplitImprovement = Param(Params._dummy(), "minSplitImprovement", "Min split improvement")
    gamma = Param(Params._dummy(), "gamma", "gamma")
    nthread = Param(Params._dummy(), "nthread", "nthread")
    maxBins = Param(Params._dummy(), "maxBins", "nbins")
    maxLeaves = Param(Params._dummy(), "maxLeaves", "max leaves")
    minSumHessianInLeaf = Param(Params._dummy(), "minSumHessianInLeaf", "min sum hessian in leaf")
    minDataInLeaf = Param(Params._dummy(), "minDataInLeaf", "min data in leaf")
    treeMethod = Param(Params._dummy(), "treeMethod", "Tree Method")
    growPolicy = Param(Params._dummy(), "growPolicy", "Grow Policy")
    booster = Param(Params._dummy(), "booster", "Booster")
    dmatrixType = Param(Params._dummy(), "dmatrixType", "DMatrix type")
    regLambda = Param(Params._dummy(), "regLambda", "req lambda")
    regAlpha = Param(Params._dummy(), "regAlpha", "req aplha")
    sampleType = Param(Params._dummy(), "sampleType", "Dart Sample Type")
    normalizeType = Param(Params._dummy(), "normalizeType", "Dart Normalize Type")
    rateDrop = Param(Params._dummy(), "rateDrop", "rate drop")
    oneDrop = Param(Params._dummy(), "oneDrop", "onde drop")
    skipDrop = Param(Params._dummy(), "skipDrop", "skip drop")
    gpuId = Param(Params._dummy(), "gpuId", "GPU id")
    backend = Param(Params._dummy(), "backend", "Backend")

    ##
    # Getters
    ##
    def getQuietMode(self):
        return self.getOrDefault(self.quietMode)

    def getMissingValuesHandling(self):
        res = self.getOrDefault(self.missingValuesHandling)
        if res is not None:
            return res.toString()
        else:
            return None

    def getNtrees(self):
        return self.getOrDefault(self.ntrees)

    def getNEstimators(self):
        return self.getOrDefault(self.nEstimators)

    def getMaxDepth(self):
        return self.getOrDefault(self.maxDepth)

    def getMinRows(self):
        return self.getOrDefault(self.minRows)

    def getMinChildWeight(self):
        return self.getOrDefault(self.minChildWeight)

    def getLearnRate(self):
        return self.getOrDefault(self.learnRate)

    def getEta(self):
        return self.getOrDefault(self.eta)

    def getLearnRateAnnealing(self):
        return self.getOrDefault(self.learnRateAnnealing)

    def getSampleRate(self):
        return self.getOrDefault(self.sampleRate)

    def getSubsample(self):
        return self.getOrDefault(self.subsample)

    def getColSampleRate(self):
        return self.getOrDefault(self.colSampleRate)

    def getColSampleByLevel(self):
        return self.getOrDefault(self.colSampleByLevel)

    def getColSampleRatePerTree(self):
        return self.getOrDefault(self.colSampleRatePerTree)

    def getColsampleBytree(self):
        return self.getOrDefault(self.colsampleBytree)

    def getMaxAbsLeafnodePred(self):
        return self.getOrDefault(self.maxAbsLeafnodePred)

    def getMaxDeltaStep(self):
        return self.getOrDefault(self.maxDeltaStep)

    def getScoreTreeInterval(self):
        return self.getOrDefault(self.scoreTreeInterval)

    def getInitialScoreInterval(self):
        return self.getOrDefault(self.initialScoreInterval)

    def getScoreInterval(self):
        return self.getOrDefault(self.scoreInterval)

    def getMinSplitImprovement(self):
        return self.getOrDefault(self.minSplitImprovement)

    def getGamma(self):
        return self.getOrDefault(self.gamma)

    def getNthread(self):
        return self.getOrDefault(self.nthread)

    def getMaxBins(self):
        return self.getOrDefault(self.maxBins)

    def getMaxLeaves(self):
        return self.getOrDefault(self.maxLeaves)

    def getMinSumHessianInLeaf(self):
        return self.getOrDefault(self.minSumHessianInLeaf)

    def getMinDataInLeaf(self):
        return self.getOrDefault(self.minDataInLeaf)

    def getTreeMethod(self):
        return self.getOrDefault(self.treeMethod).toString()

    def getGrowPolicy(self):
        return self.getOrDefault(self.growPolicy).toString()

    def getBooster(self):
        return self.getOrDefault(self.booster).toString()

    def getDmatrixType(self):
        return self.getOrDefault(self.dmatrixType).toString()

    def getRegLambda(self):
        return self.getOrDefault(self.regLambda)

    def getRegAlpha(self):
        return self.getOrDefault(self.regAlpha)

    def getSampleType(self):
        return self.getOrDefault(self.sampleType).toString()

    def getNormalizeType(self):
        return self.getOrDefault(self.normalizeType).toString()

    def getRateDrop(self):
        return self.getOrDefault(self.rateDrop)

    def getOneDrop(self):
        return self.getOrDefault(self.oneDrop)

    def getSkipDrop(self):
        return self.getOrDefault(self.skipDrop)

    def getGpuId(self):
        return self.getOrDefault(self.gpuId)

    def getBackend(self):
        return self.getOrDefault(self.backend).toString()


    ##
    # Setters
    ##
    def setQuietMode(self, value):
        assert_is_type(value, bool)
        return self._set(quietMode=value)

    def setMissingValuesHandling(self, value):
        if value is not None:
            assert_is_type(value, None, Enum("MeanImputation", "Skip"))
            jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
            correct_case_value = get_correct_case_enum(jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.MissingValuesHandling.values(), value)
            return self._set(missingValuesHandling=jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.MissingValuesHandling.valueOf(correct_case_value))
        else:
            return self._set(missingValuesHandling=None)

    def setNtrees(self, value):
        assert_is_type(value, int)
        return self._set(ntrees=value)

    def setNEstimators(self, value):
        assert_is_type(value, int)
        return self._set(nEstimators=value)

    def setMaxDepth(self, value):
        assert_is_type(value, int)
        return self._set(maxDepth=value)

    def setMinRows(self, value):
        assert_is_type(value, int, float)
        return self._set(minRows=value)

    def setMinChildWeight(self, value):
        assert_is_type(value, int, float)
        return self._set(minChildWeight=value)

    def setLearnRate(self, value):
        assert_is_type(value, int, float)
        return self._set(learnRate=value)

    def setEta(self, value):
        assert_is_type(value, int, float)
        return self._set(eta=value)

    def setLearnRateAnnealing(self, value):
        assert_is_type(value, int, float)
        return self._set(learnRateAnnealing=value)

    def setSampleRate(self, value):
        assert_is_type(value, int, float)
        return self._set(sampleRate=value)

    def setSubsample(self, value):
        assert_is_type(value, int, float)
        return self._set(subsample=value)

    def setColSampleRate(self, value):
        assert_is_type(value, int, float)
        return self._set(colSampleRate=value)

    def setColSampleByLevel(self, value):
        assert_is_type(value, int, float)
        return self._set(colSampleByLevel=value)

    def setColSampleRatePerTree(self, value):
        assert_is_type(value, int, float)
        return self._set(colSampleRatePerTree=value)

    def setColsampleBytree(self, value):
        assert_is_type(value, int, float)
        return self._set(colsampleBytree=value)

    def setMaxAbsLeafnodePred(self, value):
        assert_is_type(value, int, float)
        return self._set(maxAbsLeafnodePred=value)

    def setMaxDeltaStep(self, value):
        assert_is_type(value, int, float)
        return self._set(maxDeltaStep=value)

    def setScoreTreeInterval(self, value):
        assert_is_type(value, int)
        return self._set(scoreTreeInterval=value)

    def setInitialScoreInterval(self, value):
        assert_is_type(value, int)
        return self._set(initialScoreInterval=value)

    def setScoreInterval(self, value):
        assert_is_type(value, int)
        return self._set(scoreInterval=value)

    def setMinSplitImprovement(self, value):
        assert_is_type(value, int, float)
        return self._set(minSplitImprovement=value)

    def setGamma(self, value):
        assert_is_type(value, int, float)
        return self._set(gamma=value)

    def setNthread(self, value):
        assert_is_type(value, int)
        return self._set(nthread=value)

    def setMaxBins(self, value):
        assert_is_type(value, int)
        return self._set(maxBins=value)

    def setMaxLeaves(self, value):
        assert_is_type(value, int)
        return self._set(maxLeaves=value)

    def setMinSumHessianInLeaf(self, value):
        assert_is_type(value, int, float)
        return self._set(minSumHessianInLeaf=value)

    def setMinDataInLeaf(self, value):
        assert_is_type(value, int, float)
        return self._set(minDataInLeaf=value)

    def setTreeMethod(self, value):
        assert_is_type(value, Enum("auto", "exact", "approx", "hist"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.TreeMethod.values(), value)
        return self._set(treeMethod=jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.TreeMethod.valueOf(correct_case_value))

    def setGrowPolicy(self, value):
        assert_is_type(value, Enum("depthwise", "lossguide"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.GrowPolicy.values(), value)
        return self._set(growPolicy=jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.GrowPolicy.valueOf(correct_case_value))

    def setBooster(self, value):
        assert_is_type(value, Enum("gbtree", "gblinear", "dart"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.Booster.values(), value)
        return self._set(booster=jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.Booster.valueOf(correct_case_value))

    def setDmatrixType(self, value):
        assert_is_type(value, Enum("auto", "dense", "sparse"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.DMatrixType.values(), value)
        return self._set(dmatrixType=jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.DMatrixType.valueOf(correct_case_value))

    def setRegLambda(self, value):
        assert_is_type(value, int, float)
        return self._set(regLambda=value)

    def setRegAlpha(self, value):
        assert_is_type(value, int, float)
        return self._set(regAlpha=value)

    def setSampleType(self, value):
        assert_is_type(value, Enum("uniform", "weighted"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.DartSampleType.values(), value)
        return self._set(sampleType=jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.DartSampleType.valueOf(correct_case_value))

    def setNormalizeType(self, value):
        assert_is_type(value, Enum("tree", "forest"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.DartNormalizeType.values(), value)
        return self._set(normalizeType=jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.DartNormalizeType.valueOf(correct_case_value))

    def setRateDrop(self, value):
        assert_is_type(value, int, float)
        return self._set(rateDrop=value)

    def setOneDrop(self, value):
        assert_is_type(value, bool)
        return self._set(oneDrop=value)

    def setSkipDrop(self, value):
        assert_is_type(value, int, float)
        return self._set(skipDrop=value)

    def setGpuId(self, value):
        assert_is_type(value, int)
        return self._set(gpuId=value)

    def setBackend(self, value):
        assert_is_type(value, Enum("auto", "gpu", "cpu"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.Backend.values(), value)
        return self._set(backend=jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.Backend.valueOf(correct_case_value))

class H2OGLMParams(H2OAlgorithmParams):

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
        return self.getOrDefault(self.family).toString()

    def getLink(self):
        return self.getOrDefault(self.link).toString()

    def getSolver(self):
        return self.getOrDefault(self.solver).toString()

    def getTweedieVariancePower(self):
        return self.getOrDefault(self.tweedieVariancePower)

    def getTweedieLinkPower(self):
        return self.getOrDefault(self.tweedieLinkPower)

    def getAlpha(self):
        return self.getOrDefault(self.alpha)

    def getLambda(self):
        return self.getOrDefault(self.lambda_)

    def getMissingValuesHandling(self):
        return self.getOrDefault(self.missingValuesHandling).toString()

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
        assert_is_type(value, Enum("gaussian", "binomial", "quasibinomial", "poisson", "gamma", "multinomial", "tweedie", "ordinal"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.glm.GLMModel.GLMParameters.Family.values(), value)
        return self._set(family=jvm.hex.glm.GLMModel.GLMParameters.Family.valueOf(correct_case_value))

    def setLink(self, value):
        assert_is_type(value, Enum("family_default", "identity", "logit", "log", "inverse", "tweedie", "multinomial", "ologit", "oprobit", "ologlog"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.glm.GLMModel.GLMParameters.Link.values(), value)
        return self._set(link=jvm.hex.glm.GLMModel.GLMParameters.Link.valueOf(correct_case_value))

    def setSolver(self, value):
        assert_is_type(value, Enum("AUTO", "IRLSM", "L_BFGS", "COORDINATE_DESCENT_NAIVE", "COORDINATE_DESCENT", "GRADIENT_DESCENT_LH", "GRADIENT_DESCENT_SQERR"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.glm.GLMModel.GLMParameters.Solver.values(), value)
        return self._set(solver=jvm.hex.glm.GLMModel.GLMParameters.Solver.valueOf(correct_case_value))

    def setTweedieVariancePower(self, value):
        assert_is_type(value, int, float)
        return self._set(tweedieVariancePower=value)

    def setTweedieLinkPower(self, value):
        assert_is_type(value, int, float)
        return self._set(tweedieLinkPower=value)

    def setAlpha(self, value):
        assert_is_type(value, None, [int, float])
        return self._set(alpha=value)

    def setLambda(self, value):
        assert_is_type(value, None, [int, float])
        return self._set(lambda_=value)

    def setMissingValuesHandling(self, value):
        assert_is_type(value, Enum("MeanImputation", "Skip"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.deeplearning.DeepLearningModel.DeepLearningParameters.MissingValuesHandling.values(), value)
        return self._set(missingValuesHandling=jvm.hex.deeplearning.DeepLearningModel.DeepLearningParameters.MissingValuesHandling.valueOf(correct_case_value))

    def setPrior(self, value):
        assert_is_type(value, int, float)
        return self._set(prior=value)

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
        return self._set(lambdaMinRatio=value)

    def setMaxIterations(self, value):
        assert_is_type(value, int)
        return self._set(maxIterations=value)

    def setIntercept(self, value):
        assert_is_type(value, bool)
        return self._set(intercept=value)

    def setBetaEpsilon(self, value):
        assert_is_type(value, int, float)
        return self._set(betaEpsilon=value)

    def setObjectiveEpsilon(self, value):
        assert_is_type(value, int, float)
        return self._set(objectiveEpsilon=value)

    def setGradientEpsilon(self, value):
        assert_is_type(value, int, float)
        return self._set(gradientEpsilon=value)

    def setObjReg(self, value):
        assert_is_type(value, int, float)
        return self._set(objReg=value)

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

class H2OGridSearchParams(Params):

    ##
    # Param definitions
    ##
    featuresCols = Param(Params._dummy(), "featuresCols", "columns used as features")
    algo = Param(Params._dummy(), "algo", "Algo to run grid search on")
    ratio = Param(Params._dummy(), "ratio", "ratio")
    hyperParameters = Param(Params._dummy(), "hyperParameters", "Grid Search Hyper Params map")
    labelCol = Param(Params._dummy(), "labelCol", "Label column name")
    weightCol = Param(Params._dummy(), "weightCol", "Weight column name")
    allStringColumnsToCategorical = Param(Params._dummy(), "allStringColumnsToCategorical", "allStringColumnsToCategorical")
    columnsToCategorical = Param(Params._dummy(), "columnsToCategorical", "columnsToCategorical")
    strategy = Param(Params._dummy(), "strategy", "strategy")
    maxRuntimeSecs = Param(Params._dummy(), "maxRuntimeSecs", "maxRuntimeSecs")
    maxModels = Param(Params._dummy(), "maxModels", "maxModels")
    seed = Param(Params._dummy(), "seed", "seed")
    stoppingRounds = Param(Params._dummy(), "stoppingRounds", "stoppingRounds")
    stoppingTolerance = Param(Params._dummy(), "stoppingTolerance", "stoppingTolerance")
    stoppingMetric = Param(Params._dummy(), "stoppingMetric", "stoppingMetric")
    nfolds = Param(Params._dummy(), "nfolds", "nfolds")
    foldCol = Param(Params._dummy(), "foldCol", "Fold column name")
    selectBestModelBy = Param(Params._dummy(), "selectBestModelBy", "selectBestModelBy")
    selectBestModelDecreasing = Param(Params._dummy(), "selectBestModelDecreasing", "selectBestModelDecreasing")
    convertUnknownCategoricalLevelsToNa = Param(Params._dummy(), "convertUnknownCategoricalLevelsToNa", "If set to 'true', the model converts unknown categorical levels to NA during making predictions.")

    ##
    # Getters
    ##
    def getFeaturesCols(self):
        return self.getOrDefault(self.featuresCols)

    def getLabelCol(self):
        return self.getOrDefault(self.labelCol)

    def getWeightCol(self):
        return self.getOrDefault(self.weightCol)

    def getAlgoParams(self):
        return self._java_obj.getAlgoParams()

    def getRatio(self):
        return self.getOrDefault(self.ratio)

    def getHyperParameters(self):
        params = self.getOrDefault(self.hyperParameters)
        if isinstance(params, JavaObject):
            keys = [k for k in params.keySet().toArray()]
            map = {}
            for k in keys:
                map[k] = [v for v in params.get(k)]
            return map
        else:
            return params

    def getAllStringColumnsToCategorical(self):
        return self.getOrDefault(self.allStringColumnsToCategorical)

    def getColumnsToCategorial(self):
        return self.getOrDefault(self.columnsToCategorical)

    def getStrategy(self):
        # Convert Java Enum to String so we can represent it in Python
        return self.getOrDefault(self.strategy).toString()

    def getMaxRuntimeSecs(self):
        return self.getOrDefault(self.maxRuntimeSecs)

    def getMaxModels(self):
        return self.getOrDefault(self.maxModels)

    def getSeed(self):
        return self.getOrDefault(self.seed)

    def getStoppingRounds(self):
        return self.getOrDefault(self.stoppingRounds)

    def getStoppingTolerance(self):
        return self.getOrDefault(self.stoppingTolerance)

    def getStoppingMetric(self):
        # Convert Java Enum to String so we can represent it in Python
        return self.getOrDefault(self.stoppingMetric).toString()

    def getNfolds(self):
        return self.getOrDefault(self.nfolds)

    def getFoldCol(self):
        return self.getOrDefault(self.foldCol)

    def getSelectBestModelBy(self):
        # Convert Java Enum to String so we can represent it in Python
        return self.getOrDefault(self.selectBestModelBy).toString()

    def getSelectBestModelDecreasing(self):
        return self.getOrDefault(self.selectBestModelDecreasing)

    def getConvertUnknownCategoricalLevelsToNa(self):
        return self.getOrDefault(self.convertUnknownCategoricalLevelsToNa)

    ##
    # Setters
    ##
    def setFeaturesCols(self, value):
        assert_is_type(value, [str])
        return self._set(featuresCols=value)

    def setLabelCol(self, value):
        assert_is_type(value, str)
        return self._set(labelCol=value)

    def setWeightCol(self, value):
        assert_is_type(value, None, str)
        return self._set(weightCol=value)

    def setAlgo(self, value):
        assert_is_type(value, object)
        self._java_obj.setAlgo(value._java_obj)
        return self

    def setRatio(self, value):
        assert_is_type(value, int, float)
        return self._set(ratio=value)

    def setHyperParameters(self, value):
        assert_is_type(value, None, {str : [object]})
        return self._set(hyperParameters=value)

    def setAllStringColumnsToCategorical(self, value):
        assert_is_type(value, bool)
        return self._set(allStringColumnsToCategorical=value)

    def setColumnsToCategorial(self, value):
        assert_is_type(value, bool)
        return self._set(columnsToCategorical=value)

    def setStrategy(self, value):
        assert_is_type(value, Enum("Cartesian", "RandomDiscrete"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.grid.HyperSpaceSearchCriteria.Strategy.values(), value)
        return self._set(link=jvm.hex.grid.HyperSpaceSearchCriteria.Strategy.valueOf(correct_case_value))

    def setMaxRuntimeSecs(self, value):
        assert_is_type(value, int, float)
        return self._set(maxRuntimeSecs=value)

    def setMaxModels(self, value):
        assert_is_type(value, int)
        return self._set(maxModels=value)

    def setSeed(self, value):
        assert_is_type(value, int)
        return self._set(seed=value)

    def setStoppingRounds(self, value):
        assert_is_type(value, int)
        return self._set(stoppingRounds=value)

    def setStoppingTolerance(self, value):
        assert_is_type(value, int, float)
        return self._set(stoppingTolerance=value)

    def setStoppingMetric(self, value):
        # H2O typechecks does not check for case sensitivity
        assert_is_type(value, Enum("AUTO", "deviance", "logloss", "MSE", "RMSE", "MAE", "RMSLE", "AUC", "lift_top_group", "misclassification", "mean_per_class_error", "custom"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.hex.ScoreKeeper.StoppingMetric.values(), value)
        return self._set(stoppingMetric=jvm.hex.ScoreKeeper.StoppingMetric.valueOf(correct_case_value))

    def setNfolds(self, value):
        assert_is_type(value, int)
        return self._set(nfolds=value)

    def setFoldCol(self, value):
        assert_is_type(value, None, str)
        return self._set(foldCol=value)

    def setSelectBestModelBy(self, value):
        # H2O typechecks does not check for case sensitivity
        assert_is_type(value, None, Enum("MeanResidualDeviance", "R2", "ResidualDeviance", "ResidualDegreesOfFreedom", "NullDeviance",
                                   "NullDegreesOfFreedom", "AIC", "AUC", "Gini", "F1", "F2",
                                   "F0point5", "Precision", "Recall", "MCC", "Logloss", "Error", "MaxPerClassError", "Accuracy", "MSE", "RMSE"))
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_case_value = get_correct_case_enum(jvm.org.apache.spark.ml.h2o.algos.H2OGridSearchMetric.values(), value)
        return self._set(selectBestModelBy=jvm.org.apache.spark.ml.h2o.algos.H2OGridSearchMetric.valueOf(correct_case_value))

    def setSelectBestModelDecreasing(self, value):
        assert_is_type(value, bool)
        return self._set(selectBestModelDecreasing=value)

    def setConvertUnknownCategoricalLevelsToNa(self, value):
        assert_is_type(value, bool)
        return self._set(convertUnknownCategoricalLevelsToNa=value)
