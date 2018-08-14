from pyspark.ml.param import *
from h2o.utils.typechecks import assert_is_type, Enum
from pysparkling.context import H2OContext
from pyspark.sql import SparkSession


def get_correct_case_enum(enum_values, enum_single_value):
    for a in enum_values:
        if a.toString().lower() == enum_single_value.lower():
            return a.toString()


class H2OAlgorithmParams(Params):
    ##
    # Param definitions
    ##
    ratio = Param(Params._dummy(), "ratio", "Ration of frame which is used for training")
    predictionCol = Param(Params._dummy(), "predictionCol", "label")
    featuresCols = Param(Params._dummy(), "featuresCols", "columns used as features")
    allStringColumnsToCategorical = Param(Params._dummy(), "allStringColumnsToCategorical", "Transform all strings columns to categorical")
    columnsToCategorical = Param(Params._dummy(), "columnsToCategorical", "List of columns to convert to categoricals before modelling")
    nfolds = Param(Params._dummy(), "nfolds", "Number of folds for K-fold cross-validation (0 to disable or >= 2)")
    keepCrossValidationPredictions = Param(Params._dummy(), "keepCrossValidationPredictions", "Whether to keep the predictions of the cross-validation models", )
    keepCrossValidationFoldAssignment = Param(Params._dummy(), "keepCrossValidationFoldAssignment", "Whether to keep the cross-validation fold assignment", )
    parallelizeCrossValidation = Param(Params._dummy(), "parallelizeCrossValidation", "Allow parallel training of cross-validation models", )
    seed = Param(Params._dummy(), "seed", "Seed for random numbers (affects sampling) - Note: only reproducible when running single threaded.")
    distribution = Param(Params._dummy(), "distribution", "Distribution function")
    convertUnknownCategoricalLevelsToNa = Param(Params._dummy(), "convertUnknownCategoricalLevelsToNa", "Convert unknown categorical levels to NA during predictions")

    ##
    # Getters
    ##
    def getRatio(self):
        return self.getOrDefault(self.ratio)

    def getPredictionCol(self):
        return self.getOrDefault(self.predictionCol)

    def getFeaturesCols(self):
        return self.getOrDefault(self.featuresCols)

    def getAllStringColumnsToCategorical(self):
        return self.getOrDefault(self.allStringColumnsToCategorical)

    def getColumnsToCategorical(self):
        return self.getOrDefault(self.columnsToCategorical)

    def getNfolds(self):
        return self.getOrDefault(self.nfolds)

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
        assert_is_type(value, str)
        return self._set(predictionCol=value)

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
    predictionCol = Param(Params._dummy(), "predictionCol", "label")
    allStringColumnsToCategorical = Param(Params._dummy(), "allStringColumnsToCategorical", "Transform all strings columns to categorical")
    columnsToCategorical = Param(Params._dummy(), "columnsToCategorical", "List of columns to convert to categoricals before modelling")
    ratio = Param(Params._dummy(), "ratio", "Ration of frame which is used for training")
    foldColumn = Param(Params._dummy(), "foldColumn", "Fold column name")
    weightsColumn = Param(Params._dummy(), "weightsColumn", "Weights column name")
    ignoredColumns = Param(Params._dummy(), "ignoredColumns", "Ignored columns names")
    excludeAlgos = Param(Params._dummy(), "excludeAlgos", "Algorithms to exclude when using automl")
    projectName = Param(Params._dummy(), "projectName", "dentifier for models that should be grouped together in the leaderboard" +
                        " (e.g., airlines and iris)")
    maxRuntimeSecs = Param(Params._dummy(), "maxRuntimeSecs", "Maximum time in seconds for automl to be running")
    stoppingRounds = Param(Params._dummy(), "stoppingRounds", "Stopping rounds")
    stoppingTolerance = Param(Params._dummy(), "stoppingTolerance", "Stopping tolerance")
    stoppingMetric = Param(Params._dummy(), "stoppingMetric", "Stopping metric")
    nfolds = Param(Params._dummy(), "nfolds", "Cross-validation fold construction")
    convertUnknownCategoricalLevelsToNa = Param(Params._dummy(), "convertUnknownCategoricalLevelsToNa", "Convert unknown categorical levels to NA during predictions")
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
    def getPredictionCol(self):
        return self.getOrDefault(self.predictionCol)

    def getAllStringColumnsToCategorical(self):
        return self.getOrDefault(self.allStringColumnsToCategorical)

    def getColumnsToCategorical(self):
        return self.getOrDefault(self.columnsToCategorical)

    def getRatio(self):
        return self.getOrDefault(self.ratio)

    def getFoldColumn(self):
        return self.getOrDefault(self.foldColumn)

    def getWeightsColumn(self):
        return self.getOrDefault(self.weightsColumn)

    def getIgnoredColumns(self):
        return self.getOrDefault(self.ignoredColumns)

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
    def setPredictionCol(self, value):
        assert_is_type(value, str)
        return self._set(predictionCol=value)

    def setAllStringColumnsToCategorical(self, value):
        assert_is_type(value, bool)
        return self._set(allStringColumnsToCategorical=value)

    def setColumnsToCategorical(self, value):
        assert_is_type(value, [str])
        return self._set(columnsToCategorical=value)

    def setRatio(self, value):
        assert_is_type(value, int, float)
        return self._set(ratio=value)

    def setFoldColumn(self, value):
        assert_is_type(value, None, str)
        return self._set(foldColumn=value)

    def setWeightsColumn(self, value):
        assert_is_type(value, None, str)
        return self._set(weightsColumn=value)

    def setIgnoredColumns(self, value):
        assert_is_type(value, [str])
        return self._set(ignoredColumns=value)

    def setTryMutations(self, value):
        assert_is_type(value, bool)
        return self._set(tryMutations=value)

    def setExcludeAlgos(self, value):
        assert_is_type(value, None, [Enum("GLM", "DRF", "GBM", "DeepLearning", "StackedEnsemble")])
        # H2O typechecks does not check for case sensitivity
        java_enums = []
        if value is not None:
            for algo in value:
                jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
                java_enums.append(get_correct_case_enum(jvm.ai.h2o.automl.AutoML.algo.values(), algo))
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

