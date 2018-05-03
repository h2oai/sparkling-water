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
        assert_is_type(value, None, Enum("AUTO", "bernoulli", "multinomial", "gaussian", "poisson", "gamma", "tweedie", "laplace", "quantile", "huber"))
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
    tryMutations = Param(Params._dummy(), "tryMutations", "Whether to use mutations as part of the feature engineering")
    excludeAlgos = Param(Params._dummy(), "excludeAlgos", "Algorithms to exclude when using automl")
    projectName = Param(Params._dummy(), "projectName", "dentifier for models that should be grouped together in the leaderboard" +
                        " (e.g., airlines and iris)")
    loss = Param(Params._dummy(), "loss", "loss")
    maxRuntimeSecs = Param(Params._dummy(), "maxRuntimeSecs", "Maximum time in seconds for automl to be running")
    stoppingRounds = Param(Params._dummy(), "stoppingRounds", "Stopping rounds")
    stoppingTolerance = Param(Params._dummy(), "stoppingTolerance", "Stopping tolerance")
    stoppingMetric = Param(Params._dummy(), "stoppingMetric", "Stopping metric")
    nfolds = Param(Params._dummy(), "nfolds", "Cross-validation fold construction")
    convertUnknownCategoricalLevelsToNa = Param(Params._dummy(), "convertUnknownCategoricalLevelsToNa", "Convert unknown categorical levels to NA during predictions")
    seed = Param(Params._dummy(), "seed", "Seed for random numbers")

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
