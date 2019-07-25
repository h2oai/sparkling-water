import random
import string
import warnings
from pyspark import keyword_only
from pyspark.ml.util import JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator
from pyspark.sql.dataframe import DataFrame

from py_sparkling.ml.util import set_double_values
from py_sparkling.ml.models import H2OMOJOModel
from pysparkling.ml.params import H2OGBMParams, H2ODeepLearningParams, H2OAutoMLParams, H2OXGBoostParams, H2OGLMParams, \
    H2OGridSearchParams
from .util import JavaH2OMLReadable, validateEnumValue, validateEnumValues, arrayToDoubleArray

java_max_double_value = (2-2**(-52))*(2**1023)
from pysparkling.spark_specifics import get_input_kwargs
from pyspark.ml.util import _jvm

def propagate_value_from_deprecated_property(kwargs, from_deprecated, to_replacing):
    if from_deprecated in kwargs:
        warnings.warn("The parameter '{}' is deprecated and its usage will override a value specified via '{}'!".format(from_deprecated, to_replacing))
        kwargs[to_replacing] = kwargs[from_deprecated]
        del kwargs[from_deprecated]


class H2OGBM(H2OGBMParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):
    @keyword_only
    def __init__(self, modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                 nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                 seed=-1, distribution="AUTO", ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                 histogramType="AUTO", r2Stopping=java_max_double_value,
                 nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                 sampleRate=1.0, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                 learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=java_max_double_value,
                 predNoiseBandwidth=0.0, convertUnknownCategoricalLevelsToNa=False, foldCol=None, predictionCol="prediction",
                 detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                 convertInvalidNumbersToNa=False, **deprecatedArgs):
        super(H2OGBM, self).__init__()
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2OGBM", self.uid)

        self._setDefault(modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                         nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                         seed=-1, distribution="AUTO",
                         ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                         histogramType="AUTO",
                         r2Stopping=_jvm().Double.MAX_VALUE, nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                         sampleRate=1.0, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                         learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=_jvm().Double.MAX_VALUE,
                         predNoiseBandwidth=0.0, convertUnknownCategoricalLevelsToNa=False, foldCol=None,
                         predictionCol="prediction", detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                         convertInvalidNumbersToNa=False)
        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False,parallelizeCrossValidation=True,
                  seed=-1, distribution="AUTO", ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                  histogramType="AUTO", r2Stopping=java_max_double_value,
                  nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                  sampleRate=1.0, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                  learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=java_max_double_value,
                  predNoiseBandwidth=0.0, convertUnknownCategoricalLevelsToNa=False, foldCol=None, predictionCol="prediction",
                  detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                  convertInvalidNumbersToNa=False, **deprecatedArgs):
        kwargs = get_input_kwargs(self)

        validateEnumValue(self._H2OAlgoCommonParams__getDistributionEnum(), kwargs, "distribution")
        validateEnumValue(self._H2OSharedTreeParams__getHistogramTypeEnum(), kwargs, "histogramType")

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["minRows", "predNoiseBandwidth", "splitRatio", "learnRate", "colSampleRate", "learnRateAnnealing", "maxAbsLeafnodePred"
                        "minSplitImprovement", "r2Stopping", "sampleRate", "colSampleRateChangePerLevel", "colSampleRatePerTree"]
        set_double_values(kwargs, double_types)

        # We need to also map all doubles in the arrays
        arrayToDoubleArray("sampleRatePerClass", kwargs)

        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)


class H2ODeepLearning(H2ODeepLearningParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                 nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                 seed=-1, distribution="AUTO", epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200], reproducible=False,
                 convertUnknownCategoricalLevelsToNa=False, foldCol=None, predictionCol="prediction", detailedPredictionCol="detailed_prediction",
                 withDetailedPredictionCol=False, convertInvalidNumbersToNa=False, **deprecatedArgs):
        super(H2ODeepLearning, self).__init__()
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2ODeepLearning", self.uid)

        self._setDefault(modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                         nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                         seed=-1, distribution="AUTO",
                         epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200], reproducible=False, convertUnknownCategoricalLevelsToNa=False,
                         foldCol=None, predictionCol="prediction", detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                         convertInvalidNumbersToNa=False)
        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                  seed=-1, distribution="AUTO", epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200], reproducible=False, convertUnknownCategoricalLevelsToNa=False,
                  foldCol=None, predictionCol="prediction", detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                  convertInvalidNumbersToNa=False, **deprecatedArgs):
        kwargs = get_input_kwargs(self)

        validateEnumValue(self._H2OAlgoCommonParams__getDistributionEnum(), kwargs, "distribution")

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["splitRatio", "epochs", "l1", "l2"]
        set_double_values(kwargs, double_types)

        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)


class H2OAutoML(H2OAutoMLParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, featuresCols=[], labelCol="label", allStringColumnsToCategorical=True, columnsToCategorical=[], splitRatio=1.0, foldCol=None,
                 weightCol=None, ignoredCols=[], includeAlgos=None, excludeAlgos=None, projectName=None, maxRuntimeSecs=3600.0, stoppingRounds=3,
                 stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5, convertUnknownCategoricalLevelsToNa=True, seed=-1,
                 sortMetric="AUTO", balanceClasses=False, classSamplingFactors=None, maxAfterBalanceSize=5.0,
                 keepCrossValidationPredictions=True, keepCrossValidationModels=True, maxModels=0,
                 predictionCol="prediction", detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                 convertInvalidNumbersToNa=False, **deprecatedArgs):
        super(H2OAutoML, self).__init__()
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2OAutoML", self.uid)

        self._setDefault(featuresCols=[], labelCol="label", allStringColumnsToCategorical=True, columnsToCategorical=[], splitRatio=1.0, foldCol=None,
                         weightCol=None, ignoredCols=[], includeAlgos=None, excludeAlgos=None, projectName=None, maxRuntimeSecs=3600.0, stoppingRounds=3,
                         stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5,
                         convertUnknownCategoricalLevelsToNa=True, seed=-1, sortMetric=None, balanceClasses=False,
                         classSamplingFactors=None, maxAfterBalanceSize=5.0, keepCrossValidationPredictions=True,
                         keepCrossValidationModels=True, maxModels=0, predictionCol="prediction", detailedPredictionCol="detailed_prediction",
                         withDetailedPredictionCol=False, convertInvalidNumbersToNa=False)
        kwargs = get_input_kwargs(self)

        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCols=[], labelCol="label", allStringColumnsToCategorical=True, columnsToCategorical=[], splitRatio=1.0, foldCol=None,
                  weightCol=None, ignoredCols=[], includeAlgos=None, excludeAlgos=None, projectName=None, maxRuntimeSecs=3600.0, stoppingRounds=3,
                  stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5, convertUnknownCategoricalLevelsToNa=True, seed=-1,
                  sortMetric="AUTO", balanceClasses=False, classSamplingFactors=None, maxAfterBalanceSize=5.0, keepCrossValidationPredictions=True,
                  keepCrossValidationModels=True, maxModels=0, predictionCol="prediction", detailedPredictionCol="detailed_prediction",
                  withDetailedPredictionCol=False, convertInvalidNumbersToNa=False, **deprecatedArgs):

        kwargs = get_input_kwargs(self)

        validateEnumValues(self._H2OAutoMLParams__getAutomlAlgoEnum(), kwargs, "includeAlgos", nullEnabled=True)
        validateEnumValues(self._H2OAutoMLParams__getAutomlAlgoEnum(), kwargs, "excludeAlgos", nullEnabled=True)
        validateEnumValue(self._H2OAutoMLParams__getStoppingMetricEnum(), kwargs, "stoppingMetric")


        if "projectName" in kwargs and kwargs["projectName"] is None:
            kwargs["projectName"] = ''.join(random.choice(string.ascii_letters) for i in range(30))

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["maxRuntimeSecs", "stoppingTolerance", "splitRatio", "maxAfterBalanceSize"]
        set_double_values(kwargs, double_types)
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)

    def leaderboard(self):
        leaderboard_java = self._java_obj.leaderboard()
        if leaderboard_java.isDefined():
            return DataFrame(leaderboard_java.get(), self._hc._sql_context)
        else:
            return None


class H2OXGBoost(H2OXGBoostParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                 nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                 seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False, quietMode=True,
                 ntrees=50, nEstimators=0, maxDepth=6, minRows=1.0, minChildWeight=1.0, learnRate=0.3, eta=0.3, learnRateAnnealing=1.0,
                 sampleRate=1.0, subsample=1.0, colSampleRate=1.0, colSampleByLevel=1.0, colSampleRatePerTree=1.0, colsampleBytree=1.0,
                 maxAbsLeafnodePred=0.0, maxDeltaStep=0.0, scoreTreeInterval=0, initialScoreInterval=4000, scoreInterval=4000,
                 minSplitImprovement=0.0, gamma=0.0, nthread=-1, maxBins=256, maxLeaves=0,
                 minSumHessianInLeaf=100.0, minDataInLeaf=0.0, treeMethod="auto", growPolicy="depthwise",
                 booster="gbtree", dmatrixType="auto", regLambda=0.0, regAlpha=0.0, sampleType="uniform",
                 normalizeType="tree", rateDrop=0.0, oneDrop=False, skipDrop=0.0, gpuId=0, backend="auto",
                 foldCol=None, predictionCol="prediction", detailedPredictionCol="detailed_prediction",
                 withDetailedPredictionCol=False, convertInvalidNumbersToNa=False, **deprecatedArgs):
        super(H2OXGBoost, self).__init__()
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2OXGBoost", self.uid)

        self._setDefault(modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                         nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                         seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False,
                         quietMode=True, ntrees=50, nEstimators=0, maxDepth=6, minRows=1.0, minChildWeight=1.0,
                         learnRate=0.3, eta=0.3, learnRateAnnealing=1.0, sampleRate=1.0, subsample=1.0, colSampleRate=1.0, colSampleByLevel=1.0,
                         colSampleRatePerTree=1.0, colsampleBytree=1.0, maxAbsLeafnodePred=0.0, maxDeltaStep=0.0, scoreTreeInterval=0,
                         initialScoreInterval=4000, scoreInterval=4000, minSplitImprovement=0.0, gamma=0.0, nthread=-1, maxBins=256, maxLeaves=0,
                         minSumHessianInLeaf=100.0, minDataInLeaf=0.0,
                         treeMethod="auto", growPolicy="depthwise", booster="gbtree", dmatrixType="auto", regLambda=0.0, regAlpha=0.0,
                         sampleType="uniform", normalizeType="tree", rateDrop=0.0, oneDrop=False, skipDrop=0.0, gpuId=0,
                         backend="auto", foldCol=None, predictionCol="prediction", detailedPredictionCol="detailed_prediction",
                         withDetailedPredictionCol=False, convertInvalidNumbersToNa=False)

        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                  seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False, quietMode=True,
                  ntrees=50, nEstimators=0, maxDepth=6, minRows=1.0, minChildWeight=1.0, learnRate=0.3, eta=0.3, learnRateAnnealing=1.0,
                  sampleRate=1.0, subsample=1.0, colSampleRate=1.0, colSampleByLevel=1.0, colSampleRatePerTree=1.0, colsampleBytree=1.0,
                  maxAbsLeafnodePred=0.0, maxDeltaStep=0.0, scoreTreeInterval=0, initialScoreInterval=4000, scoreInterval=4000,
                  minSplitImprovement=0.0, gamma=0.0, nthread=-1, maxBins=256, maxLeaves=0,
                  minSumHessianInLeaf=100.0, minDataInLeaf=0.0, treeMethod="auto", growPolicy="depthwise",
                  booster="gbtree", dmatrixType="auto", regLambda=0.0, regAlpha=0.0, sampleType="uniform",
                  normalizeType="tree", rateDrop=0.0, oneDrop=False, skipDrop=0.0, gpuId=0, backend="auto",
                  foldCol=None, predictionCol="prediction", detailedPredictionCol="detailed_prediction",
                  withDetailedPredictionCol=False, convertInvalidNumbersToNa=False, **deprecatedArgs):
        kwargs = get_input_kwargs(self)

        validateEnumValue(self._H2OAlgoCommonParams__getDistributionEnum(), kwargs, "distribution")
        validateEnumValue(self._H2OXGBoostParams__getTreeMethodEnum(), kwargs, "treeMethod")
        validateEnumValue(self._H2OXGBoostParams__getGrowPolicyEnum(), kwargs, "growPolicy")
        validateEnumValue(self._H2OXGBoostParams__getBoosterEnum(), kwargs, "booster")
        validateEnumValue(self._H2OXGBoostParams__getDmatrixTypeEnum(), kwargs, "dmatrixType")
        validateEnumValue(self._H2OXGBoostParams__getSampleTypeEnum(), kwargs, "sampleType")
        validateEnumValue(self._H2OXGBoostParams__getNormalizeTypeEnum(), kwargs, "normalizeType")
        validateEnumValue(self._H2OXGBoostParams__getBackendEnum(), kwargs, "backend")


    # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["splitRatio", "minRows", "minChildWeight", "learnRate", "eta", "learnRateAnnealing"
                        "sampleRate", "subsample", "colSampleRate", "colSampleByLevel", "colSampleRatePerTree",
                        "colsampleBytree", "maxAbsLeafnodePred", "maxDeltaStep", "minSplitImprovement", "gamma",
                        "minSumHessianInLeaf", "minDataInLeaf", "regLambda", "regAlpha", "rateDrop", "skipDrop"]
        set_double_values(kwargs, double_types)
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)


class H2OGLM(H2OGLMParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):
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
                 convertInvalidNumbersToNa=False, **deprecatedArgs):
        super(H2OGLM, self).__init__()
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2OGLM", self.uid)

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
                         convertInvalidNumbersToNa=False)
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
                  convertInvalidNumbersToNa=False, **deprecatedArgs):
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


class H2OGridSearch(H2OGridSearchParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):
    @keyword_only
    def __init__(self, featuresCols=[], algo=None, splitRatio=1.0, hyperParameters={}, labelCol="label", weightCol=None, allStringColumnsToCategorical=True,
                 columnsToCategorical=[], strategy="Cartesian", maxRuntimeSecs=0.0, maxModels=0, seed=-1,
                 stoppingRounds=0, stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=0, selectBestModelBy="AUTO",
                 selectBestModelDecreasing=True, foldCol=None, convertUnknownCategoricalLevelsToNa=True,
                 predictionCol="prediction", detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                 convertInvalidNumbersToNa=False, **deprecatedArgs):
        super(H2OGridSearch, self).__init__()
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2OGridSearch", self.uid)

        self._setDefault(featuresCols=[], algo=None, splitRatio=1.0, hyperParameters={}, labelCol="label", weightCol=None, allStringColumnsToCategorical=True,
                         columnsToCategorical=[], strategy="Cartesian",
                         maxRuntimeSecs=0.0, maxModels=0, seed=-1,
                         stoppingRounds=0, stoppingTolerance=0.001,
                         stoppingMetric="AUTO", nfolds=0,
                         selectBestModelBy="AUTO", selectBestModelDecreasing=True, foldCol=None,
                         convertUnknownCategoricalLevelsToNa=True, predictionCol="prediction",
                         detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                         convertInvalidNumbersToNa=False)
        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCols=[], algo=None, splitRatio=1.0, hyperParameters={}, labelCol="label", weightCol=None, allStringColumnsToCategorical=True,
                  columnsToCategorical=[], strategy="Cartesian", maxRuntimeSecs=0.0, maxModels=0, seed=-1,
                  stoppingRounds=0, stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=0, selectBestModelBy="AUTO",
                  selectBestModelDecreasing=True, foldCol=None, convertUnknownCategoricalLevelsToNa=True,
                  predictionCol="prediction", detailedPredictionCol="detailed_prediction", withDetailedPredictionCol=False,
                  convertInvalidNumbersToNa=False, **deprecatedArgs):
        kwargs = get_input_kwargs(self)

        validateEnumValue(self._H2OGridSearchParams__getStrategyEnum(), kwargs, "strategy")
        validateEnumValue(self._H2OGridSearchParams__getStoppingMetricEnum(), kwargs, "stoppingMetric")
        validateEnumValue(self._H2OGridSearchParams__getSelectBestModelByEnum(), kwargs, "selectBestModelBy")

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["splitRatio", "stoppingTolerance", "maxRuntimeSecs"]
        set_double_values(kwargs, double_types)
        if "algo" in kwargs and kwargs["algo"] is not None:
            tmp = kwargs["algo"]
            del kwargs['algo']
            self._java_obj.setAlgo(tmp._java_obj)

        return self._set(**kwargs)

    def get_grid_models(self):
        return [H2OMOJOModel(m) for m in self._java_obj.getGridModels()]

    def get_grid_models_params(self):
        return DataFrame(self._java_obj.getGridModelsParams(),  self._hc._sql_context)

    def get_grid_models_metrics(self):
        return DataFrame(self._java_obj.getGridModelsMetrics(),  self._hc._sql_context)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)
