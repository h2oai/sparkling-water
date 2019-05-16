from pyspark import keyword_only
from pyspark.ml.util import JavaMLWritable, JavaMLReader, MLReadable
from pyspark.ml.wrapper import JavaEstimator
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import random
import string
import warnings

from pysparkling import *
from pysparkling.ml.params import H2OGBMParams, H2ODeepLearningParams, H2OAutoMLParams, H2OXGBoostParams, H2OGLMParams, H2OGridSearchParams
from .util import JavaH2OMLReadable
from py_sparkling.ml.models import H2OMOJOModel
from py_sparkling.ml.util import get_enum_array_from_str_array
java_max_double_value = (2-2**(-52))*(2**1023)
from pysparkling.spark_specifics import get_input_kwargs

def set_double_values(kwargs, values):
    for v in values:
        if v in kwargs:
            kwargs[v] = float(kwargs[v])


def propagate_value_from_deprecated_property(kwargs, from_deprecated, to_replacing):
    if from_deprecated in kwargs:
        warnings.warn("The parameter '{}' is deprecated and its usage will override a value specified via '{}'!".format(from_deprecated, to_replacing))
        kwargs[to_replacing] = kwargs[from_deprecated]
        del kwargs[from_deprecated]


class H2OGBM(H2OGBMParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):
    @keyword_only
    def __init__(self, ratio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                 nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                 seed=-1, distribution="AUTO", ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                 histogramType="AUTO", r2Stopping=java_max_double_value,
                 nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                 sampleRate=1.0, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                 learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=java_max_double_value,
                 predNoiseBandwidth=0.0, convertUnknownCategoricalLevelsToNa=False, foldCol=None, **deprecatedArgs):
        super(H2OGBM, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2OGBM", self.uid)

        self._setDefault(ratio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                         nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                         seed=-1, distribution=self._hc._jvm.hex.genmodel.utils.DistributionFamily.valueOf("AUTO"),
                         ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                         histogramType=self._hc._jvm.hex.tree.SharedTreeModel.SharedTreeParameters.HistogramType.valueOf("AUTO"),
                         r2Stopping=self._hc._jvm.Double.MAX_VALUE, nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                         sampleRate=1.0, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                         learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=self._hc._jvm.Double.MAX_VALUE,
                         predNoiseBandwidth=0.0, convertUnknownCategoricalLevelsToNa=False, foldCol=None)
        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, ratio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False,parallelizeCrossValidation=True,
                  seed=-1, distribution="AUTO", ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                  histogramType="AUTO", r2Stopping=java_max_double_value,
                  nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                  sampleRate=1.0, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                  learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=java_max_double_value,
                  predNoiseBandwidth=0.0, convertUnknownCategoricalLevelsToNa=False, foldCol=None, **deprecatedArgs):
        kwargs = get_input_kwargs(self)

        if "distribution" in kwargs:
            kwargs["distribution"] = self._hc._jvm.hex.genmodel.utils.DistributionFamily.valueOf(kwargs["distribution"])
        if "histogramType" in kwargs:
            kwargs["histogramType"] = self._hc._jvm.hex.tree.SharedTreeModel.SharedTreeParameters.HistogramType.valueOf(kwargs["histogramType"])

        propagate_value_from_deprecated_property(kwargs, "predictionCol", "labelCol")

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["minRows", "predNoiseBandwidth", "ratio", "learnRate", "colSampleRate", "learnRateAnnealing", "maxAbsLeafnodePred"
                        "minSplitImprovement", "r2Stopping", "sampleRate", "colSampleRateChangePerLevel", "colSampleRatePerTree"]
        set_double_values(kwargs, double_types)

        # We need to also map all doubles in the arrays
        if "sampleRatePerClass" in kwargs:
            kwargs["sampleRatePerClass"] = map(float, kwargs["sampleRatePerClass"])

        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)


class H2ODeepLearning(H2ODeepLearningParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, ratio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                 nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                 seed=-1, distribution="AUTO", epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200], reproducible=False,
                 convertUnknownCategoricalLevelsToNa=False, foldCol=None, **deprecatedArgs):
        super(H2ODeepLearning, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2ODeepLearning", self.uid)

        self._setDefault(ratio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                         nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                         seed=-1, distribution=self._hc._jvm.hex.genmodel.utils.DistributionFamily.valueOf("AUTO"),
                         epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200], reproducible=False, convertUnknownCategoricalLevelsToNa=False,
                         foldCol=None)
        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, ratio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                  seed=-1, distribution="AUTO", epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200], reproducible=False, convertUnknownCategoricalLevelsToNa=False,
                  foldCol=None, **deprecatedArgs):
        kwargs = get_input_kwargs(self)

        if "distribution" in kwargs:
            kwargs["distribution"] = self._hc._jvm.hex.genmodel.utils.DistributionFamily.valueOf(kwargs["distribution"])

        propagate_value_from_deprecated_property(kwargs, "predictionCol", "labelCol")

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["ratio", "epochs", "l1", "l2"]
        set_double_values(kwargs, double_types)

        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)


class H2OAutoML(H2OAutoMLParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, featuresCols=[], labelCol="label", allStringColumnsToCategorical=True, columnsToCategorical=[], ratio=1.0, foldCol=None,
                 weightCol=None, ignoredCols=[], includeAlgos=None, excludeAlgos=None, projectName=None, maxRuntimeSecs=3600.0, stoppingRounds=3,
                 stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5, convertUnknownCategoricalLevelsToNa=True, seed=-1,
                 sortMetric="AUTO", balanceClasses=False, classSamplingFactors=None, maxAfterBalanceSize=5.0,
                 keepCrossValidationPredictions=True, keepCrossValidationModels=True, maxModels=0, **deprecatedArgs):
        super(H2OAutoML, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2OAutoML", self.uid)

        self._setDefault(featuresCols=[], labelCol="label", allStringColumnsToCategorical=True, columnsToCategorical=[], ratio=1.0, foldCol=None,
                         weightCol=None, ignoredCols=[], includeAlgos=None, excludeAlgos=None, projectName=None, maxRuntimeSecs=3600.0, stoppingRounds=3,
                         stoppingTolerance=0.001, stoppingMetric=self._hc._jvm.hex.ScoreKeeper.StoppingMetric.valueOf("AUTO"), nfolds=5,
                         convertUnknownCategoricalLevelsToNa=True, seed=-1, sortMetric=None, balanceClasses=False,
                         classSamplingFactors=None, maxAfterBalanceSize=5.0, keepCrossValidationPredictions=True,
                         keepCrossValidationModels=True, maxModels=0)
        kwargs = get_input_kwargs(self)

        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCols=[], labelCol="label", allStringColumnsToCategorical=True, columnsToCategorical=[], ratio=1.0, foldCol=None,
                  weightCol=None, ignoredCols=[], includeAlgos=None, excludeAlgos=None, projectName=None, maxRuntimeSecs=3600.0, stoppingRounds=3,
                  stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5, convertUnknownCategoricalLevelsToNa=True, seed=-1,
                  sortMetric="AUTO", balanceClasses=False, classSamplingFactors=None, maxAfterBalanceSize=5.0, keepCrossValidationPredictions=True,
                  keepCrossValidationModels=True, maxModels=0, **deprecatedArgs):

        kwargs = get_input_kwargs(self)

        if "stoppingMetric" in kwargs:
            kwargs["stoppingMetric"] = self._hc._jvm.hex.ScoreKeeper.StoppingMetric.valueOf(kwargs["stoppingMetric"])

        if "projectName" in kwargs and kwargs["projectName"] is None:
            kwargs["projectName"] = ''.join(random.choice(string.ascii_letters) for i in range(30))

        if "excludeAlgos" in kwargs:
            jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
            kwargs["excludeAlgos"] = get_enum_array_from_str_array(kwargs["excludeAlgos"], jvm.ai.h2o.automl.Algo)

        if "includeAlgos" in kwargs:
            jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
            kwargs["includeAlgos"] = get_enum_array_from_str_array(kwargs["includeAlgos"], jvm.ai.h2o.automl.Algo)

        propagate_value_from_deprecated_property(kwargs, "weightsColumn", "weightCol")
        propagate_value_from_deprecated_property(kwargs, "foldColumn", "foldCol")
        propagate_value_from_deprecated_property(kwargs, "predictionCol", "labelCol")
        propagate_value_from_deprecated_property(kwargs, "ignoredColumns", "ignoredCols")

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["maxRuntimeSecs", "stoppingTolerance", "ratio", "maxAfterBalanceSize"]
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
    def __init__(self, ratio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                 nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                 seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False, quietMode=True, missingValuesHandling=None,
                 ntrees=50, nEstimators=0, maxDepth=6, minRows=1.0, minChildWeight=1.0, learnRate=0.3, eta=0.3, learnRateAnnealing=1.0,
                 sampleRate=1.0, subsample=1.0, colSampleRate=1.0, colSampleByLevel=1.0, colSampleRatePerTree=1.0, colsampleBytree=1.0,
                 maxAbsLeafnodePred=0.0, maxDeltaStep=0.0, scoreTreeInterval=0, initialScoreInterval=4000, scoreInterval=4000,
                 minSplitImprovement=0.0, gamma=0.0, nthread=-1, maxBins=256, maxLeaves=0,
                 minSumHessianInLeaf=100.0, minDataInLeaf=0.0, treeMethod="auto", growPolicy="depthwise",
                 booster="gbtree", dmatrixType="auto", regLambda=0.0, regAlpha=0.0, sampleType="uniform",
                 normalizeType="tree", rateDrop=0.0, oneDrop=False, skipDrop=0.0, gpuId=0, backend="auto",
                 foldCol=None, **deprecatedArgs):
        super(H2OXGBoost, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2OXGBoost", self.uid)

        self._setDefault(ratio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                         nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                         seed=-1, distribution=self._hc._jvm.hex.genmodel.utils.DistributionFamily.valueOf("AUTO"), convertUnknownCategoricalLevelsToNa=False,
                         quietMode=True, missingValuesHandling=None, ntrees=50, nEstimators=0, maxDepth=6, minRows=1.0, minChildWeight=1.0,
                         learnRate=0.3, eta=0.3, learnRateAnnealing=1.0, sampleRate=1.0, subsample=1.0, colSampleRate=1.0, colSampleByLevel=1.0,
                         colSampleRatePerTree=1.0, colsampleBytree=1.0, maxAbsLeafnodePred=0.0, maxDeltaStep=0.0, scoreTreeInterval=0,
                         initialScoreInterval=4000, scoreInterval=4000, minSplitImprovement=0.0, gamma=0.0, nthread=-1, maxBins=256, maxLeaves=0,
                         minSumHessianInLeaf=100.0, minDataInLeaf=0.0,
                         treeMethod=self._hc._jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.TreeMethod.valueOf("auto"),
                         growPolicy=self._hc._jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.GrowPolicy.valueOf("depthwise"),
                         booster=self._hc._jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.Booster.valueOf("gbtree"),
                         dmatrixType=self._hc._jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.DMatrixType.valueOf("auto"),
                         regLambda=0.0, regAlpha=0.0,
                         sampleType=self._hc._jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.DartSampleType.valueOf("uniform"),
                         normalizeType=self._hc._jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.DartNormalizeType.valueOf("tree"),
                         rateDrop=0.0, oneDrop=False, skipDrop=0.0, gpuId=0,
                         backend=self._hc._jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.Backend.valueOf("auto"),
                         foldCol=None)

        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, ratio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                  seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False, quietMode=True, missingValuesHandling=None,
                  ntrees=50, nEstimators=0, maxDepth=6, minRows=1.0, minChildWeight=1.0, learnRate=0.3, eta=0.3, learnRateAnnealing=1.0,
                  sampleRate=1.0, subsample=1.0, colSampleRate=1.0, colSampleByLevel=1.0, colSampleRatePerTree=1.0, colsampleBytree=1.0,
                  maxAbsLeafnodePred=0.0, maxDeltaStep=0.0, scoreTreeInterval=0, initialScoreInterval=4000, scoreInterval=4000,
                  minSplitImprovement=0.0, gamma=0.0, nthread=-1, maxBins=256, maxLeaves=0,
                  minSumHessianInLeaf=100.0, minDataInLeaf=0.0, treeMethod="auto", growPolicy="depthwise",
                  booster="gbtree", dmatrixType="auto", regLambda=0.0, regAlpha=0.0, sampleType="uniform",
                  normalizeType="tree", rateDrop=0.0, oneDrop=False, skipDrop=0.0, gpuId=0, backend="auto",
                  foldCol=None, **deprecatedArgs):
        kwargs = get_input_kwargs(self)

        if "distribution" in kwargs:
            kwargs["distribution"] = self._hc._jvm.hex.genmodel.utils.DistributionFamily.valueOf(kwargs["distribution"])

        if "treeMethod" in kwargs:
            kwargs["treeMethod"] = self._hc._jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.TreeMethod.valueOf(kwargs["treeMethod"])

        if "growPolicy" in kwargs:
            kwargs["growPolicy"] = self._hc._jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.GrowPolicy.valueOf(kwargs["growPolicy"])

        if "booster" in kwargs:
            kwargs["booster"] = self._hc._jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.Booster.valueOf(kwargs["booster"])

        if "dmatrixType" in kwargs:
            kwargs["dmatrixType"] = self._hc._jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.DMatrixType.valueOf(kwargs["dmatrixType"])

        if "sampleType" in kwargs:
            kwargs["sampleType"] = self._hc._jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.DartSampleType.valueOf(kwargs["sampleType"])

        if "normalizeType" in kwargs:
            kwargs["normalizeType"] = self._hc._jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.DartNormalizeType.valueOf(kwargs["normalizeType"])

        if "backend" in kwargs:
            kwargs["backend"] = self._hc._jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.Backend.valueOf(kwargs["backend"])

        propagate_value_from_deprecated_property(kwargs, "predictionCol", "labelCol")

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["ratio", "minRows", "minChildWeight", "learnRate", "eta", "learnRateAnnealing"
                        "sampleRate", "subsample", "colSampleRate", "colSampleByLevel", "colSampleRatePerTree",
                        "colsampleBytree", "maxAbsLeafnodePred", "maxDeltaStep", "minSplitImprovement", "gamma",
                        "minSumHessianInLeaf", "minDataInLeaf", "regLambda", "regAlpha", "rateDrop", "skipDrop"]
        set_double_values(kwargs, double_types)
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)


class H2OGLM(H2OGLMParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):
    @keyword_only
    def __init__(self, ratio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                 nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                 seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False,
                 standardize=True, family="gaussian", link="family_default", solver="AUTO", tweedieVariancePower=0.0,
                 tweedieLinkPower=0.0, alpha=None, lambda_=None, missingValuesHandling="MeanImputation",
                 prior=-1.0, lambdaSearch=False, nlambdas=-1, nonNegative=False, exactLambdas=False,
                 lambdaMinRatio=-1.0,maxIterations=-1, intercept=True, betaEpsilon=1e-4, objectiveEpsilon=-1.0,
                 gradientEpsilon=-1.0, objReg=-1.0, computePValues=False, removeCollinearCols=False,
                 interactions=None, interactionPairs=None, earlyStopping=True, foldCol=None, **deprecatedArgs):
        super(H2OGLM, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2OGLM", self.uid)

        self._setDefault(ratio=1.0, labelCol="label", weightCol=None,  featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                         nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                         seed=-1, distribution=self._hc._jvm.hex.genmodel.utils.DistributionFamily.valueOf("AUTO"),
                         convertUnknownCategoricalLevelsToNa=False,
                         standardize=True, family=self._hc._jvm.hex.glm.GLMModel.GLMParameters.Family.valueOf("gaussian"),
                         link=self._hc._jvm.hex.glm.GLMModel.GLMParameters.Link.valueOf("family_default"),
                         solver=self._hc._jvm.hex.glm.GLMModel.GLMParameters.Solver.valueOf("AUTO"), tweedieVariancePower=0.0,
                         tweedieLinkPower=0.0, alpha=None, lambda_=None,
                         missingValuesHandling=self._hc._jvm.hex.deeplearning.DeepLearningModel.DeepLearningParameters.MissingValuesHandling.valueOf("MeanImputation"),
                         prior=-1.0, lambdaSearch=False, nlambdas=-1, nonNegative=False, exactLambdas=False,
                         lambdaMinRatio=-1.0,maxIterations=-1, intercept=True, betaEpsilon=1e-4, objectiveEpsilon=-1.0,
                         gradientEpsilon=-1.0, objReg=-1.0, computePValues=False, removeCollinearCols=False,
                         interactions=None, interactionPairs=None, earlyStopping=True, foldCol=None)
        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, ratio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False,parallelizeCrossValidation=True,
                  seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False,
                  standardize=True, family="gaussian", link="family_default", solver="AUTO", tweedieVariancePower=0.0,
                  tweedieLinkPower=0.0, alpha=None, lambda_=None, missingValuesHandling="MeanImputation",
                  prior=-1.0, lambdaSearch=False, nlambdas=-1, nonNegative=False, exactLambdas=False,
                  lambdaMinRatio=-1.0,maxIterations=-1, intercept=True, betaEpsilon=1e-4, objectiveEpsilon=-1.0,
                  gradientEpsilon=-1.0, objReg=-1.0, computePValues=False, removeCollinearCols=False,
                  interactions=None, interactionPairs=None, earlyStopping=True, foldCol=None, **deprecatedArgs):
        kwargs = get_input_kwargs(self)

        if "distribution" in kwargs:
            kwargs["distribution"] = self._hc._jvm.hex.genmodel.utils.DistributionFamily.valueOf(kwargs["distribution"])

        if "family" in kwargs:
            kwargs["family"] = self._hc._jvm.hex.glm.GLMModel.GLMParameters.Family.valueOf(kwargs["family"])

        if "link" in kwargs:
            kwargs["link"] = self._hc._jvm.hex.glm.GLMModel.GLMParameters.Link.valueOf(kwargs["link"])

        if "solver" in kwargs:
            kwargs["solver"] = self._hc._jvm.hex.glm.GLMModel.GLMParameters.Solver.valueOf(kwargs["solver"])

        if "missingValuesHandling" in kwargs:
            kwargs["missingValuesHandling"] = self._hc._jvm.hex.deeplearning.DeepLearningModel.DeepLearningParameters.MissingValuesHandling.valueOf(kwargs["missingValuesHandling"])

        propagate_value_from_deprecated_property(kwargs, "predictionCol", "labelCol")
        propagate_value_from_deprecated_property(kwargs, "removeCollinearColumns", "removeCollinearCols")

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["ratio", "tweedieVariancePower", "tweedieLinkPower", "prior", "lambdaMinRatio",
                        "betaEpsilon", "objectiveEpsilon", "gradientEpsilon", "objReg"]
        set_double_values(kwargs, double_types)

        # We need to also map all doubles in the arrays
        if "alpha" in kwargs:
            kwargs["alpha"] = map(float, kwargs["alpha"])

        if "lambda_" in kwargs:
            kwargs["lambda_"] = map(float, kwargs["lambda_"])

        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)

class H2OGridSearch(H2OGridSearchParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):
    @keyword_only
    def __init__(self, featuresCols=[], algo=None, ratio=1.0, hyperParameters={}, labelCol="label", weightCol=None, allStringColumnsToCategorical=True,
                 columnsToCategorical=[], strategy="Cartesian", maxRuntimeSecs=0.0, maxModels=0, seed=-1,
                 stoppingRounds=0, stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=0, selectBestModelBy=None,
                 selectBestModelDecreasing=True, foldCol=None, convertUnknownCategoricalLevelsToNa=True, **deprecatedArgs):
        super(H2OGridSearch, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2OGridSearch", self.uid)

        self._setDefault(featuresCols=[], algo=None, ratio=1.0, hyperParameters={}, labelCol="label", weightCol=None, allStringColumnsToCategorical=True,
                         columnsToCategorical=[], strategy=self._hc._jvm.hex.grid.HyperSpaceSearchCriteria.Strategy.valueOf("Cartesian"),
                         maxRuntimeSecs=0.0, maxModels=0, seed=-1,
                         stoppingRounds=0, stoppingTolerance=0.001,
                         stoppingMetric=self._hc._jvm.hex.ScoreKeeper.StoppingMetric.valueOf("AUTO"), nfolds=0,
                         selectBestModelBy=None, selectBestModelDecreasing=True, foldCol=None,
                         convertUnknownCategoricalLevelsToNa=True)
        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCols=[], algo=None, ratio=1.0, hyperParameters={}, labelCol="label", weightCol=None, allStringColumnsToCategorical=True,
                  columnsToCategorical=[], strategy="Cartesian", maxRuntimeSecs=0.0, maxModels=0, seed=-1,
                  stoppingRounds=0, stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=0, selectBestModelBy=None,
                  selectBestModelDecreasing=True, foldCol=None, convertUnknownCategoricalLevelsToNa=True, **deprecatedArgs):
        kwargs = get_input_kwargs(self)


        if "stoppingMetric" in kwargs:
            kwargs["stoppingMetric"] = self._hc._jvm.hex.ScoreKeeper.StoppingMetric.valueOf(kwargs["stoppingMetric"])

        if "strategy" in kwargs:
            kwargs["strategy"] = self._hc._jvm.hex.grid.HyperSpaceSearchCriteria.Strategy.valueOf(kwargs["strategy"])

        if "selectBestModelBy" in kwargs and kwargs["selectBestModelBy"] is not None:
            kwargs["selectBestModelBy"] = self._hc._jvm.org.apache.spark.ml.h2o.algos.H2OGridSearchMetric.valueOf(kwargs["selectBestModelBy"])

        propagate_value_from_deprecated_property(kwargs, "predictionCol", "labelCol")

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["ratio", "stoppingTolerance", "maxRuntimeSecs"]
        set_double_values(kwargs, double_types)
        if "algo" in kwargs and kwargs["algo"] is not None:
            tmp = kwargs["algo"]
            del kwargs['algo']
            self._java_obj.setAlgo(tmp._java_obj)

        return self._set(**kwargs)

    def get_grid_models(self):
         return [ H2OMOJOModel(m) for m in self._java_obj.getGridModels()]

    def get_grid_models_params(self):
        return DataFrame(self._java_obj.getGridModelsParams(),  self._hc._sql_context)

    def get_grid_models_metrics(self):
        return DataFrame(self._java_obj.getGridModelsMetrics(),  self._hc._sql_context)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)
