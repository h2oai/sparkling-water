from pyspark import keyword_only
from pyspark.ml.util import JavaMLWritable, JavaMLReader, MLReadable
from pyspark.ml.wrapper import JavaEstimator
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from pysparkling import *
from pysparkling.ml.params import H2OGBMParams, H2ODeepLearningParams, H2OAutoMLParams, H2OXGBoostParams
from .models import H2OGBMModel, H2ODeepLearningModel, H2OAutoMLModel, H2OXGBoostModel

java_max_double_value = (2-2**(-52))*(2**1023)


def set_double_values(kwargs, values):
    for v in values:
        if v in kwargs:
            kwargs[v] = float(kwargs[v])


class JavaH2OMLReadable(MLReadable):
    """
    Special version of JavaMLReadable to be able to load pipelines exported together with H2O pipeline stages
    """
    def __init__(self):
        super(JavaH2OMLReadable, self).__init__()

    """
    (Private) Mixin for instances that provide JavaMLReader.
    """

    @classmethod
    def read(cls):
        """Returns an MLReader instance for this class."""
        return JavaH2OMLReader(cls)


class JavaH2OMLReader(JavaMLReader):

    def __init__(self, clazz):
        super(JavaH2OMLReader, self).__init__(clazz)
        self._clazz = clazz
        self._jread = self._load_java_obj(clazz).read()

    @classmethod
    def _java_loader_class(cls, clazz):
        """
        Returns the full class name of the Java ML instance. The default
        implementation replaces "pyspark" by "org.apache.spark" in
        the Python full class name.
        """
        java_package = clazz.__module__.replace("pysparkling", "py_sparkling")
        if clazz.__name__ in ("Pipeline", "PipelineModel"):
            # Remove the last package name "pipeline" for Pipeline and PipelineModel.
            java_package = ".".join(java_package.split(".")[0:-1])
        return java_package + "." + clazz.__name__


class H2OGBM(H2OGBMParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):
    @keyword_only
    def __init__(self, ratio=1.0, predictionCol="predictionCol", featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[], nfolds=0,
                 keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                 seed=-1, distribution="AUTO", ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                 histogramType="AUTO", r2Stopping=java_max_double_value,
                 nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                 sampleRate=1.0, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                 learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=java_max_double_value,
                 predNoiseBandwidth=0.0, convertUnknownCategoricalLevelsToNa=False):
        super(H2OGBM, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2OGBM",
                                            self.uid,
                                            self._hc._jhc.h2oContext(),
                                            self._hc._jsql_context)

        self._setDefault(ratio=1.0, predictionCol="predictionCol", featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                         nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                         seed=-1, distribution=self._hc._jvm.hex.genmodel.utils.DistributionFamily.valueOf("AUTO"),
                         ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                         histogramType=self._hc._jvm.hex.tree.SharedTreeModel.SharedTreeParameters.HistogramType.valueOf("AUTO"),
                         r2Stopping=self._hc._jvm.Double.MAX_VALUE, nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                         sampleRate=1.0, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                         learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=self._hc._jvm.Double.MAX_VALUE,
                         predNoiseBandwidth=0.0, convertUnknownCategoricalLevelsToNa=False)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, ratio=1.0, predictionCol="predictionCol", featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False,parallelizeCrossValidation=True,
                  seed=-1, distribution="AUTO", ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                  histogramType="AUTO", r2Stopping=java_max_double_value,
                  nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                  sampleRate=1.0, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                  learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=java_max_double_value,
                  predNoiseBandwidth=0.0, convertUnknownCategoricalLevelsToNa=False):
        kwargs = self._input_kwargs

        if "distribution" in kwargs:
            kwargs["distribution"] = self._hc._jvm.hex.genmodel.utils.DistributionFamily.valueOf(kwargs["distribution"])
        if "histogramType" in kwargs:
            kwargs["histogramType"] = self._hc._jvm.hex.tree.SharedTreeModel.SharedTreeParameters.HistogramType.valueOf(kwargs["histogramType"])

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
        return H2OGBMModel(java_model)


class H2ODeepLearning(H2ODeepLearningParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, ratio=1.0, predictionCol="predictionCol", featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                 nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                 seed=-1, distribution="AUTO", epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200], reproducible=False,
                 convertUnknownCategoricalLevelsToNa=False):
        super(H2ODeepLearning, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2ODeepLearning",
                                            self.uid,
                                            self._hc._jhc.h2oContext(),
                                            self._hc._jsql_context)

        self._setDefault(ratio=1.0, predictionCol="predictionCol", featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                         nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                         seed=-1, distribution=self._hc._jvm.hex.genmodel.utils.DistributionFamily.valueOf("AUTO"),
                         epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200], reproducible=False, convertUnknownCategoricalLevelsToNa=False)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, ratio=1.0, predictionCol="predictionCol", featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                  seed=-1, distribution="AUTO", epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200], reproducible=False, convertUnknownCategoricalLevelsToNa=False):
        kwargs = self._input_kwargs

        if "distribution" in kwargs:
            kwargs["distribution"] = self._hc._jvm.hex.genmodel.utils.DistributionFamily.valueOf(kwargs["distribution"])

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["ratio", "epochs", "l1", "l2"]
        set_double_values(kwargs, double_types)

        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2ODeepLearningModel(java_model)


class H2OAutoML(H2OAutoMLParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, predictionCol="predictionCol", allStringColumnsToCategorical=True, columnsToCategorical=[], ratio=1.0, foldColumn=None, weightsColumn=None,
                 ignoredColumns=[], excludeAlgos=None, projectName=None, maxRuntimeSecs=3600.0, stoppingRounds=3,
                 stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5, convertUnknownCategoricalLevelsToNa=False, seed=-1,
                 sortMetric="AUTO", balanceClasses=False, classSamplingFactors=None, maxAfterBalanceSize=5.0,
                 keepCrossValidationPredictions=True, keepCrossValidationModels=True, maxModels=0):
        super(H2OAutoML, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2OAutoML",
                                            self.uid,
                                            self._hc._jhc.h2oContext(),
                                            self._hc._jsql_context)

        self._setDefault(predictionCol="predictionCol", allStringColumnsToCategorical=True, columnsToCategorical=[], ratio=1.0, foldColumn=None, weightsColumn=None,
                         ignoredColumns=[], excludeAlgos=None, projectName=None, maxRuntimeSecs=3600.0, stoppingRounds=3,
                         stoppingTolerance=0.001, stoppingMetric=self._hc._jvm.hex.ScoreKeeper.StoppingMetric.valueOf("AUTO"), nfolds=5,
                         convertUnknownCategoricalLevelsToNa=False, seed=-1, sortMetric=None, balanceClasses=False,
                         classSamplingFactors=None, maxAfterBalanceSize=5.0, keepCrossValidationPredictions=True,
                         keepCrossValidationModels=True, maxModels=0)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, predictionCol="predictionCol", allStringColumnsToCategorical=True, columnsToCategorical=[], ratio=1.0, foldColumn=None, weightsColumn=None,
                  ignoredColumns=[], excludeAlgos=None, projectName=None, maxRuntimeSecs=3600.0, stoppingRounds=3,
                  stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5, convertUnknownCategoricalLevelsToNa=False, seed=-1,
                  sortMetric="AUTO", balanceClasses=False, classSamplingFactors=None, maxAfterBalanceSize=5.0, keepCrossValidationPredictions=True,
                  keepCrossValidationModels=True, maxModels=0):
        kwargs = self._input_kwargs

        if "stoppingMetric" in kwargs:
            kwargs["stoppingMetric"] = self._hc._jvm.hex.ScoreKeeper.StoppingMetric.valueOf(kwargs["stoppingMetric"])

        if "sortMetric" in kwargs:
            kwargs["sortMetric"] = None

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["maxRuntimeSecs", "stoppingTolerance", "ratio", "maxAfterBalanceSize"]
        set_double_values(kwargs, double_types)
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OAutoMLModel(java_model)

    def leaderboard(self):
        leaderboard_java = self._java_obj.leaderboard()
        if leaderboard_java.isDefined():
            return DataFrame(leaderboard_java.get(), self._hc._sql_context)
        else:
            return None

class H2OXGBoost(H2OXGBoostParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, ratio=1.0, predictionCol="predictionCol", featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                 nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                 seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False, quietMode=True, missingValuesHandling=None,
                 ntrees=50, nEstimators=0, maxDepth=6, minRows=1.0, minChildWeight=1.0, learnRate=0.3, eta=0.3, learnRateAnnealing=1.0,
                 sampleRate=1.0, subsample=1.0, colSampleRate=1.0, colSampleByLevel=1.0, colSampleRatePerTree=1.0, colsampleBytree=1.0,
                 maxAbsLeafnodePred=0.0, maxDeltaStep=0.0, scoreTreeInterval=0, initialScoreInterval=4000, scoreInterval=4000,
                 minSplitImprovement=0.0, gamma=0.0, nthread=-1, maxBins=256, maxLeaves=0,
                 minSumHessianInLeaf=100.0, minDataInLeaf=0.0, treeMethod="auto", growPolicy="depthwise",
                 booster="gbtree", dmatrixType="auto", regLambda=0.0, regAlpha=0.0, sampleType="uniform",
                 normalizeType="tree", rateDrop=0.0, oneDrop=False, skipDrop=0.0, gpuId=0, backend="auto"):
        super(H2OXGBoost, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2OXGBoost",
                                            self.uid,
                                            self._hc._jhc.h2oContext(),
                                            self._hc._jsql_context)

        self._setDefault(ratio=1.0, predictionCol="predictionCol", featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
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
                         backend=self._hc._jvm.hex.tree.xgboost.XGBoostModel.XGBoostParameters.Backend.valueOf("auto"))

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, ratio=1.0, predictionCol="predictionCol", featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                  seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False, quietMode=True, missingValuesHandling=None,
                  ntrees=50, nEstimators=0, maxDepth=6, minRows=1.0, minChildWeight=1.0, learnRate=0.3, eta=0.3, learnRateAnnealing=1.0,
                  sampleRate=1.0, subsample=1.0, colSampleRate=1.0, colSampleByLevel=1.0, colSampleRatePerTree=1.0, colsampleBytree=1.0,
                  maxAbsLeafnodePred=0.0, maxDeltaStep=0.0, scoreTreeInterval=0, initialScoreInterval=4000, scoreInterval=4000,
                  minSplitImprovement=0.0, gamma=0.0, nthread=-1, maxBins=256, maxLeaves=0,
                  minSumHessianInLeaf=100.0, minDataInLeaf=0.0, treeMethod="auto", growPolicy="depthwise",
                  booster="gbtree", dmatrixType="auto", regLambda=0.0, regAlpha=0.0, sampleType="uniform",
                  normalizeType="tree", rateDrop=0.0, oneDrop=False, skipDrop=0.0, gpuId=0, backend="auto"):
        kwargs = self._input_kwargs

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

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["ratio", "minRows", "minChildWeight", "learnRate", "eta", "learnRateAnnealing"
                        "sampleRate", "subsample", "colSampleRate", "colSampleByLevel", "colSampleRatePerTree",
                        "colsampleBytree", "maxAbsLeafnodePred", "maxDeltaStep", "minSplitImprovement", "gamma",
                        "minSumHessianInLeaf", "minDataInLeaf", "regLambda", "regAlpha", "rateDrop", "skipDrop"]
        set_double_values(kwargs, double_types)
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OXGBoostModel(java_model)