from pyspark import keyword_only
from pyspark.ml.util import JavaMLWritable, JavaMLReader, MLReadable
from pyspark.ml.wrapper import JavaEstimator
from pyspark.sql import SparkSession

from pysparkling import *
from pysparkling.ml.params import H2OGBMParams, H2ODeepLearningParams, H2OAutoMLParams
from .models import H2OGBMModel, H2ODeepLearningModel, H2OAutoMLModel

def get_input_kwargs(self, spark_context):
    if spark_context.version == "2.1.0":
        return self.__init__._input_kwargs
    else:
        # on newer versions we need to use the following variant
        return self._input_kwargs


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

        kwargs = get_input_kwargs(self, self._hc._sc)
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

        kwargs = get_input_kwargs(self, self._hc._sc)

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

        kwargs = get_input_kwargs(self, self._hc._sc)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, ratio=1.0, predictionCol="predictionCol", featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                  seed=-1, distribution="AUTO", epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200], reproducible=False,
                  convertUnknownCategoricalLevelsToNa=False):
        
        kwargs = get_input_kwargs(self, self._hc._sc)

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
                 ignoredColumns=[], tryMutations=True, excludeAlgos=None, projectName=None, loss="AUTO", maxRuntimeSecs=3600.0, stoppingRounds=3,
                 stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5, convertUnknownCategoricalLevelsToNa=False, seed=-1):
        super(H2OAutoML, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("py_sparkling.ml.algos.H2OAutoML",
                                            self.uid,
                                            self._hc._jhc.h2oContext(),
                                            self._hc._jsql_context)

        self._setDefault(predictionCol="predictionCol", allStringColumnsToCategorical=True, columnsToCategorical=[], ratio=1.0, foldColumn=None, weightsColumn=None,
                         ignoredColumns=[], tryMutations=True, excludeAlgos=None, projectName=None, loss="AUTO", maxRuntimeSecs=3600.0, stoppingRounds=3,
                         stoppingTolerance=0.001, stoppingMetric=self._hc._jvm.hex.ScoreKeeper.StoppingMetric.valueOf("AUTO"), nfolds=5,
                         convertUnknownCategoricalLevelsToNa=False, seed=-1)
        kwargs = get_input_kwargs(self, self._hc._sc)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, predictionCol="predictionCol", allStringColumnsToCategorical=True, columnsToCategorical=[], ratio=1.0, foldColumn=None, weightsColumn=None,
                  ignoredColumns=[], tryMutations=True, excludeAlgos=None, projectName=None, loss="AUTO", maxRuntimeSecs=3600.0, stoppingRounds=3,
                  stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5, convertUnknownCategoricalLevelsToNa=False, seed=-1):
        kwargs = get_input_kwargs(self, self._hc._sc)

        if "stoppingMetric" in kwargs:
            kwargs["stoppingMetric"] = self._hc._jvm.hex.ScoreKeeper.StoppingMetric.valueOf(kwargs["stoppingMetric"])

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        double_types = ["maxRuntimeSecs", "stoppingTolerance", "ratio"]
        set_double_values(kwargs, double_types)
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OAutoMLModel(java_model)
