from pyspark import since, keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaTransformer, _jvm
from pyspark.sql import SparkSession
from pysparkling import *
from .params import H2OGBMParams, H2ODeepLearningParams, H2OAutoMLParams

java_max_double_value = (2-2**(-52))*(2**1023)

def set_double_values(kwargs, values):
    for v in values:
        if v in kwargs:
            kwargs[v] = float(kwargs[v])

class H2OGBM(JavaEstimator, H2OGBMParams, JavaMLReadable, JavaMLWritable):
    @keyword_only
    def __init__(self, ratio=1.0, predictionCol=None, featuresCols=[], allStringColumnsToCategorical=True, nfolds=0,
                 keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                 seed=-1, distribution="AUTO", ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                 histogramType="AUTO", r2Stopping=java_max_double_value,
                 nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                 sampleRate=1.0, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                 learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=java_max_double_value,
                 predNoiseBandwidth=0.0, convertUnknownCategoricalLevelsToNa=False):
        super(H2OGBM, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("org.apache.spark.ml.h2o.algos.H2OGBM",
                                            self.uid,
                                            self._hc._jhc.h2oContext(),
                                            self._hc._jsql_context)

        self._setDefault(ratio=1.0, predictionCol=None, featuresCols=[], allStringColumnsToCategorical=True,
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
    def setParams(self, ratio=1.0, predictionCol=None, featuresCols=[], allStringColumnsToCategorical=True,
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

class H2OGBMModel(JavaModel, JavaMLWritable, JavaMLReadable):
    pass

class H2ODeepLearning(JavaEstimator, H2ODeepLearningParams, JavaMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, ratio=1.0, predictionCol=None, featuresCols=[], allStringColumnsToCategorical=True,
                 nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                 seed=-1, distribution="AUTO", epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200], reproducible=False,
                 convertUnknownCategoricalLevelsToNa=False):
        super(H2ODeepLearning, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("org.apache.spark.ml.h2o.algos.H2ODeepLearning",
                                            self.uid,
                                            self._hc._jhc.h2oContext(),
                                            self._hc._jsql_context)

        self._setDefault(ratio=1.0, predictionCol=None, featuresCols=[], allStringColumnsToCategorical=True,
                         nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                         seed=-1, distribution=self._hc._jvm.hex.genmodel.utils.DistributionFamily.valueOf("AUTO"),
                         epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200], reproducible=False, convertUnknownCategoricalLevelsToNa=False)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, ratio=1.0, predictionCol=None, featuresCols=[], allStringColumnsToCategorical=True,
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


class H2ODeepLearningModel(JavaModel, JavaMLWritable, JavaMLReadable):
    pass

class H2OAutoML(JavaEstimator, H2OAutoMLParams, JavaMLWritable, JavaMLReadable):

    @keyword_only
    def __init__(self, predictionCol=None, allStringColumnsToCategorical=True, ratio=1.0, foldColumn=None, weightsColumn=None,
                       ignoredColumns=[], tryMutations=True, excludeAlgos=None, projectName=None, loss="AUTO", maxRuntimeSecs=3600.0, stoppingRounds=3,
                       stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5, convertUnknownCategoricalLevelsToNa=False):
        super(H2OAutoML, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("org.apache.spark.ml.h2o.algos.H2OAutoML",
                                        self.uid,
                                        self._hc._jhc.h2oContext(),
                                        self._hc._jsql_context)

        self._setDefault(predictionCol=None, allStringColumnsToCategorical=True, ratio=1.0, foldColumn=None, weightsColumn=None,
                         ignoredColumns=[], tryMutations=True, excludeAlgos=None, projectName=None, loss="AUTO", maxRuntimeSecs=3600.0, stoppingRounds=3,
                         stoppingTolerance=0.001, stoppingMetric=self._hc._jvm.hex.ScoreKeeper.StoppingMetric.valueOf("AUTO"), nfolds=5, convertUnknownCategoricalLevelsToNa=False)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, predictionCol=None, allStringColumnsToCategorical=True, ratio=1.0, foldColumn=None, weightsColumn=None,
                  ignoredColumns=[], tryMutations=True, excludeAlgos=None, projectName=None, loss="AUTO", maxRuntimeSecs=3600.0, stoppingRounds=3,
                  stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5, convertUnknownCategoricalLevelsToNa=False):
        kwargs = self._input_kwargs

        if "stoppingMetric" in kwargs:
            kwargs["stoppingMetric"] = self._hc._jvm.hex.ScoreKeeper.StoppingMetric.valueOf(kwargs["stoppingMetric"])

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        double_types = ["maxRuntimeSecs", "stoppingTolerance", "ratio"]
        set_double_values(kwargs, double_types)
        return self._set(**kwargs)


    def _create_model(self, java_model):
        return H2OAutoMLModel(java_model)

class H2OAutoMLModel(JavaModel, JavaMLWritable, JavaMLReadable):
    pass