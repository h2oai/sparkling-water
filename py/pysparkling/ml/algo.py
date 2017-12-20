from pyspark import since, keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaTransformer, _jvm
from pyspark.sql import SparkSession
from pysparkling import *
from .params import H2OGBMParams, H2ODeepLearningParams

class H2OGBM(JavaEstimator, H2OGBMParams, JavaMLReadable, JavaMLWritable):
    @keyword_only
    def __init__(self, ratio=1.0, predictionCol=None, featuresCols=[], allStringColumnsToCategorical=True, nfolds=0,
                 keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=False,
                 seed=-1, distribution="AUTO", ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                 histogramType="AUTO", r2Stopping=SparkSession.builder.getOrCreate()._jvm.Double.MAX_VALUE,
                 nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                 sampleRate=0.632, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                 learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=SparkSession.builder.getOrCreate()._jvm.Double.MAX_VALUE,
                 predNoiseBandwidth=0.0):
        super(H2OGBM, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("org.apache.spark.ml.h2o.algos.H2OGBM",
                                            self.uid,
                                            self._hc._jhc.h2oContext(),
                                            self._hc._jsql_context)

        self._setDefault(ratio=1.0, predictionCol=None, featuresCols=[], allStringColumnsToCategorical=True,
                         nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=False,
                         seed=-1, distribution=self._hc._jvm.hex.genmodel.utils.DistributionFamily.valueOf("AUTO"),
                         ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                         histogramType=self._hc._jvm.hex.tree.SharedTreeModel.SharedTreeParameters.HistogramType.valueOf("AUTO"),
                         r2Stopping=self._hc._jvm.Double.MAX_VALUE, nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                         sampleRate=0.632, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                         learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=self._hc._jvm.Double.MAX_VALUE,
                         predNoiseBandwidth=0.0)
        kwargs = self._input_kwargs
        print self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, ratio=1.0, predictionCol=None, featuresCols=[], allStringColumnsToCategorical=True,
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False,parallelizeCrossValidation=False,
                  seed=-1, distribution="AUTO", ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
                  histogramType="AUTO", r2Stopping=SparkSession.builder.getOrCreate()._jvm.Double.MAX_VALUE,
                  nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
                  sampleRate=0.632, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
                  learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=SparkSession.builder.getOrCreate()._jvm.Double.MAX_VALUE,
                  predNoiseBandwidth=0.0):
        kwargs = self._input_kwargs

        # we need to convert few parameters explicitly to float as py4j can't handle that
        if "minRows" in kwargs:
            kwargs["minRows"] = float(kwargs["minRows"])
        if "predNoiseBandwidth" in kwargs:
            kwargs["predNoiseBandwidth"] = float(kwargs["predNoiseBandwidth"])
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OGBMModel(java_model)

class H2OGBMModel(JavaModel, JavaMLWritable, JavaMLReadable):
    pass

class H2ODeepLearning(JavaEstimator, H2ODeepLearningParams, JavaMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, ratio=1.0, predictionCol=None, featuresCols=[], allStringColumnsToCategorical=True,
                 nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False,parallelizeCrossValidation=False,
                 seed=-1, distribution="AUTO", epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200]):
        super(H2ODeepLearning, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("org.apache.spark.ml.h2o.algos.H2ODeepLearning",
                                            self.uid,
                                            self._hc._jhc.h2oContext(),
                                            self._hc._jsql_context)

        self._setDefault(ratio=1.0, predictionCol=None, featuresCols=[], allStringColumnsToCategorical=True,
                         nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False,parallelizeCrossValidation=False,
                         seed=-1, distribution=self._hc._jvm.hex.genmodel.utils.DistributionFamily.valueOf("AUTO"),
                         epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200])
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, ratio=1.0, predictionCol=None, featuresCols=[], allStringColumnsToCategorical=True,
                  nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False,parallelizeCrossValidation=False,
                  seed=-1, distribution="AUTO", epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200]):
        kwargs = self._input_kwargs

        # we need to convert few parameters explicitly to float as py4j can't handle that
        if "epochs" in kwargs:
            kwargs["epochs"] = float(kwargs["epochs"])
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2ODeepLearningModel(java_model)


class H2ODeepLearningModel(JavaModel, JavaMLWritable, JavaMLReadable):
    pass