from pyspark import keyword_only
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator
from pyspark.ml.wrapper import JavaModel
from pyspark.sql import SparkSession

import py_sparkling.ml.algos
import py_sparkling.ml.models
from pysparkling import *
from .params import H2OAutoMLParams


def set_double_values(kwargs, values):
    for v in values:
        if v in kwargs:
            kwargs[v] = float(kwargs[v])

class H2OGBM(py_sparkling.ml.algos.H2OGBM):
   pass

class H2ODeepLearning(py_sparkling.ml.algos.H2ODeepLearning):
    pass


class H2OAutoML(H2OAutoMLParams, JavaEstimator, JavaMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, predictionCol=None, allStringColumnsToCategorical=True, columnsToCategorical=[], ratio=1.0, foldColumn=None, weightsColumn=None,
                       ignoredColumns=[], tryMutations=True, excludeAlgos=None, projectName=None, loss="AUTO", maxRuntimeSecs=3600.0, stoppingRounds=3,
                       stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5, convertUnknownCategoricalLevelsToNa=False, seed=-1):
        super(H2OAutoML, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("org.apache.spark.ml.h2o.algos.H2OAutoML",
                                        self.uid,
                                        self._hc._jhc.h2oContext(),
                                        self._hc._jsql_context)

        self._setDefault(predictionCol=None, allStringColumnsToCategorical=True, columnsToCategorical=[], ratio=1.0, foldColumn=None, weightsColumn=None,
                         ignoredColumns=[], tryMutations=True, excludeAlgos=None, projectName=None, loss="AUTO", maxRuntimeSecs=3600.0, stoppingRounds=3,
                         stoppingTolerance=0.001, stoppingMetric=self._hc._jvm.hex.ScoreKeeper.StoppingMetric.valueOf("AUTO"), nfolds=5,
                         convertUnknownCategoricalLevelsToNa=False, seed=-1)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, predictionCol=None, allStringColumnsToCategorical=True, columnsToCategorical=[], ratio=1.0, foldColumn=None, weightsColumn=None,
                  ignoredColumns=[], tryMutations=True, excludeAlgos=None, projectName=None, loss="AUTO", maxRuntimeSecs=3600.0, stoppingRounds=3,
                  stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5, convertUnknownCategoricalLevelsToNa=False, seed=-1):
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