from pyspark import since, keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaTransformer, _jvm
from pyspark.sql import SparkSession
from pysparkling import *
from .params import H2OAlgorithmParams


class H2OGBM(JavaEstimator, H2OAlgorithmParams, JavaMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, ratio=1.0, featuresCols=[], predictionCol=None):
        super(H2OGBM, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.h2o.algos.H2OGBM",
                                            self.uid,
                                            H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jhc.h2oContext(),
                                            H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jsql_context
                                            )
        self._setDefault( ratio=1.0, featuresCols=[], predictionCol=None)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, ratio=1.0, featuresCols=[], predictionCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OGBMModel(java_model)


class H2OGBMModel(JavaModel, JavaMLWritable, JavaMLReadable):
    pass

class H2ODeepLearning(JavaEstimator, H2OAlgorithmParams, JavaMLReadable, JavaMLWritable):

    epochs = Param(Params._dummy(), "epochs", "The number of passes over the training dataset to be carried out")

    l1 = Param(Params._dummy(), "l1", "A regularization method that constrains the absolute value of the weights and"
                                      " has the net effect of dropping some weights (setting them to zero) from"
                                      " a model to reduce complexity and avoid overfitting.")

    l2 = Param(Params._dummy(), "l2", "A regularization method that constrains the sum of the squared weights."
                                      " This method introduces bias into parameter estimates, but frequently"
                                      " produces substantial gains in modeling as estimate variance is reduced.")

    hidden = Param(Params._dummy(), "hidden", "The number and size of each hidden layer in the model")


    @keyword_only
    def __init__(self, ratio=1.0, featuresCols=[], predictionCol=None, epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200]):
        super(H2ODeepLearning, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.h2o.algos.H2ODeepLearning",
                                            self.uid,
                                            H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jhc.h2oContext(),
                                            H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jsql_context
                                            )
        self._setDefault( ratio=1.0, featuresCols=[], predictionCol=None, epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200])
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, ratio=1.0, featuresCols=[], predictionCol=None, epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200]):
        kwargs = self._input_kwargs

        kwargs["epochs"] = float(kwargs["epochs"]) # we need to convert explicitly to float
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2ODeepLearningModel(java_model)



class H2ODeepLearningModel(JavaModel, JavaMLWritable, JavaMLReadable):
    pass