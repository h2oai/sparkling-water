from ai.h2o.sparkling.Initializer import Initializer
from pyspark.ml.param import *
from pyspark.ml.util import _jvm


class H2OBinaryModel:

    def __init__(self, javaModel):
        self._java_obj = javaModel
        self.modelId = javaModel.modelId()

    @staticmethod
    def read(path):
        # We need to make sure that Sparkling Water classes are available on the Spark
        # driver and executor paths
        Initializer.load_sparkling_jar()
        javaModel = _jvm().ai.h2o.sparkling.ml.models.H2OBinaryModel.read(path)
        return H2OBinaryModel(javaModel)

    def write(self, path):
        self._java_obj.write(path)
