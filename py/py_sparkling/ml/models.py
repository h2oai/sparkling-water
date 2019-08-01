from pyspark.ml.param import *
from pyspark.ml.util import JavaMLWritable
from pyspark.ml.wrapper import JavaModel
from pyspark.sql import SparkSession
from pyspark.sql.column import Column

from ai.h2o.sparkling import Initializer
from ai.h2o.sparkling.ml.params import H2OMOJOAlgoSharedParams
from ai.h2o.sparkling.ml.models import H2OMOJOSettings
from py_sparkling.ml.util import JavaH2OMLReadable


class H2OMOJOModelBase(H2OMOJOAlgoSharedParams, JavaModel, JavaMLWritable, JavaH2OMLReadable):

    # Overriding the method to avoid changes on the companion Java object
    def _transfer_params_to_java(self):
        pass

class H2OMOJOModel(H2OMOJOModelBase):

    @staticmethod
    def createFromMojo(pathToMojo, settings=H2OMOJOSettings.default()):
        spark_session = SparkSession.builder.getOrCreate()
        # We need to make sure that Sparkling Water classes are available on the Spark driver and executor paths
        Initializer.load_sparkling_jar(spark_session._sc)
        javaModel = spark_session._jvm.py_sparkling.ml.models.H2OMOJOModel.createFromMojo(pathToMojo,
                                                                                          settings.toJavaObject())
        return H2OMOJOModel(javaModel)

    def getModelDetails(self):
        return self._java_obj.getModelDetails()



class H2OMOJOPipelineModel(H2OMOJOModelBase):

    @staticmethod
    def createFromMojo(pathToMojo, settings=H2OMOJOSettings.default()):
        spark_session = SparkSession.builder.getOrCreate()
        # We need to make sure that Sparkling Water classes are available on the Spark driver and executor paths
        Initializer.load_sparkling_jar(spark_session._sc)
        javaModel = spark_session._jvm.py_sparkling.ml.models.H2OMOJOPipelineModel.createFromMojo(pathToMojo,
                                                                                                  settings.toJavaObject())
        return H2OMOJOPipelineModel(javaModel)

    def selectPredictionUDF(self, column):
        java_col = self._java_obj.selectPredictionUDF(column)
        return Column(java_col)
