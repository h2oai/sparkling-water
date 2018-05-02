from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaModel
from pysparkling.initializer import *
from pyspark.sql import SparkSession


class H2OGBMModel(JavaModel, JavaMLWritable, JavaMLReadable):
    pass


class H2ODeepLearningModel(JavaModel, JavaMLWritable, JavaMLReadable):
    pass


class H2OAutoMLModel(JavaModel, JavaMLWritable, JavaMLReadable):
    pass


class H2OMOJOModel(JavaModel, JavaMLWritable, JavaMLReadable):

    @staticmethod
    def create_from_mojo(path_to_mojo):
        spark_session = SparkSession.builder.getOrCreate()
        # We need to make sure that Sparkling Water classes are available on the Spark driver and executor paths
        Initializer.load_sparkling_jar(spark_session._sc)
        return H2OMOJOModel(spark_session._jvm.org.apache.spark.ml.h2o.models.JavaH2OMOJOModelHelper.createFromMojo(path_to_mojo))

    def predict(self, dataframe):
        return self.transform(dataframe)

    def getConvertUnknownCategoricalLevelsToNa(self):
        return self._java_obj.getConvertUnknownCategoricalLevelsToNa()

    def setConvertUnknownCategoricalLevelsToNa(self, value):
        self._java_obj.setConvertUnknownCategoricalLevelsToNa(value)
        return self


class H2OMOJOPipelineModel(JavaModel, JavaMLWritable, JavaMLReadable):

    @staticmethod
    def create_from_mojo(path_to_mojo):
        spark_session = SparkSession.builder.getOrCreate()
        # We need to make sure that Sparkling Water classes are available on the Spark driver and executor paths
        Initializer.load_sparkling_jar(spark_session._sc)
        return H2OMOJOPipelineModel(spark_session._jvm.org.apache.spark.ml.h2o.models.JavaH2OMOJOPipelineModelHelper.createFromMojo(path_to_mojo))

    def predict(self, dataframe):
        return self.transform(dataframe)
