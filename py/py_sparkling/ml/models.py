from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaModel, JavaWrapper
from pysparkling.initializer import *
from pyspark.sql import SparkSession
from pyspark.sql.column import Column
from .util import JavaH2OMLReadable
from h2o.utils.typechecks import assert_is_type
from pyspark.ml.param import *


class H2OMOJOSettings(JavaWrapper):

    def __init__(self,
                 predictionCol="prediction",
                 detailedPredictionCol="detailed_prediction",
                 withDetailedPredictionCol=False,
                 convertUnknownCategoricalLevelsToNa=False,
                 convertInvalidNumbersToNa=False,
                 namedMojoOutputColumns=True):
        assert_is_type(predictionCol, str)
        assert_is_type(detailedPredictionCol, str)
        assert_is_type(withDetailedPredictionCol, bool)
        assert_is_type(convertUnknownCategoricalLevelsToNa, bool)
        assert_is_type(convertInvalidNumbersToNa, bool)
        assert_is_type(namedMojoOutputColumns, bool)
        self.predictionCol = predictionCol
        self.detailedPredictionCol = detailedPredictionCol
        self.withDetailedPredictionCol = withDetailedPredictionCol
        self.convertUnknownCategoricalLevelsToNa = convertUnknownCategoricalLevelsToNa
        self.convertInvalidNumbersToNa = convertInvalidNumbersToNa
        self.namedMojoOutputColumns = namedMojoOutputColumns

    def toJavaObject(self):
        return self._new_java_obj("org.apache.spark.ml.h2o.models.H2OMOJOSettings",
                                  self.predictionCol,
                                  self.detailedPredictionCol,
                                  self.withDetailedPredictionCol,
                                  self.convertUnknownCategoricalLevelsToNa,
                                  self.convertInvalidNumbersToNa,
                                  self.namedMojoOutputColumns)

    @staticmethod
    def default():
        return H2OMOJOSettings()


class H2OMOJOModelBase(JavaModel, JavaMLWritable, JavaH2OMLReadable):

    def getConvertUnknownCategoricalLevelsToNa(self):
        return self._java_obj.getConvertUnknownCategoricalLevelsToNa()

    def getConvertInvalidNumbersToNa(self):
        return self._java_obj.getConvertInvalidNumbersToNa()

    def getFeaturesCols(self):
        return list(self._java_obj.getFeaturesCols())

    def getPredictionCol(self):
        return self._java_obj.getPredictionCol()

    def getDetailedPredictionCol(self):
        return self._java_obj.getDetailedPredictionCol()

    def getWithDetailedPredictionCol(self):
        return self._java_obj.getWithDetailedPredictionCol()

    def getModelDetails(self):
        return self._java_obj.getModelDetails()

    def getNamedMojoOutputColumns(self):
        return self._java_obj.getNamedMojoOutputColumns()

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
