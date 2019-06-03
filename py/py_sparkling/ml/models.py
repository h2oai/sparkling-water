from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaModel, JavaWrapper
from pysparkling.initializer import *
from pyspark.sql import SparkSession
from pyspark.sql.column import Column
from .util import JavaH2OMLReadable
from h2o.utils.typechecks import assert_is_type
from pyspark.ml.param import *
import warnings


class H2OMOJOSettings(JavaWrapper):

    def __init__(self,
                 convertUnknownCategoricalLevelsToNa = False,
                 convertInvalidNumbersToNa = False,
                 namedMojoOutputColumns = True):
        assert_is_type(convertUnknownCategoricalLevelsToNa, bool)
        assert_is_type(convertInvalidNumbersToNa, bool)
        assert_is_type(namedMojoOutputColumns, bool)
        self.convertUnknownCategoricalLevelsToNa = convertUnknownCategoricalLevelsToNa
        self.convertInvalidNumbersToNa = convertInvalidNumbersToNa
        self.namedMojoOutputColumns = namedMojoOutputColumns

    def toJavaObject(self):
        return self._new_java_obj("org.apache.spark.ml.h2o.models.H2OMOJOSettings",
                           self.convertUnknownCategoricalLevelsToNa,
                           self.convertInvalidNumbersToNa,
                           self.namedMojoOutputColumns)

    @staticmethod
    def default():
        return H2OMOJOSettings()


class H2OMOJOModel(JavaModel, JavaMLWritable, JavaH2OMLReadable):

    @staticmethod
    def createFromMojo(pathToMojo, settings = H2OMOJOSettings.default()):
        spark_session = SparkSession.builder.getOrCreate()
        # We need to make sure that Sparkling Water classes are available on the Spark driver and executor paths
        Initializer.load_sparkling_jar(spark_session._sc)
        javaModel = spark_session._jvm.py_sparkling.ml.models.H2OMOJOModel.createFromMojo(pathToMojo, settings.toJavaObject())
        return H2OMOJOModel(javaModel)

    @staticmethod
    def create_from_mojo(path_to_mojo):
        warnings.warn("The method 'create_from_mojo' is deprecated. Use 'createFromMojo' instead!")
        return H2OMOJOModel.createFromMojo(path_to_mojo)

    def predict(self, dataframe):
        warnings.warn("The method 'predict' is deprecated. Use 'transform' instead!")
        return self.transform(dataframe)

    def getConvertUnknownCategoricalLevelsToNa(self):
        return self._java_obj.getConvertUnknownCategoricalLevelsToNa()

    def setConvertUnknownCategoricalLevelsToNa(self, value):
        warnings.warn("The method 'setConvertUnknownCategoricalLevelsToNa' is deprecated." +
                      " Use 'H2OMOJOSettings' passed to 'create_from_mojo' instead!")
        self._java_obj.setConvertUnknownCategoricalLevelsToNa(value)
        return self

    def getFeaturesCols(self):
        return list(self._java_obj.getFeaturesCols())

    def getPredictionCol(self):
        return self._java_obj.getPredictionCol()

    def getModelDetails(self):
        return self._java_obj.getModelDetails()

    # Overriding the method to avoid changes on the companion Java object
    def _transfer_params_to_java(self):
        pass


class H2OMOJOPipelineModel(JavaModel, JavaMLWritable, JavaH2OMLReadable):

    @staticmethod
    def createFromMojo(pathToMojo, settings = H2OMOJOSettings.default()):
        spark_session = SparkSession.builder.getOrCreate()
        # We need to make sure that Sparkling Water classes are available on the Spark driver and executor paths
        Initializer.load_sparkling_jar(spark_session._sc)
        javaModel = spark_session._jvm.py_sparkling.ml.models.H2OMOJOPipelineModel.createFromMojo(pathToMojo, settings.toJavaObject())
        return H2OMOJOPipelineModel(javaModel)

    @staticmethod
    def create_from_mojo(path_to_mojo):
        warnings.warn("The method 'create_from_mojo' is deprecated. Use 'createFromMojo' instead!")
        return H2OMOJOPipelineModel.createFromMojo(path_to_mojo)

    def predict(self, dataframe):
        warnings.warn("The method 'predict' is deprecated. Use 'transform' instead!")
        return self.transform(dataframe)

    def get_input_names(self):
        warnings.warn("The method 'get_input_names' is deprecated. Use 'getFeaturesCols' instead!")
        return self.getFeaturesCols()

    def getFeaturesCols(self):
        return list(self._java_obj.getFeaturesCols())

    def getPredictionCol(self):
        return self._java_obj.getPredictionCol()

    def get_named_mojo_output_columns(self):
        warnings.warn(
            "The method 'get_named_mojo_output_columns' is deprecated. Use 'getNamedMojoOutputColumns' instead!")
        return self.getNamedMojoOutputColumns()

    def getNamedMojoOutputColumns(self):
        return self._java_obj.getNamedMojoOutputColumns()

    def set_named_mojo_output_columns(self, value):
        warnings.warn("The method 'set_named_mojo_output_columns' is deprecated." +
                      " Use 'H2OMOJOSettings' passed to 'create_from_mojo' instead!")
        return self.setNamedMojoOutputColumns(value)

    def setNamedMojoOutputColumns(self, value):
        warnings.warn("The method 'setNamedMojoOutputColumns' is deprecated." +
                      " Use 'H2OMOJOSettings' passed to 'create_from_mojo' instead!")
        self._java_obj.setNamedMojoOutputColumns(value)
        return self

    def select_prediction_udf(self, column):
        warnings.warn("The method 'select_prediction_udf' is deprecated. Use 'selectPredictionUDF' instead!")
        self.selectPredictionUDF(column)

    def selectPredictionUDF(self, column):
        java_col = self._java_obj.selectPredictionUDF(column)
        return Column(java_col)

    # Overriding the method to avoid changes on the companion Java object
    def _transfer_params_to_java(self):
        pass
