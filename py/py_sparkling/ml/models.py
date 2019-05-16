from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaModel
from pysparkling.initializer import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from .util import JavaH2OMLReadable
from pyspark.ml.param import *
import warnings


class H2OMOJOModel(JavaModel, JavaMLWritable, JavaH2OMLReadable):

    @staticmethod
    def create_from_mojo(path_to_mojo):
        spark_session = SparkSession.builder.getOrCreate()
        # We need to make sure that Sparkling Water classes are available on the Spark driver and executor paths
        Initializer.load_sparkling_jar(spark_session._sc)
        return H2OMOJOModel(
            spark_session._jvm.py_sparkling.ml.models.H2OMOJOModel.createFromMojo(path_to_mojo))

    def predict(self, dataframe):
        warnings.warn("The method 'predict' is deprecated. Use 'transform' instead!")
        return self.transform(dataframe)

    def getConvertUnknownCategoricalLevelsToNa(self):
        return self._java_obj.getConvertUnknownCategoricalLevelsToNa()

    def setConvertUnknownCategoricalLevelsToNa(self, value):
        self._java_obj.setConvertUnknownCategoricalLevelsToNa(value)
        return self

    def getFeaturesCols(self):
        return list(self._java_obj.getFeaturesCols())

    def getPredictionCol(self):
        return self._java_obj.getPredictionCol()

class H2OMOJOPipelineModel(JavaModel, JavaMLWritable, JavaH2OMLReadable):

    @staticmethod
    def create_from_mojo(path_to_mojo):
        spark_session = SparkSession.builder.getOrCreate()
        # We need to make sure that Sparkling Water classes are available on the Spark driver and executor paths
        Initializer.load_sparkling_jar(spark_session._sc)
        return H2OMOJOPipelineModel(
            spark_session._jvm.py_sparkling.ml.models.H2OMOJOPipelineModel.createFromMojo(path_to_mojo))

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

    def get_output_names(self):
        warnings.warn("The method 'get_output_names' is deprecated.")
        return list(self._java_obj.getOutputNames())

    def get_named_mojo_output_columns(self):
        warnings.warn(
            "The method 'get_named_mojo_output_columns' is deprecated. Use 'getNamedMojoOutputColumns' instead!")
        return self.getNamedMojoOutputColumns()

    def getNamedMojoOutputColumns(self):
        return self._java_obj.getNamedMojoOutputColumns()

    def set_named_mojo_output_columns(self, value):
        warnings.warn(
            "The method 'set_named_mojo_output_columns' is deprecated. Use 'setNamedMojoOutputColumns' instead!")
        return self.setNamedMojoOutputColumns(value)

    def setNamedMojoOutputColumns(self, value):
        self._java_obj.setNamedMojoOutputColumns(value)
        return self

    def select_prediction_udf(self, column):
        warnings.warn("The method 'select_prediction_udf' is deprecated. Use 'selectPredictionUdf' instead!")
        self.selectPredictionUdf(column)

    def selectPredictionUdf(self, column):
        if column not in self.get_output_names():
            raise ValueError("Column '" + column + "' is not defined as the output column in MOJO Pipeline.")

        if self.getNamedMojoOutputColumns():
            func = udf(lambda d: d, DoubleType())
            return func(self.getPredictionCol() + "." + column).alias(column)
        else:
            idx = list(self._java_obj.getOutputNames()).index(column)
            func = udf(lambda arr: arr[idx], DoubleType())
            return func(self.getPredictionCol() + ".preds").alias(column)
