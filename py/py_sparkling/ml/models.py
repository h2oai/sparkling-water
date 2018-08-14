from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaModel
from pysparkling.initializer import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

class H2OGBMModel(JavaModel, JavaMLWritable, JavaMLReadable):
    pass


class H2ODeepLearningModel(JavaModel, JavaMLWritable, JavaMLReadable):
    pass


class H2OAutoMLModel(JavaModel, JavaMLWritable, JavaMLReadable):
    pass

class H2OXGBoostModel(JavaModel, JavaMLWritable, JavaMLReadable):
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

    def get_input_names(self):
        return list(self._java_obj.getInputNames())

    def get_input_types(self):
        return list(self._java_obj.getInputTypes())

    def get_output_names(self):
        return list(self._java_obj.getOutputNames())

    def get_output_types(self):
        return list(self._java_obj.getOutputTypes())

    def get_named_mojo_output_columns(self):
        return self._java_obj.getNamedMojoOutputColumns()

    def set_named_mojo_output_columns(self, value):
        self._java_obj.setNamedMojoOutputColumns(value)
        return self

    def select_prediction_udf(self, column):
        if column not in self.get_output_names():
            raise ValueError("Column '" + column + "' is not defined as the output column in MOJO Pipeline.")

        if self.get_named_mojo_output_columns():
            func = udf(lambda d: d, DoubleType())
            return func("prediction." + column).alias(column)
        else:
            idx = self.get_output_names().index(column)
            func = udf(lambda arr: arr[idx], DoubleType())
            return func("prediction.preds").alias(column)
