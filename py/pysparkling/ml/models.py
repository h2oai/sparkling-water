from pyspark.sql import SparkSession
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaModel
from pysparkling import *

class H2OMOJOModel(JavaModel, JavaMLWritable, JavaMLReadable):

    @staticmethod
    def create_from_mojo(path_to_mojo):
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        return H2OMOJOModel(jvm.org.apache.spark.ml.h2o.models.JavaH2OMOJOModelHelper.createFromMojo(path_to_mojo))

    def predict(self, dataframe):
        return self.transform(dataframe)



