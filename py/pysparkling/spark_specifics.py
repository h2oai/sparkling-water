# Methods, whose implementation changes between Spark versions
# and we need to handle it differently
from pyspark.sql import SparkSession


def get_input_kwargs(instance):
    spark_version = SparkSession.builder.getOrCreate().version

    if spark_version == "2.1.0":
        return instance.__init__._input_kwargs
    else:
        # on newer versions we need to use the following variant
        return instance._input_kwargs
