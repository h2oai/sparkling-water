from pyspark.sql import DataFrame, SparkSession
from py4j.java_gateway import JavaObject


class H2ODataFrameConverters(object):

    @staticmethod
    def scalaToPythonDataFrame(jdf):
        if jdf is None:
            return None
        elif isinstance(jdf, JavaObject):
            session = SparkSession.builder.getOrCreate()
            if hasattr(session, '_wrapped'):
                sqlContext = session._wrapped
            else:
                sqlContext = session  # Spark 3.3+ utilizes SparkSession instead of SQLContext
            return DataFrame(jdf, sqlContext)
        else:
            raise TypeError("Invalid type.")

    @staticmethod
    def scalaDfArrayToPythonDfArray(array):
        if array is None:
            return None
        elif isinstance(array, JavaObject):
            return [H2ODataFrameConverters.scalaToPythonDataFrame(v) for v in array]
        else:
            raise TypeError("Invalid type.")
