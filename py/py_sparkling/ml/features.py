from pyspark import keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer
from pyspark.sql import SparkSession


def get_input_kwargs(self, spark_session):
    if spark_session.version == "2.1.0":
        return self.__init__._input_kwargs
    else:
        # on newer versions we need to use the following variant
        return self._input_kwargs


class ColumnPruner(JavaTransformer, JavaMLReadable, JavaMLWritable):
    keep = Param(Params._dummy(), "keep", "keep the specified columns in the frame")

    columns = Param(Params._dummy(), "columns", "specified columns")

    @keyword_only
    def __init__(self, keep=False, columns=[]):
        super(ColumnPruner, self).__init__()
        self._java_obj = self._new_java_obj("py_sparkling.ml.features.ColumnPruner", self.uid)
        self._setDefault(keep=False, columns=[])
        self._spark = SparkSession.builder.getOrCreate()
        kwargs = get_input_kwargs(self, self._spark)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, keep=False, columns=[]):
        kwargs = get_input_kwargs(self, self._spark)
        return self._set(**kwargs)

    def setKeep(self, value):
        return self._set(keep=value)

    def setColumns(self, value):
        return self._set(columns=value)

    def getKeep(self):
        self.getOrDefault(self.keep)

    def getColumns(self):
        self.getOrDefault(self.columns)
