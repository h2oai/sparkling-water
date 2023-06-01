from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasIgnoredCols(Params):
    ignoredCols = Param(
        Params._dummy(),
        "ignoredCols",
        "Names of columns to ignore for training.",
        H2OTypeConverters.toNullableListString())

    def getIgnoredCols(self):
        return self.getOrDefault(self.ignoredCols)

    def setIgnoredCols(self, value):
        return self._set(ignoredCols=value)
