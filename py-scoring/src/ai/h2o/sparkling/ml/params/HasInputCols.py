from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasInputCols(Params):
    inputCols = Param(
        Params._dummy(),
        "inputCols",
        "Names of input columns",
        H2OTypeConverters.toNullableListString())

    def getInputCols(self):
        return self.getOrDefault(self.inputCols)

    def setInputCols(self, value):
        return self._set(inputCols=value)
