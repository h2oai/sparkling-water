from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *

class HasInputCol(Params):
    inputCol = Param(
        Params._dummy(),
        "inputCol",
        "Input columns name",
        H2OTypeConverters.toNullableString())

    def getInputCol(self):
        return self.getOrDefault(self.inputCol)

    def setInputCol(self, value):
        return self._set(inputCol=value)
