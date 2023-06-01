from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasOutputCol(Params):
    outputCol = Param(
        Params._dummy(),
        "outputCol",
        "Output columns name",
        H2OTypeConverters.toNullableString())

    def getOutputCol(self):
        return self.getOrDefault(self.outputCol)

    def setOutputCol(self, value):
        return self._set(outputCol=value)
