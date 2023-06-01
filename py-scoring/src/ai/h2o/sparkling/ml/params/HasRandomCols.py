from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasRandomCols(Params):
    randomCols = Param(
        Params._dummy(),
        "randomCols",
        "Names of random columns for HGLM.",
        H2OTypeConverters.toNullableListString())

    def getRandomCols(self):
        return self.getOrDefault(self.randomCols)

    def setRandomCols(self, value):
        return self._set(randomCols=value)
