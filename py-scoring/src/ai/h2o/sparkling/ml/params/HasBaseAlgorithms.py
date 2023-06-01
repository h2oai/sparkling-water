from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasBaseAlgorithms(Params):
    baseAlgorithms = Param(
        Params._dummy(),
        "baseAlgorithms",
        "Algorithms used by a meta learner.",
        H2OTypeConverters.toNullableListJavaObject())

    def getBaseAlgorithms(self):
        return self.getOrDefault(self.baseAlgorithms)

    def setBaseAlgorithms(self, algos):
        return self._set(baseAlgorithms=algos)
