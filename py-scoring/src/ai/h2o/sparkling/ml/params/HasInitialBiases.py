from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasInitialBiases(Params):
    initialBiases = Param(
        Params._dummy(),
        "initialBiases",
        "A array of weight vectors to be used for bias initialization of every network layer. "
        "If this parameter is set, the parameter 'initialWeights' has to be set as well.",
        H2OTypeConverters.toNullableListDenseVector())

    def getInitialBiases(self):
        return self.getOrDefault(self.initialBiases)

    def setInitialBiases(self, value):
        return self._set(initialBiases=value)
