from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasInitialWeights(Params):
    initialWeights = Param(
        Params._dummy(),
        "initialWeights",
        "A array of weight matrices to be used for initialization of the neural network. "
        "If this parameter is set, the parameter 'initialBiases' has to be set as well.",
        H2OTypeConverters.toNullableListDenseMatrix())

    def getInitialWeights(self):
        return self.getOrDefault(self.initialWeights)

    def setInitialWeights(self, value):
        return self._set(initialWeights=value)
