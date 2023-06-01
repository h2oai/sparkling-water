from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasBetaConstraints(Params):
    betaConstraints = Param(
        Params._dummy(),
        "betaConstraints",
        "Data frame of beta constraints enabling to set special conditions over the model coefficients.",
        H2OTypeConverters.toNullableDataFrame())

    def getBetaConstraints(self):
        return self.getOrDefault(self.betaConstraints)

    def setBetaConstraints(self, value):
        return self._set(betaConstraints=value)
