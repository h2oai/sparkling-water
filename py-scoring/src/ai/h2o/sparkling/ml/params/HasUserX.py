from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasUserX(Params):
    userX = Param(
        Params._dummy(),
        "userX",
        "User-specified initial matrix X.",
        H2OTypeConverters.toNullableDataFrame())

    def getUserX(self):
        return self.getOrDefault(self.userX)

    def setUserX(self, value):
        return self._set(userX=value)
