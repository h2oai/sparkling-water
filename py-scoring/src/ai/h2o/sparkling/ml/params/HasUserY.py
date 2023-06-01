from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasUserY(Params):
    userY = Param(
        Params._dummy(),
        "userY",
        "User-specified initial matrix Y.",
        H2OTypeConverters.toNullableDataFrame())

    def getUserY(self):
        return self.getOrDefault(self.userY)

    def setUserY(self, value):
        return self._set(userY=value)
