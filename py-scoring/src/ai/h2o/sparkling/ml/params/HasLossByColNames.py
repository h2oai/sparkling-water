from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasLossByColNames(Params):
    lossByColNames = Param(
        Params._dummy(),
        "lossByColNames",
        "Column names for which loss function will be overridden by the 'lossByCol' parameter",
        H2OTypeConverters.toNullableListString())

    def getLossByColNames(self):
        return self.getOrDefault(self.lossByColNames)

    def setLossByColNames(self, value):
        return self._set(lossByColNames=value)
