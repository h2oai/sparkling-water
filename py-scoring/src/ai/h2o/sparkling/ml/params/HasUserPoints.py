from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasUserPoints(Params):
    userPoints = Param(
        Params._dummy(),
        "userPoints",
        "This option allows you to specify array of points, where each point represents coordinates of an initial"
        " cluster center. The user-specified"
        " points must have the same number of columns as the training observations. The number of rows must equal"
        " the number of clusters.",
        H2OTypeConverters.toNullableListListFloat())

    def getUserPoints(self):
        return self.getOrDefault(self.userPoints)

    def setUserPoints(self, value):
        return self._set(userPoints=value)
