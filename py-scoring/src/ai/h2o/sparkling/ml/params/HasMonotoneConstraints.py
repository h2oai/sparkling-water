from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasMonotoneConstraints(Params):
    monotoneConstraints = Param(
        Params._dummy(),
        "monotoneConstraints",
        "Monotone Constraints - A key must correspond to a feature name and value could be 1 or -1",
        H2OTypeConverters.toDictionaryWithFloatElements())

    def getMonotoneConstraints(self):
        return self.getOrDefault(self.monotoneConstraints)

    def setMonotoneConstraints(self, value):
        return self._set(monotoneConstraints=value)
