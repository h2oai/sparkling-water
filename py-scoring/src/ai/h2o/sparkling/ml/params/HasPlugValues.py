from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasPlugValues(Params):
    plugValues = Param(
        Params._dummy(),
        "plugValues",
        "A dictionary containing values that will be used to impute missing values of the training/validation frame, "
        "use with conjunction missingValuesHandling = 'PlugValues')",
        H2OTypeConverters.toNullableDictionaryWithAnyElements())

    def getPlugValues(self):
        return self.getOrDefault(self.plugValues)

    def setPlugValues(self, value):
        return self._set(plugValues=value)
