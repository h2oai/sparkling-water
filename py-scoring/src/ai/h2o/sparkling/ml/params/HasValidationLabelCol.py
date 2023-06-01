from pyspark.ml.param import *

from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters


class HasValidationLabelCol(Params):
    validationLabelCol = Param(
        Params._dummy(),
        "validationLabelCol",
        "(experimental) Name of the label column in the validation data frame. "
        "The label column should be a string column with two distinct values indicating the anomaly. "
        "The negative value must be alphabetically smaller than the positive value. (E.g. '0'/'1', 'False'/'True')",
        H2OTypeConverters.toString())

    def getValidationLabelCol(self):
        return self.getOrDefault(self.validationLabelCol)

    def setValidationLabelCol(self, value):
        return self._set(validationLabelCol=value)
