from pyspark.ml.param import *
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from ai.h2o.sparkling.ml.params.HasInputCols import HasInputCols
from ai.h2o.sparkling.ml.params.HasOutputCol import HasOutputCol


class H2OAutoEncoderExtraParams(HasInputCols, HasOutputCol):
    originalCol = Param(
        Params._dummy(),
        "originalCol",
        "Original column name. This column contains input values to the neural network of auto encoder.",
        H2OTypeConverters.toNullableString())

    withOriginalCol = Param(
        Params._dummy(),
        "withOriginalCol",
        "A flag identifying whether a column with input values to the neural network will be produced or not.",
        H2OTypeConverters.toBoolean())

    mseCol = Param(
        Params._dummy(),
        "mseCol",
        "MSE column name. This column contains mean square error calculated from original and output values.",
        H2OTypeConverters.toNullableString())

    withMSECol = Param(
        Params._dummy(),
        "withMSECol",
        "A flag identifying whether a column with mean square error will be produced or not.",
        H2OTypeConverters.toBoolean())

    def getOriginalCol(self):
        return self.getOrDefault(self.originalCol)

    def getWithOriginalCol(self):
        return self.getOrDefault(self.withOriginalCol)

    def getMSECol(self):
        return self.getOrDefault(self.mseCol)

    def getWithMSECol(self):
        return self.getOrDefault(self.withMSECol)

    def setOriginalCol(self, value):
        return self._set(originalCol=value)

    def setWithOriginalCol(self, value):
        return self._set(withOriginalCol=value)

    def setMSECol(self, value):
        return self._set(mseCol=value)

    def setWithMSECol(self, value):
        return self._set(withMSECol=value)
