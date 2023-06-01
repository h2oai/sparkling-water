from pyspark.ml.param import *
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from ai.h2o.sparkling.ml.params.H2ODimReductionExtraParams import H2ODimReductionExtraParams


class H2OGLRMExtraParams(H2ODimReductionExtraParams):
    reconstructedCol = Param(
        Params._dummy(),
        "reconstructedCol",
        "Reconstructed column name. This column contains reconstructed input values (A_hat=X*Y instead of just X).",
        H2OTypeConverters.toNullableString())

    withReconstructedCol = Param(
        Params._dummy(),
        "withReconstructedCol",
        "A flag identifying whether a column with reconstructed input values will be produced or not.",
        H2OTypeConverters.toBoolean())

    maxScoringIterations = Param(
        Params._dummy(),
        "maxScoringIterations",
        "The maximum number of iterations used in MOJO scoring to update X.",
        H2OTypeConverters.toInt())

    def getReconstructedCol(self):
        return self.getOrDefault(self.reconstructedCol)

    def getWithReconstructedCol(self):
        return self.getOrDefault(self.withReconstructedCol)

    def getMaxScoringIterations(self):
        return self.getOrDefault(self.maxScoringIterations)

    def setReconstructedCol(self, value):
        return self._set(reconstructedCol=value)

    def setWithReconstructedCol(self, value):
        return self._set(withReconstructedCol=value)

    def setMaxScoringIterations(self, value):
        return self._set(maxScoringIterations=value)
