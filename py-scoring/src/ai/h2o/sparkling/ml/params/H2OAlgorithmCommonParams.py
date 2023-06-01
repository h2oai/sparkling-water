from ai.h2o.sparkling.ml.params.H2OCommonParams import H2OCommonParams
from ai.h2o.sparkling.ml.params.H2OAlgorithmMOJOParams import H2OAlgorithmMOJOParams
from pyspark.ml.param import *
import warnings


class H2OAlgorithmCommonParams(H2OCommonParams, H2OAlgorithmMOJOParams):
    # Setters for parameters which are defined on MOJO as well
    def setPredictionCol(self, value):
        return self._set(predictionCol=value)

    def setDetailedPredictionCol(self, value):
        return self._set(detailedPredictionCol=value)

    def setFeaturesCols(self, value):
        return self._set(featuresCols=value)

    def setWithContributions(self, value):
        return self._set(withContributions=value)

    def setWithLeafNodeAssignments(self, value):
        return self._set(withLeafNodeAssignments=value)

    def setWithStageResults(self, value):
        return self._set(withStageResults=value)
