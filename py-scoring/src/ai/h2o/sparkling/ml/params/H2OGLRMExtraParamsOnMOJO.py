from pyspark.ml.param import *
from ai.h2o.sparkling.ml.params.H2ODimReductionExtraParamsOnMOJO import H2ODimReductionExtraParamsOnMOJO


class H2OGLRMExtraParamsOnMOJO(H2ODimReductionExtraParamsOnMOJO):
    def getReconstructedCol(self):
        return self._java_obj.getReconstructedCol()

    def getWithReconstructedlCol(self):
        return self._java_obj.getWithReconstructedCol()

    def getMaxScoringIterations(self):
        return self._java_obj.getMaxScoringIterations()

    def setReconstructedCol(self, value):
        self._java_obj.setReconstructedCol(value)
        return self

    def setWithReconstructedCol(self, value):
        self._java_obj.setWithReconstructedCol(value)
        return self

    def setMaxScoringIterations(self, value):
        self._java_obj.setMaxScoringIterations(value)
        return self
