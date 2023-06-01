from pyspark.ml.param import *
from ai.h2o.sparkling.ml.models.H2OMOJOModelBase import H2OMOJOModelBase

import warnings


class H2OAlgorithmMOJOModelBase(H2OMOJOModelBase):

    ##
    # Getters
    ##
    def getPredictionCol(self):
        return self._java_obj.getPredictionCol()

    def getDetailedPredictionCol(self):
        return self._java_obj.getDetailedPredictionCol()

    def getFeaturesCols(self):
        return list(self._java_obj.getFeaturesCols())

    def getWithContributions(self):
        return self._java_obj.getWithContributions()

    def getWithLeafNodeAssignments(self):
        return self._java_obj.getWithLeafNodeAssignments()

    def getWithStageResults(self):
        return self._java_obj.getWithStageResults()
