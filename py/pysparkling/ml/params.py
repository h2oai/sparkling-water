from pyspark.ml.param import *

class H2OAlgorithmParams(Params):

    ratio = Param(Params._dummy(), "ratio", "Ration of frame which is used for training")

    featuresCols = Param(Params._dummy(), "featuresCols", "columns used as features")

    predictionCol = Param(Params._dummy(), "predictionCol", "label")

    def setRatio(self, value):
        return self._set(ratio=value)

    def setFeaturesCols(self, value):
        return self._set(featuresCols=value)

    def setPredictionCol(self, value):
        return self._set(predictionCol=value)

    def getRatio(self):
        return self.getOrDefault(self.ratio)

    def getFeaturesCols(self):
        self.getOrDefault(self.featuresCols)

    def getPredictionCol(self):
        self.getOrDefault(self.predictionCol)
