from pyspark.ml.param import *

from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters


class HasGamCols(Params):
    gamCols = Param(
        Params._dummy(),
        "gamCols",
        "Arrays of predictor column names for gam for smoothers using single or multiple predictors "
        "like {{'c1'},{'c2','c3'},{'c4'},...}",
        H2OTypeConverters.toNullableListListString())

    def getGamCols(self):
        return self.getOrDefault(self.gamCols)

    def setGamCols(self, value):
        return self._set(gamCols=self._convertGamCols(value))

    def _toStringArray(self, value):
        if isinstance(value, list):
            return value
        else:
            return [str(value)]

    def _convertGamCols(self, value):
        if isinstance(value, list):
            return [self._toStringArray(item) for item in value]
        else:
            return value

    def _updateInitKwargs(self, kwargs):
        if 'gamCols' in kwargs:
            kwargs['gamCols'] = self._convertGamCols(kwargs['gamCols'])
        return kwargs
