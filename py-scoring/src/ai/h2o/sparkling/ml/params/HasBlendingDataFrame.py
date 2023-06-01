from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasBlendingDataFrame(Params):
    blendingDataFrame = Param(
        Params._dummy(),
        "blendingDataFrame",
        "This parameter is used for  computing the predictions that serve as the training frame for the meta-learner. "
        "If provided, this triggers blending mode on the stacked ensemble training stage. Blending mode is faster "
        "than cross-validating the base learners (though these ensembles may not perform as well as the Super Learner "
        "ensemble).",
        H2OTypeConverters.toNullableDataFrame())

    def getBlendingDataFrame(self):
        return self.getOrDefault(self.blendingDataFrame)

    def setBlendingDataFrame(self, value):
        return self._set(blendingDataFrame=value)
