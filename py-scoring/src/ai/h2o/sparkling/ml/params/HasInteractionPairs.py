from pyspark.ml.param import *
import warnings

from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters


class HasInteractionPairs(Params):
    interactionPairs = Param(
        Params._dummy(),
        "interactionPairs",
        "A list of pairwise (first order) column interactions.",
        H2OTypeConverters.toNullableListPairString())

    def getInteractionPairs(self):
        return None

    def setInteractionPairs(self, value):
        warnings.warn("Interaction pairs are not supported.")
        return self
