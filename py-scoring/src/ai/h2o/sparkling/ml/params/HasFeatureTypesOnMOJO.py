from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters


class HasFeatureTypesOnMOJO:

    def getFeatureTypes(self):
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getFeatureTypes())
