from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters


class HasMonotoneConstraintsOnMOJO:

    def getMonotoneConstraints(self):
        value = self._java_obj.getMonotoneConstraints()
        return H2OTypeConverters.nullableScalaMapStringStringToDictStringAny(value)
