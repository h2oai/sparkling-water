from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters


class HasIgnoredColsOnMOJO:

    def getIgnoredCols(self):
        value = self._java_obj.getIgnoredCols()
        return H2OTypeConverters.scalaArrayToPythonArray(value)
