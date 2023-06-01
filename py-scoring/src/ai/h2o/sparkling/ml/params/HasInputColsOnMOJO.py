from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters


class HasInputColsOnMOJO:

    def getInputCols(self):
        value = self._java_obj.getInputCols()
        return H2OTypeConverters.scalaArrayToPythonArray(value)
