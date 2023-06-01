from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters


class HasGamColsOnMOJO:

    def getGamCols(self):
        value = self._java_obj.getGamCols()
        return H2OTypeConverters.toNullableListListString()(value)
