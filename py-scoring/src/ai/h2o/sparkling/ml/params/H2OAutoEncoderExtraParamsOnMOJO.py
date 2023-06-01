from pyspark.ml.param import *
from ai.h2o.sparkling.ml.params.HasInputColsOnMOJO import HasInputColsOnMOJO
from ai.h2o.sparkling.ml.params.HasOutputColOnMOJO import HasOutputColOnMOJO


class H2OAutoEncoderExtraParamsOnMOJO(HasInputColsOnMOJO, HasOutputColOnMOJO):
    def getOriginalCol(self):
        return self._java_obj.getOriginalCol()

    def getWithOriginalCol(self):
        return self._java_obj.getWithOriginalCol()

    def getMSECol(self):
        return self._java_obj.getMSECol()

    def getWithMSECol(self):
        return self._java_obj.getWithMSECol()

    def setOriginalCol(self, value):
        self._java_obj.setOriginalCol(value)
        return self

    def setWithOriginalCol(self, value):
        self._java_obj.setWithOriginalCol(value)
        return self

    def setMSECol(self, value):
        self._java_obj.setMSECol(value)
        return self

    def setWithMSECol(self, value):
        self._java_obj.setWithMSECol(value)
        return self
