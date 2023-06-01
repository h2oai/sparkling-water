from ai.h2o.sparkling.ml.models import H2OMOJOModel
from ai.h2o.sparkling.H2ODataFrameConverters import H2ODataFrameConverters


class H2OGridSearchExtras:

    def getGridModels(self):
        return [H2OMOJOModel(m) for m in self._java_obj.getGridModels()]

    def getGridModelsParams(self):
        jdf = self._java_obj.getGridModelsParams()
        return H2ODataFrameConverters.scalaToPythonDataFrame(jdf)

    def getGridModelsMetrics(self):
        jdf = self._java_obj.getGridModelsMetrics()
        return H2ODataFrameConverters.scalaToPythonDataFrame(jdf)
