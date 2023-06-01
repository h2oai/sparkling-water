from ai.h2o.sparkling.ml.H2OStageBase import H2OStageBase
from ai.h2o.sparkling.ml.models.H2OBinaryModel import H2OBinaryModel
from ai.h2o.sparkling.ml.models.H2OMOJOModel import H2OMOJOModelFactory
from pyspark.ml.wrapper import JavaEstimator


class H2OEstimator(H2OStageBase, JavaEstimator):

    def getBinaryModel(self):
        return H2OBinaryModel(self._java_obj.getBinaryModel())

    def _create_model(self, javaModel):
        return H2OMOJOModelFactory.createSpecificMOJOModel(javaModel)

    def _updateInitKwargs(self, kwargs):
        return kwargs
