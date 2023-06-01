from ai.h2o.sparkling.Initializer import Initializer
from ai.h2o.sparkling.ml.models import H2OMOJOSettings
from ai.h2o.sparkling.ml.models.H2OAlgorithmMOJOModelBase import H2OAlgorithmMOJOModelBase
from pyspark.ml.param import *
from pyspark.ml.util import _jvm
from pyspark.sql.column import Column


class H2OMOJOPipelineModel(H2OAlgorithmMOJOModelBase):

    @staticmethod
    def createFromMojo(pathToMojo, settings=H2OMOJOSettings.default()):
        # We need to make sure that Sparkling Water classes are available on the Spark driver and executor paths
        Initializer.load_sparkling_jar()
        javaModel = _jvm().ai.h2o.sparkling.ml.models.H2OMOJOPipelineModel.createFromMojo(pathToMojo,
                                                                                          settings.toJavaObject())
        return H2OMOJOPipelineModel(javaModel)

    def selectPredictionUDF(self, column):
        java_col = self._java_obj.selectPredictionUDF(column)
        return Column(java_col)

    def getOutputSubCols(self):
        return list(self._java_obj.getOutputSubCols())

    def getWithInternalContributions(self):
        return self._java_obj.getWithInternalContributions()

    def getWithPredictionInterval(self):
        return self._java_obj.getWithPredictionInterval()

    def getScoringBulkSize(self):
        return self._java_obj.getScoringBulkSize()
