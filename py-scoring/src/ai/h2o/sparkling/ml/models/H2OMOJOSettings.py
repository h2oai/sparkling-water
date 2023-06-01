from pyspark.ml.param import *
from pyspark.ml.wrapper import JavaWrapper


class H2OMOJOSettings(JavaWrapper):

    def __init__(self,
                 predictionCol="prediction",
                 detailedPredictionCol="detailed_prediction",
                 convertUnknownCategoricalLevelsToNa=False,
                 convertInvalidNumbersToNa=False,
                 withContributions=False,
                 withInternalContributions=False,
                 withPredictionInterval=False,
                 withLeafNodeAssignments=False,
                 withStageResults=False,
                 dataFrameSerializer="ai.h2o.sparkling.utils.JSONDataFrameSerializer",
                 scoringBulkSize=1000):
        self._java_obj = None

        assert isinstance(predictionCol, str)
        assert isinstance(detailedPredictionCol, str)
        assert isinstance(convertUnknownCategoricalLevelsToNa, bool)
        assert isinstance(convertInvalidNumbersToNa, bool)
        assert isinstance(withContributions, bool)
        assert isinstance(withInternalContributions, bool)
        assert isinstance(withPredictionInterval, bool)
        assert isinstance(withLeafNodeAssignments, bool)
        assert isinstance(withStageResults, bool)
        assert isinstance(dataFrameSerializer, str)
        assert isinstance(scoringBulkSize, int)
        self.predictionCol = predictionCol
        self.detailedPredictionCol = detailedPredictionCol
        self.convertUnknownCategoricalLevelsToNa = convertUnknownCategoricalLevelsToNa
        self.convertInvalidNumbersToNa = convertInvalidNumbersToNa
        self.withContributions = withContributions
        self.withInternalContributions = withInternalContributions
        self.withPredictionInterval = withPredictionInterval
        self.withLeafNodeAssignments = withLeafNodeAssignments
        self.withStageResults = withStageResults
        self.dataFrameSerializer = dataFrameSerializer
        self.scoringBulkSize = scoringBulkSize

    def toJavaObject(self):
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.models.H2OMOJOSettings",
                                            self.predictionCol,
                                            self.detailedPredictionCol,
                                            self.convertUnknownCategoricalLevelsToNa,
                                            self.convertInvalidNumbersToNa,
                                            self.withContributions,
                                            self.withInternalContributions,
                                            self.withPredictionInterval,
                                            self.withLeafNodeAssignments,
                                            self.withStageResults,
                                            self.dataFrameSerializer,
                                            self.scoringBulkSize)
        return self._java_obj

    @staticmethod
    def default():
        return H2OMOJOSettings()
