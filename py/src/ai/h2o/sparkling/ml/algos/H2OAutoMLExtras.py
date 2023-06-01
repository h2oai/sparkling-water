from ai.h2o.sparkling.ml.models.H2OMOJOModel import H2OMOJOModelFactory
from ai.h2o.sparkling.H2ODataFrameConverters import H2ODataFrameConverters


class H2OAutoMLExtras:

    def getLeaderboard(self, *extraColumns):
        if len(extraColumns) == 1 and isinstance(extraColumns[0], list):
            extraColumns = extraColumns[0]
        leaderboard_java = self._java_obj.getLeaderboard(extraColumns)
        return H2ODataFrameConverters.scalaToPythonDataFrame(leaderboard_java)

    def getAllModels(self):
        javaModels = self._java_obj.getAllModels()
        return [H2OMOJOModelFactory.createSpecificMOJOModel(javaModel) for javaModel in javaModels]
