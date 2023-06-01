from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasLeaderboardDataFrame(Params):
    leaderboardDataFrame = Param(
        Params._dummy(),
        "leaderboardDataFrame",
        "This parameter allows the user to specify a particular data frame to use to score and rank models " +
        "on the leaderboard. This data frame will not be used for anything besides leaderboard scoring.",
        H2OTypeConverters.toNullableDataFrame())

    def getLeaderboardDataFrame(self):
        return self.getOrDefault(self.leaderboardDataFrame)

    def setLeaderboardDataFrame(self, value):
        return self._set(leaderboardDataFrame=value)
