package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame
import org.apache.spark.expose.Logging
import org.apache.spark.sql.DataFrame
import ai.h2o.sparkling.utils.DataFrameSerializationWrappers._

trait HasLeaderboardDataFrame extends H2OAlgoParamsBase with Logging with HasDataFrameSerializer {

  val uid: String

  private val leaderboardDataFrame = new NullableDataFrameParam(
    this,
    "leaderboardDataFrame",
    "This parameter allows the user to specify a particular data frame to use to score and rank models " +
      "on the leaderboard. This data frame will not be used for anything besides leaderboard scoring.")

  setDefault(leaderboardDataFrame -> null)

  def getLeaderboardDataFrame(): DataFrame = $(leaderboardDataFrame)

  def setLeaderboardDataFrame(value: DataFrame): this.type = set(leaderboardDataFrame, toWrapper(value))

  private[sparkling] def getLeaderboardDataFrameParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("leaderboard_frame" -> convertDataFrameToH2OFrameKey(getLeaderboardDataFrame()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("leaderboardDataFrame" -> "leaderboard_frame")
  }
}
