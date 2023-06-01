package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame

trait HasUserPoints extends H2OAlgoParamsBase {
  private val userPoints = new NullableDoubleArrayArrayParam(
    this,
    "userPoints",
    "This option allows" +
      " you to specify array of points, where each point represents coordinates of an initial cluster center. The user-specified" +
      " points must have the same number of columns as the training observations. The number of rows must equal" +
      " the number of clusters.")

  setDefault(userPoints -> null)

  def getUserPoints(): Array[Array[Double]] = $(userPoints)

  def setUserPoints(value: Array[Array[Double]]): this.type = set(userPoints, value)

  private[sparkling] def getUserPointsParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("user_points" -> convert2dArrayToH2OFrame(getUserPoints()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("userPoints" -> "user_points")
  }
}
