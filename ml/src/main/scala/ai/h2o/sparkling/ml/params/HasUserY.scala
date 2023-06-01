package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame
import org.apache.spark.sql.DataFrame
import ai.h2o.sparkling.utils.DataFrameSerializationWrappers._

trait HasUserY extends H2OAlgoParamsBase with HasDataFrameSerializer {

  private val userY = new NullableDataFrameParam(this, "userY", "User-specified initial matrix Y.")

  setDefault(userY -> null)

  def getUserY(): DataFrame = $(userY)

  def setUserY(value: DataFrame): this.type = set(userY, toWrapper(value))

  private[sparkling] def getUserYParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("user_y" -> convertDataFrameToH2OFrameKey(getUserY()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("userY" -> "user_y")
  }
}
