package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame
import org.apache.spark.sql.DataFrame
import ai.h2o.sparkling.utils.DataFrameSerializationWrappers._

trait HasUserX extends H2OAlgoParamsBase with HasDataFrameSerializer {

  private val userX = new NullableDataFrameParam(this, "userX", "User-specified initial matrix X.")

  setDefault(userX -> null)

  def getUserX(): DataFrame = $(userX)

  def setUserX(value: DataFrame): this.type = set(userX, toWrapper(value))

  private[sparkling] def getUserXParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("user_x" -> convertDataFrameToH2OFrameKey(getUserX()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("userX" -> "user_x")
  }
}
