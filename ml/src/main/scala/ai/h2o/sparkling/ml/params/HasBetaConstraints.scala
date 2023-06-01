package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame
import org.apache.spark.sql.DataFrame
import ai.h2o.sparkling.utils.DataFrameSerializationWrappers._

trait HasBetaConstraints extends H2OAlgoParamsBase with HasDataFrameSerializer {
  private val betaConstraints = new NullableDataFrameParam(
    this,
    "betaConstraints",
    "Data frame of beta constraints enabling to set special conditions over the model coefficients.")

  setDefault(betaConstraints -> null)

  def getBetaConstraints(): DataFrame = $(betaConstraints)

  def setBetaConstraints(value: DataFrame): this.type = set(betaConstraints, toWrapper(value))

  private[sparkling] def getBetaConstraintsParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("beta_constraints" -> convertDataFrameToH2OFrameKey(getBetaConstraints()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("betaConstraints" -> "beta_constraints")
  }
}
