package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.ml.algos.H2OAlgoCommonUtils

trait HasIgnoredCols extends H2OAlgoParamsBase {
  private val ignoredCols =
    new NullableStringArrayParam(this, "ignoredCols", "Names of columns to ignore for training.")

  setDefault(ignoredCols -> null)

  def getIgnoredCols(): Array[String] = $(ignoredCols)

  def setIgnoredCols(value: Array[String]): this.type = set(ignoredCols, value)

  private[sparkling] def getIgnoredColsParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("ignored_columns" -> getIgnoredCols())
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() + ("ignoredCols" -> "ignored_columns")
  }
}
