package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame

trait HasLossByColNames extends H2OAlgoParamsBase {
  private val lossByColNames = new NullableStringArrayParam(
    this,
    "lossByColNames",
    "Columns names for which loss function will be overridden by the 'lossByCol' parameter")

  setDefault(lossByColNames -> null)

  def getLossByColNames(): Array[String] = $(lossByColNames)

  def setLossByColNames(value: Array[String]): this.type = set(lossByColNames, value)

  def getLossByColNamesParam(trainingFrame: H2OFrame): Map[String, Any] = {
    val names = getLossByColNames()
    val indices = if (names == null) {
      null
    } else {
      val frameColumns = trainingFrame.columnNames
      val indices = names.map(frameColumns.indexOf)
      indices
    }

    Map("loss_by_col_idx" -> indices)
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("lossByColNames" -> "loss_by_col_idx")
  }

}
