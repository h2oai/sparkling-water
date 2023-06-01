package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.ml.algos.H2OAlgoCommonUtils

trait HasGamCols extends H2OAlgoParamsBase with H2OAlgoCommonUtils {
  protected val gamCols = nullableStringArrayArrayParam(
    name = "gamCols",
    doc = "Arrays of predictor column names for gam for smoothers using single or " +
      "multiple predictors like {{'c1'},{'c2','c3'},{'c4'},...}")

  setDefault(gamCols -> null)

  def getGamCols(): Array[Array[String]] = $(gamCols)

  def setGamCols(value: Array[Array[String]]): this.type = set(gamCols, value)

  def setGamCols(value: Array[String]): this.type = setGamCols(value.map(Array(_)))

  private[sparkling] def getGamColsParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("gam_columns" -> getGamCols())
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() + ("gamCols" -> "gam_columns")
  }

  override private[sparkling] def getAdditionalCols(): Seq[String] = {
    val gamCols = getGamCols()
    if (gamCols != null) {
      super.getAdditionalCols() ++ gamCols.flatten.distinct
    } else {
      super.getAdditionalCols()
    }
  }
}
