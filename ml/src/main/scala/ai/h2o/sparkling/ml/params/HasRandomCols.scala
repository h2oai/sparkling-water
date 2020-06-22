package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.ml.algos.H2OAlgoCommonUtils

trait HasRandomCols extends H2OAlgoParamsBase with H2OAlgoCommonUtils {
  private val randomCols = new NullableStringArrayParam(
    this,
    "randomCols",
    "Names of random columns for HGLM.")

  setDefault(randomCols -> null)

  def getRandomCols(): Array[String] = $(randomCols)

  def setRandomCols(value: Array[String]): this.type = set(randomCols, value)

  override private[sparkling] def getH2OAlgorithmParams(trainingFrame: H2OFrame): Map[String, Any] = {
    val randomColumnNames = getRandomCols()
    val indices = if (randomColumnNames == null) {
      null
    } else {
      val frameColumns = trainingFrame.columnNames
      val indices = randomColumnNames.map(frameColumns.indexOf)
      indices
    }

    super.getH2OAlgorithmParams(trainingFrame) ++ Map("random_columns" -> indices)
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("randomCols" -> "random_columns")
  }

  override private[sparkling] def getExcludedCols(): Seq[String] = {
    val randomCols = getRandomCols()
    if (randomCols == null) {
      super.getExcludedCols()
    } else {
      super.getExcludedCols() ++ randomCols
    }
  }
}
