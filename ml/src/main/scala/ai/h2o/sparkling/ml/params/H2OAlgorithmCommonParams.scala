package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.macros.DeprecatedMethod

import scala.collection.JavaConverters._

/**
  * This trait contains parameters that are shared across all algorithms.
  */
trait H2OAlgorithmCommonParams extends H2OCommonParams with H2OAlgorithmMOJOParams {

  //
  // Getters
  //
  override def getFeaturesCols(): Array[String] = {
    val excludedCols = getExcludedCols()
    $(featuresCols).filter(c => excludedCols.forall(e => c.compareToIgnoreCase(e) != 0))
  }

  //
  // Setters
  //

  // Setters for parameters which are defined on MOJO as well
  def setPredictionCol(columnName: String): this.type = set(predictionCol, columnName)

  def setDetailedPredictionCol(columnName: String): this.type = set(detailedPredictionCol, columnName)

  def setWithContributions(enabled: Boolean): this.type = set(withContributions, enabled)

  def setWithLeafNodeAssignments(enabled: Boolean): this.type = set(withLeafNodeAssignments, enabled)

  def setWithStageResults(enabled: Boolean): this.type = set(withStageResults, enabled)

  def setFeaturesCol(first: String): this.type = setFeaturesCols(first)

  def setFeaturesCols(first: String, others: String*): this.type = set(featuresCols, Array(first) ++ others)

  def setFeaturesCols(columnNames: Array[String]): this.type = {
    require(columnNames.length > 0, "Array with feature columns must contain at least one column.")
    set(featuresCols, columnNames)
  }

  def setFeaturesCols(columnNames: java.util.ArrayList[String]): this.type = {
    setFeaturesCols(columnNames.asScala.toArray)
  }

  private[sparkling] def getExcludedCols(): Seq[String]
}
