package org.apache.spark.ml.h2o.param

import org.apache.spark.internal.Logging
import org.apache.spark.ml.param.{Param, Params, StringArrayParam}

/**
  * This trait contains parameters that are shared across all algorithms.
  */
trait H2OCommonParams extends Params with Logging {

  protected final val featuresCols = new StringArrayParam(this, "featuresCols", "Name of feature columns")
  private val labelCol = new Param[String](this, "labelCol", "Label column name")
  private val foldCol = new NullableStringParam(this, "foldCol", "Fold column name")
  private val weightCol = new NullableStringParam(this, "weightCol", "Weight column name")

  //
  // Default values
  //
  setDefault(
    featuresCols -> Array.empty[String],
    labelCol -> "label",
    foldCol -> null,
    weightCol -> null
  )

  //
  // Getters
  //
  def getFeaturesCols(): Array[String] = {
    val excludedCols = getExcludedCols()
    $(featuresCols).filter(c => excludedCols.forall(e => c.compareToIgnoreCase(e) != 0))
  }

  def getLabelCol(): String = $(labelCol)

  def getFoldCol(): String = $(foldCol)

  def getWeightCol(): String = $(weightCol)

  //
  // Setters
  //
  def setFeaturesCol(first: String): this.type = setFeaturesCols(first)

  def setFeaturesCols(first: String, others: String*): this.type = set(featuresCols, Array(first) ++ others)

  def setFeaturesCols(columnNames: Array[String]): this.type = {
    require(columnNames.length > 0, "Array with feature columns must contain at least one column.")
    set(featuresCols, columnNames)
  }

  def setLabelCol(columnName: String): this.type = set(labelCol, columnName)

  def setFoldCol(columnName: String): this.type = set(foldCol, columnName)

  def setWeightCol(columnName: String): this.type = set(weightCol, columnName)

  //
  // Other methods
  //
  protected def getExcludedCols(): Seq[String] = {
    Seq(getLabelCol(), getFoldCol(), getWeightCol())
      .flatMap(Option(_)) // Remove nulls
  }

}
