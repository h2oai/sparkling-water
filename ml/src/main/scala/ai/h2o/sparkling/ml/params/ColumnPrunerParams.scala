package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{BooleanParam, Params, StringArrayParam}

trait ColumnPrunerParams extends Params {

  //
  // Param definitions
  //
  private final val keep = new BooleanParam(
    this,
    "keep",
    "Determines if the column specified in the 'columns' parameter should be kept or removed")
  private final val columns = new StringArrayParam(this, "columns", "List of columns to be kept or removed")

  //
  // Default values
  //
  setDefault(
    keep -> false, // default is false which means remove specified columns
    columns -> Array[String]() // default is empty array which means no columns are removed
  )

  //
  // Getters
  //
  def getKeep() = $(keep)

  def getColumns() = $(columns)

  //
  // Setters
  //
  def setKeep(value: Boolean): this.type = set(keep, value)

  def setColumns(value: Array[String]): this.type = set(columns, value)
}
