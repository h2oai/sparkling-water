package ai.h2o.sparkling.ml.params

trait HasInputCols extends HasInputColsOnMOJO {
  def setInputCols(columns: Array[String]): this.type = set(inputCols -> columns)

  def setInputCols(column: String, otherColumns: String*): this.type = setInputCols(Array(column) ++ otherColumns)
}
