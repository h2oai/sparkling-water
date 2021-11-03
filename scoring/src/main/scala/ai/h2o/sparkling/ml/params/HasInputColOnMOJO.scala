package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{Param, Params}

trait HasInputColOnMOJO extends Params {
  protected val inputCol: Param[String] = new Param[String](this, "inputCol", "Input column name")

  def getInputCol(): String = $(inputCol)

  setDefault(inputCol -> "")

  def setInputCol(name: String): this.type = set(inputCol -> name)

}
