package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{Param, Params}

trait HasOutputCol extends Params {

  private val outputCol: Param[String] = new Param[String](this, "outputCol", "Output column name")

  setDefault(outputCol, uid + "__output")

  def getOutputCol(): String = $(outputCol)

  def setOutputCol(name: String): this.type = set(outputCol -> name)
}
