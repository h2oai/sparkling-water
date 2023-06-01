package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.ml.models.SpecificMOJOParameters
import hex.genmodel.MojoModel
import org.apache.spark.expose.Logging

trait HasInputColsOnMOJO extends ParameterConstructorMethods with SpecificMOJOParameters with Logging {
  protected val inputCols = nullableStringArrayParam(name = "inputCols", doc = "The array of input columns")

  def getInputCols(): Array[String] = $(inputCols)

  setDefault(inputCols -> Array.empty[String])

  override private[sparkling] def setSpecificParams(h2oMojo: MojoModel): Unit = {
    super.setSpecificParams(h2oMojo)
    set(inputCols -> h2oMojo.features())
  }
}
