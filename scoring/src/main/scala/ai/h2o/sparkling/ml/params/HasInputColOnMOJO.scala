package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.ml.models.SpecificMOJOParameters
import hex.genmodel.MojoModel
import org.apache.spark.expose.Logging
import org.apache.spark.ml.param.{Param, Params}

trait HasInputColOnMOJO extends Params with SpecificMOJOParameters with Logging {
  protected val inputCol: Param[String] = new Param[String](this, "inputCol", "Input column name")

  def getInputCol(): String = $(inputCol)

  def setInputCol(name: String): this.type = set(inputCol -> name)

  override private[sparkling] def setSpecificParams(h2oMojo: MojoModel): Unit = {
    super.setSpecificParams(h2oMojo)
    h2oMojo.features().headOption.foreach { feature =>
      set(inputCol -> feature)
    }
  }

}
