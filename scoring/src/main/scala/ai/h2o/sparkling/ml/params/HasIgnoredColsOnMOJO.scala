package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.ml.models.SpecificMOJOParameters
import hex.genmodel.MojoModel
import org.apache.spark.expose.Logging

trait HasIgnoredColsOnMOJO extends ParameterConstructorMethods with SpecificMOJOParameters with Logging {
  private val ignoredCols = nullableStringArrayParam("ignoredCols", "Names of columns to ignore for training.")

  def getIgnoredCols(): Array[String] = $(ignoredCols)

  override private[sparkling] def setSpecificParams(h2oMojo: MojoModel): Unit = {
    super.setSpecificParams(h2oMojo)
    try {
      val h2oParameters = h2oMojo._modelAttributes.getModelParameters()
      val h2oParametersMap = h2oParameters.map(i => i.name -> i.actual_value).toMap
      h2oParametersMap.get("ignored_columns").foreach(value => set("ignoredCols", value))
    } catch {
      case e: Throwable => logError("An error occurred during a try to access H2O MOJO parameters.", e)
    }
  }
}
