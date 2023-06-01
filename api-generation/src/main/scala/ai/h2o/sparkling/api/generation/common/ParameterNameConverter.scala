package ai.h2o.sparkling.api.generation.common

object ParameterNameConverter {
  val h2oToSWExceptions: Map[String, String] = Map(
    "response_column" -> "labelCol",
    "validation_response_column" -> "validationLabelCol",
    "weights_column" -> "weightCol",
    "lambda" -> "lambdaValue",
    "alpha" -> "alphaValue",
    "colsample_bylevel" -> "colSampleByLevel",
    "colsample_bytree" -> "colSampleByTree",
    "colsample_bynode" -> "colSampleByNode",
    "rand_family" -> "randomFamily",
    "rand_link" -> "randomLink",
    "calibration_frame" -> "calibrationDataFrame",
    "variable_importances" -> "calculateFeatureImportances")

  val conversionRules: Map[String, String] = Map("Column" -> "Col")

  def convertFromH2OToSW(parameterName: String): String = {
    val parts = parameterName.split("_")
    val capitalizedParts = parts.head +: parts.tail.map(_.capitalize)
    val regularValue = conversionRules.foldLeft(capitalizedParts.mkString) {
      case (input, (from, to)) => input.replace(from, to)
    }
    h2oToSWExceptions.getOrElse(parameterName, regularValue)
  }
}
