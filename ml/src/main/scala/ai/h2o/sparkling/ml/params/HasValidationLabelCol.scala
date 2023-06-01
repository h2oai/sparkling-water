package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.ml.algos.H2OAlgoCommonUtils

trait HasValidationLabelCol extends H2OAlgoParamsBase with H2OAlgoCommonUtils {
  protected val validationLabelCol = stringParam(
    name = "validationLabelCol",
    doc = "(experimental) Name of the label column in the validation data frame. " +
      "The label column should be a string column with two distinct values indicating the anomaly. " +
      "The negative value must be alphabetically smaller than the positive value. (E.g. '0'/'1', 'False'/'True'")

  setDefault(validationLabelCol -> "label")

  def getValidationLabelCol(): String = $(validationLabelCol)

  def setValidationLabelCol(value: String): this.type = set(validationLabelCol, value)

  private[sparkling] def getValidationLabelColParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("validation_response_column" -> getValidationLabelCol())
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() + ("validationLabelCol" -> "validation_response_column")
  }

  override private[sparkling] def getAdditionalValidationCols(): Seq[String] = {
    super.getAdditionalValidationCols() :+ getValidationLabelCol()
  }
}
