package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.params.H2OAlgorithmMOJOParams
import ai.h2o.sparkling.utils.DataFrameSerializer

case class H2OMOJOSettings(
    predictionCol: String = "prediction",
    detailedPredictionCol: String = "detailed_prediction",
    convertUnknownCategoricalLevelsToNa: Boolean = false,
    convertInvalidNumbersToNa: Boolean = false,
    withContributions: Boolean = false,
    withInternalContributions: Boolean = false,
    withPredictionInterval: Boolean = false,
    withLeafNodeAssignments: Boolean = false,
    withStageResults: Boolean = false,
    dataFrameSerializer: String = DataFrameSerializer.default.getClass().getName(),
    scoringBulkSize: Int = 1000)

object H2OMOJOSettings {
  def default = H2OMOJOSettings()

  def createFromModelParams(params: H2OAlgorithmMOJOParams): H2OMOJOSettings = {
    H2OMOJOSettings(
      predictionCol = params.getPredictionCol(),
      detailedPredictionCol = params.getDetailedPredictionCol(),
      withContributions = params.getWithContributions(),
      convertUnknownCategoricalLevelsToNa = params.getConvertUnknownCategoricalLevelsToNa(),
      convertInvalidNumbersToNa = params.getConvertInvalidNumbersToNa(),
      withLeafNodeAssignments = params.getWithLeafNodeAssignments(),
      withStageResults = params.getWithStageResults(),
      dataFrameSerializer = params.getDataFrameSerializer())
  }
}
