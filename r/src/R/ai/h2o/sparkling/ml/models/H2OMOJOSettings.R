H2OMOJOSettings.default <- function() {
  H2OMOJOSettings()
}

#' @export H2OMOJOSettings
H2OMOJOSettings <- setRefClass("H2OMOJOSettings",
                               fields = list(predictionCol = "character",
                                             detailedPredictionCol = "character",
                                             convertUnknownCategoricalLevelsToNa = "logical",
                                             convertInvalidNumbersToNa = "logical",
                                             withContributions = "logical",
                                             withInternalContributions = "logical",
                                             withPredictionInterval = "logical",
                                             withLeafNodeAssignments = "logical",
                                             withStageResults = "logical",
                                             dataFrameSerializer = "character",
                                             scoringBulkSize = "integer"),
                               methods = list(
                                 initialize = function(predictionCol = "prediction",
                                                       detailedPredictionCol = "detailed_prediction",
                                                       convertUnknownCategoricalLevelsToNa = FALSE,
                                                       convertInvalidNumbersToNa = FALSE,
                                                       withContributions = FALSE,
                                                       withInternalContributions = FALSE,
                                                       withPredictionInterval = FALSE,
                                                       withLeafNodeAssignments = FALSE,
                                                       withStageResults = FALSE,
                                                       dataFrameSerializer = "ai.h2o.sparkling.utils.JSONDataFrameSerializer",
                                                       scoringBulkSize = 1000L) {
                                   .self$predictionCol <- predictionCol
                                   .self$detailedPredictionCol <- detailedPredictionCol
                                   .self$convertUnknownCategoricalLevelsToNa <- convertUnknownCategoricalLevelsToNa
                                   .self$convertInvalidNumbersToNa <- convertInvalidNumbersToNa
                                   .self$withContributions <- withContributions
                                   .self$withInternalContributions <- withInternalContributions
                                   .self$withPredictionInterval <- withPredictionInterval
                                   .self$withLeafNodeAssignments <- withLeafNodeAssignments
                                   .self$withStageResults <- withStageResults
                                   .self$dataFrameSerializer <- dataFrameSerializer
                                   .self$scoringBulkSize <- scoringBulkSize
                                 },
                                 toJavaObject = function() {
                                   sc <- spark_connection_find()[[1]]
                                   invoke_new(sc, "ai.h2o.sparkling.ml.models.H2OMOJOSettings",
                                              .self$predictionCol,
                                              .self$detailedPredictionCol,
                                              .self$convertUnknownCategoricalLevelsToNa,
                                              .self$convertInvalidNumbersToNa,
                                              .self$withContributions,
                                              .self$withInternalContributions,
                                              .self$withPredictionInterval,
                                              .self$withLeafNodeAssignments,
                                              .self$withStageResults,
                                              .self$dataFrameSerializer,
                                              .self$scoringBulkSize)
                                 }
                               ))
