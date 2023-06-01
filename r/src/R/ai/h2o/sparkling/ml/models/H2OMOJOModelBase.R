#' @export H2OMOJOModelBase
H2OMOJOModelBase <- setRefClass("H2OMOJOModelBase", fields = list(jmojo = "ANY"), methods = list(
  initialize = function(jmojo) {
    .self$jmojo <- jmojo
  },
  getConvertUnknownCategoricalLevelsToNa = function() {
    invoke(.self$jmojo, "getConvertUnknownCategoricalLevelsToNa")
  },
  getConvertInvalidNumbersToNa = function() {
    invoke(.self$jmojo, "getConvertInvalidNumbersToNa")
  },
  transform = function(sparkFrame) {
    sparkFrame <- spark_dataframe(sparkFrame)
    outputFrame <- invoke(.self$jmojo, "transform", sparkFrame)
    sdf_register(outputFrame)
  }
))

#' @export H2OAlgorithmMOJOModelBase
H2OAlgorithmMOJOModelBase <- setRefClass("H2OAlgorithmMOJOModelBase", contains = ("H2OMOJOModelBase"), methods = list(
  getPredictionCol = function() {
    invoke(.self$jmojo, "getPredictionCol")
  },
  getDetailedPredictionCol = function() {
    invoke(.self$jmojo, "getDetailedPredictionCol")
  },
  getWithContributions = function() {
    invoke(.self$jmojo, "getWithContributions")
  },
  getFeaturesCols = function() {
    invoke(.self$jmojo, "getFeaturesCols")
  },
  getWithLeafNodeAssignments = function() {
    invoke(.self$jmojo, "getWithLeafNodeAssignments")
  },
  getWithStageResults = function() {
    invoke(.self$jmojo, "getWithStageResults")
  }
))
