source(file.path("R", "H2OMOJOModelBase.R"))

H2OMOJOPipelineModel.createFromMojo <- function(pathToMojo, settings = H2OMOJOSettings.default()) {
  sc <- spark_connection_find()[[1]]
  jmojo <- invoke_static(sc, "ai.h2o.sparkling.ml.models.H2OMOJOPipelineModel", "createFromMojo", pathToMojo, settings$toJavaObject())
  H2OMOJOPipelineModel(jmojo)
}

#' @export H2OMOJOPipelineModel
H2OMOJOPipelineModel <- setRefClass("H2OMOJOPipelineModel", contains = ("H2OAlgorithmMOJOModelBase"), methods = list(
  getWithInternalContributions = function() {
    invoke(.self$jmojo, "getWithInternalContributions")
  },
  getWithPredictionInterval = function() {
    invoke(.self$jmojo, "getWithPredictionInterval")
  },
  getScoringBulkSize = function() {
    invoke(.self$jmojo, "getScoringBulkSize")
  }
))
