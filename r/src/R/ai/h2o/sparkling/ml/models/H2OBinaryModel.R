H2OBinaryModel.read <- function(path) {
  sc <- spark_connection_find()[[1]]
  jmodel <- invoke_static(sc, "ai.h2o.sparkling.ml.models.H2OBinaryModel", "read", path)
  H2OBinaryModel(jmodel)
}

#' @export H2OBinaryModel
H2OBinaryModel <- setRefClass("H2OBinaryModel", fields = list(
  jmodel = "ANY", modelId = "character"), methods = list(
  initialize = function(jmodel) {
    .self$jmodel <- jmodel
    .self$modelId <- invoke(.self$jmodel, "modelId")
  },
  write = function(path) {
    invoke(.self$jmodel, "write", path)
  }
))
