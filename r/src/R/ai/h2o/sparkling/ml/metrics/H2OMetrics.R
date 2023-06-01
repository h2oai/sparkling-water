#' @export rsparkling.H2OMetrics
rsparkling.H2OMetrics <- setRefClass("rsparkling.H2OMetrics", fields = list(javaObject = "ANY"), methods = list(
initialize = function(javaObject) {
    .self$javaObject <- javaObject
}
))
