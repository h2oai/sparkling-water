getOption <- function(option) {
  if (invoke(option, "isDefined")) {
    invoke(option, "get")
  } else {
    NA_character_
  }
}

#' @export H2OConf
H2OConf <- setRefClass("H2OConf", fields = list(jconf = "ANY"), methods = list(
  initialize = function(spark) {
    .self$jconf <- invoke_new(spark, "org.apache.spark.h2o.H2OConf", spark_context(spark))
  },

  userName = function() { getOption(invoke(jconf, "userName")) },
  password = function() { getOption(invoke(jconf, "password")) },
  setUserName = function(value) { invoke(jconf, "setUserName", value) },
  setPassword = function(value) { invoke(jconf, "setPassword", value) }
))
