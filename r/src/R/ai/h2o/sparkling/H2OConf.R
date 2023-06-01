if (!exists("SharedBackendConf")) source(file.path("R", "SharedBackendConf.R"))
if (!exists("ExternalBackendConf")) source(file.path("R", "ExternalBackendConf.R"))
if (!exists("InternalBackendConf")) source(file.path("R", "InternalBackendConf.R"))

#' @export H2OConf
H2OConf <- setRefClass("H2OConf", fields = list(jconf = "ANY"),
                       contains = c("SharedBackendConf", "ExternalBackendConf", "InternalBackendConf"), methods = list(
    initialize = function() {
        sc <- spark_connection_find()[[1]]
        .self$jconf <- invoke_new(sc, "ai.h2o.sparkling.H2OConf")
    },

    set = function(option, value) { invoke(jconf, "set", option, value); .self },

    get = function(option) { invoke(jconf, "get", option) }
))
