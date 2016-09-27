
#' @import sparklyr h2o utils
NULL

# define required spark packages
spark_dependencies <- function(spark_version, scala_version, ...) {
  sparklyr::spark_dependency(packages = c(
    sprintf("ai.h2o:sparkling-water-core_%s:2.0.0", scala_version),
    sprintf("ai.h2o:sparkling-water-ml_%s:2.0.0", scala_version),
    sprintf("ai.h2o:sparkling-water-repl_%s:2.0.0", scala_version)
  ))
}

.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}

