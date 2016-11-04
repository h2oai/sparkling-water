#' @importFrom utils capture.output browseURL
#' @importFrom sparklyr spark_dependency register_extension invoke_static invoke spark_connection spark_dataframe sdf_register spark_context
#' @importFrom h2o h2o.getFrame h2o.getId h2o.init

# define required spark packages
spark_dependencies <- function(spark_version, scala_version, ...) {
  spark_dependency(packages = c(
    sprintf("ai.h2o:sparkling-water-core_%s:2.0.0", scala_version),
    sprintf("ai.h2o:sparkling-water-ml_%s:2.0.0", scala_version),
    sprintf("ai.h2o:sparkling-water-repl_%s:2.0.0", scala_version)
  ))
}

.onLoad <- function(libname, pkgname) {
  register_extension(pkgname)
}

