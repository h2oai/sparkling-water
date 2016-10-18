#' @importFrom utils capture.output browseURL
#' @importFrom sparklyr spark_dependency register_extension invoke_static invoke spark_connection spark_dataframe sdf_register spark_context
#' @importFrom h2o h2o.getFrame h2o.getId h2o.init

#' @import sparklyr h2o utils
NULL

# define required spark packages
spark_dependencies <- function(spark_version, scala_version, ...) {
  sw_version = getOption("rsparkling.sparklingwater.version", default = "1.6.7")

  if(as.package_version(spark_version)$major != as.package_version(sw_version)$major){
    stop(cat(paste0("Major version of Sparkling Water does not correspond to major Spark version.
    \nMajor Sparkling Water Version = ",as.package_version(sw_version)$major,
    "\nMajor Spark Version = ",as.package_version(spark_version)$major)))
  }
  if(as.package_version(spark_version)$minor != as.package_version(sw_version)$minor){
     stop(cat(paste0("Minor version of Sparkling Water does not correspond to minor Spark version.
     \nMinor Sparkling Water Version = ",as.package_version(sw_version)$minor,
     "\nMinor Spark Version = ",as.package_version(spark_version)$minor)))
  }
}

.onLoad <- function(libname, pkgname) {
  register_extension(pkgname)
}

