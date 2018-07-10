#' @importFrom utils capture.output browseURL
#' @importFrom sparklyr spark_dependency register_extension invoke_static invoke spark_connection spark_dataframe sdf_register spark_context
#' @importFrom h2o h2o.getFrame h2o.getId h2o.init
#' @importFrom utils read.table
#' @importFrom utils packageVersion
NULL

#' Get Spark Major and Minor version from full Spark version
#'
#' @return Major and minor version of Spark as String
#' @export
get_spark_major_minor_version <- function(spark_version) {
  spark_major_version = as.package_version(spark_version)$major
  spark_minor_version = as.package_version(spark_version)$minor
  sprintf("%s.%s", spark_major_version, spark_minor_version)
}

#' Get Sparkling Water Major and Minor version from full Spark version
#'
#' @return Major and minor version of Sparkling Water as String
#' @export
get_sw_major_minor_version <- function(sw_version) {
  sw_major_version = as.package_version(sw_version)$major
  sw_minor_version = as.package_version(sw_version)$minor
  sprintf("%s.%s", sw_major_version, sw_minor_version)
}

#' Determines the last Sparkling Water version for given Spark version
#'
#' @return Sparkling Water version
#' @export
get_latest_sw_version_for_spark <- function(spark_version) {
  Spark_Version <- NULL
  Sparkling_Water_Version <- NULL
  
  spark_major_minor <- get_spark_major_minor_version(spark_version)

  sw_info <- head(subset(release_table, Spark_Version == spark_major_minor), 1)
  sw_version <- sw_info$Sparkling_Water_Version

  message(sprintf("Spark version %s detected. Will call latest Sparkling Water version %s", spark_version, sw_version))

  sw_version

}

#' Verify H2O version
#'
#' @export
verify_h2o_version <- function(spark_version, sw_version) {
    Spark_Version <- NULL
    Sparkling_Water_Version <- NULL
    spark_major_minor <- get_spark_major_minor_version(spark_version)
    sw_info <- head(subset(release_table, Spark_Version == spark_major_minor & Sparkling_Water_Version == sw_version), 1)
    h2o_version <- sw_info$H2O_Version
    h2o_build_version <- sw_info$H2O_Release_Patch_Number
    h2o_build_name <- sw_info$H2O_Release_Name
    sw_version <- sw_info$Sparkling_Water_Version
    current_h2o_version <- paste(packageVersion("h2o"))
    if (current_h2o_version != h2o_version) {
        message(paste0('\nDetected H2O version ', current_h2o_version ,'. Please install H2O version ', h2o_version ,', which is compliant with the Sparkling Water version ', sw_version ,' for Spark ', spark_major_minor ,'.\n
  To update your h2o R package, copy/paste the following commands and then restart your R session:

     detach("package:rsparkling", unload = TRUE)
     if ("package:h2o" %in% search()) { detach("package:h2o", unload = TRUE) }
     if (isNamespaceLoaded("h2o")){ unloadNamespace("h2o") }
     remove.packages("h2o")
     install.packages("h2o", type = "source", repos = "https://h2o-release.s3.amazonaws.com/h2o/', h2o_build_name ,'/', h2o_build_version ,'/R")\n'))
    }
}

#' This method checks if we are running on Spark supported by Sparkling Water and ends otherwise
#'
#' @export
check_spark_version_for_any_sw <- function(spark_version) {
  spark_major_minor <- get_spark_major_minor_version(spark_version)
  supported_spark_versions <- c("2.3", "2.2", "2.1", "2.0", "1.6")
  if (!(spark_major_minor %in% supported_spark_versions)){
    stop(sprintf("Supported Spark for Sparkling Water not detected. Please install Spark %s", paste(supported_spark_versions, collapse=", ")))
  }
}

#' This method checks if we are running on Spark required by specific Sparkling Water version
#'
#' @export
check_spark_version_for_sw_version <- function(spark_version, sw_version){
  spark_major_minor <- get_spark_major_minor_version(spark_version)
  sw_major_minor <- get_sw_major_minor_version(sw_version)

  if(sw_major_minor != spark_major_minor){
    stop(sprintf("You requested to use Sparkling Water %s, but it requires Spark %s.*. Current Spark is %s.", sw_version, sw_major_minor, spark_version))
  }
}

# Define required spark packages
spark_dependencies <- function(spark_version, scala_version, ...) {

  sw_version <- getOption("rsparkling.sparklingwater.version", default = NULL)
  sw_location <- getOption("rsparkling.sparklingwater.location", default = NULL)

  # Check if SW version and SW location is provided. That means that
  # the user is passing custom Sparkling Water assembly JAR

  if (is.null(sw_version) && is.null(sw_location)) {
    # The user did not provide SW version and SW location, that means that we can automatically
    # detect latest Sparkling Water version for the current Spark version.

    # First, check if we are running on any Spark supported by Sparkling Water
    check_spark_version_for_any_sw(spark_version)
    # Automatically determine the latest Sparkling Water version based on current Spark version
    sw_version = get_latest_sw_version_for_spark(spark_version)
    verify_h2o_version(spark_version, sw_version)
  }else if(!is.null(sw_version) && is.null(sw_location)){
    # The user specified SW version, but not SW location. It means we need to fetch Sparkling Water and need to
    # check if correct spark is available and stop if not.
    check_spark_version_for_sw_version(spark_version, sw_version)
    verify_h2o_version(spark_version, sw_version)
  }else if(!is.null(sw_version) && !is.null(sw_location)){
    # The user provided both SW version and SW artifact, just check if we are running on correct Spark version for the
    # desired Sparkling Water version
    if(!grepl('-SNAPSHOT', sw_version)){
      check_spark_version_for_sw_version(spark_version, sw_version)
      verify_h2o_version(spark_version, sw_version)
    }
  }else{
    # SW artifact is not empty, however Sparkling Water version is not specified.
    stop(sprintf("You specified location to Sparkling artifact, but did not specify the version. The version is
    required argument in case Sparkling Water artifact is specified."))
  }

  # If sparkling water jar artifact is specified, use it
  if (!is.null(sw_location)) {
    spark_dependency(
      jars = c(sw_location)
    )
  } else {
    spark_dependency(packages = c(
      sprintf("ai.h2o:sparkling-water-core_%s:%s", scala_version, sw_version),
      sprintf("ai.h2o:sparkling-water-ml_%s:%s", scala_version, sw_version),
      sprintf("ai.h2o:sparkling-water-repl_%s:%s", scala_version, sw_version),
      sprintf("no.priv.garshol.duke:duke:1.2")
    ))
  }
}


.onLoad <- function(libname, pkgname) {
  register_extension(pkgname)
}

