#' @importFrom utils capture.output browseURL
#' @importFrom sparklyr spark_dependency register_extension invoke_static invoke spark_connection spark_dataframe sdf_register spark_context
#' @importFrom h2o h2o.getFrame h2o.getId h2o.init
#' @importFrom utils read.table
#' @importFrom utils packageVersion
NULL



# Verify H2O version
verify_h2o_version <- function(h2o_version, h2o_build_name, h2o_build_version, sw_version) {
    current_h2o_version <- paste(packageVersion("h2o"))
    if (current_h2o_version != h2o_version) {
        message(paste0('\nDetected H2O ', current_h2o_version ,'. Please install H2O ', h2o_version ,', which is required for Sparkling Water ', sw_version ,'.\n

  To update your h2o R package, copy/paste the following commands and then restart your R session:

     detach("package:rsparkling", unload = TRUE)
     if ("package:h2o" %in% search()) { detach("package:h2o", unload = TRUE) }
     if (isNamespaceLoaded("h2o")){ unloadNamespace("h2o") }
     remove.packages("h2o")
     install.packages("h2o", type = "source", repos = "https://h2o-release.s3.amazonaws.com/h2o/rel-', h2o_build_name ,'/', h2o_build_version ,'/R")\n'))
    }
}


# Define required spark packages
spark_dependencies <- function(spark_version, scala_version, ...) {

  buildinfoFile <- system.file("buildinfo.txt", package = "rsparkling")
  lines <- readLines(buildinfoFile, warn = FALSE)
  h2o_version <- lines[1]
  h2o_name <- lines[2]
  h2o_build <- lines[3]
  sw_version <- lines[4]
  verify_h2o_version(h2o_version, h2o_name, h2o_build, sw_version)

  assembly_jar <- system.file("java/sparkling_water_assembly.jar", package = "rsparkling")

  spark_dependency(jars = c(assembly_jar))
}

.onLoad <- function(libname, pkgname) {
  register_extension(pkgname)
}

