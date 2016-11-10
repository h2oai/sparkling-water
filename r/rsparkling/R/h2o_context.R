#' Get the H2OContext. Will create the context if it has not been previously created.
#'
#' @param x Object of type \code{spark_connection} or \code{spark_jobj}.
#' @param strict_version_check (Optional) Setting this to FALSE does not cross check version of H2O and attempts to connect.
#' @export
h2o_context <- function(x, strict_version_check = TRUE) {
  UseMethod("h2o_context")
}

#' @export
h2o_context.spark_connection <- function(x, strict_version_check = TRUE) {
  hc <- invoke_static(x, "org.apache.spark.h2o.H2OContext", "getOrCreate", spark_context(x))
  ip <- invoke(hc, "h2oLocalClientIp")
  port <- invoke(hc, "h2oLocalClientPort")
  invisible(capture.output(h2o.init(ip = ip, port = port, strict_version_check = strict_version_check)))
  hc
}

#' @export
h2o_context.spark_jobj <- function(x, strict_version_check = TRUE) {
  h2o_context.spark_connection(spark_connection(x), strict_version_check=strict_version_check)
}

#' Open the H2O Flow UI in a browser
#'
#' @inheritParams h2o_context
#'
#' @param sc Object of type \code{spark_connection}.
#' @export
h2o_flow <- function(sc) {
  flow <- invoke(h2o_context(sc), "h2oLocalClient")
  browseURL(paste0("http://", flow))
}

#' Convert a Spark DataFrame to an H2O Frame
#'
#' @param sc Object of type \code{spark_connection}.
#' @param x A \code{spark_dataframe}.
#' @param name The name of the H2OFrame.
#' @param strict_version_check (Optional) Setting this to FALSE does not cross check version of H2O and attempts to connect.
#' @export
as_h2o_frame <- function(sc, x, name=NULL, strict_version_check=TRUE) {
  # sc is not actually required since the sc is monkey-patched into the Spark DataFrame
  # it is kept as an argument for API consistency

  # Ensure we are dealing with a Spark DataFrame (might be e.g. a tbl)
  x <- spark_dataframe(x)
  
  # Convert the Spark DataFrame to an H2OFrame
  hc <- h2o_context(x, strict_version_check=strict_version_check)
  jhf <- if(is.null(name)) {
    invoke(hc, "asH2OFrame", x)
  } else {
    invoke(hc, "asH2OFrame", x, name)
  }

  key <- invoke(invoke(jhf, "key"), "toString")
  h2o.getFrame(key)
}

#' Convert an H2O Frame to a Spark DataFrame
#'
#' @param sc Object of type \code{spark_connection}.
#' @param x An \code{H2OFrame}.
#' @param name The name to assign the data frame in Spark.
#' @param strict_version_check (Optional) Setting this to FALSE does not cross check version of H2O and attempts to connect.
#' @export
as_spark_dataframe <- function(sc, x, name = paste(deparse(substitute(x)), collapse=""), strict_version_check=TRUE) {
  # TO DO: ensure we are dealing with a H2OFrame

  # Get SQLContext
  sqlContext <- invoke_static(sc, "org.apache.spark.sql.SQLContext", "getOrCreate", spark_context(sc))
  # Get H2OContext
  hc <- h2o_context(sc, strict_version_check=strict_version_check)
  # Invoke H2OContext#asDataFrame method on the backend
  spark_df <- invoke(hc, "asDataFrame", h2o.getId(x), TRUE, sqlContext)
  # Register returned spark_jobj as a table for dplyr
  sdf_register(spark_df, name = name)
}
