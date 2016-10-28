#' Get the H2OContext. Will create the context if it has not been previously created.
#'
#' @param x Object of type \code{spark_connection} or \code{spark_jobj}.
#'
#' @export
h2o_context <- function(x) {
  UseMethod("h2o_context")
}

#' @export
h2o_context.spark_connection <- function(x) {
  hc <- sparklyr::invoke_static(x, "org.apache.spark.h2o.H2OContext", "getOrCreate", sparklyr::spark_context(x))
  ip <- sparklyr::invoke(hc, "h2oLocalClientIp")
  port <- sparklyr::invoke(hc, "h2oLocalClientPort")
  invisible(utils::capture.output(h2o::h2o.init(ip = ip, port = port, strict_version_check = FALSE)))  #should update strict_version_check to TRUE
  hc
}

#' @export
h2o_context.spark_jobj <- function(x) {
  h2o_context(sparklyr::spark_connection(x))
}


#' Open the H2O Flow UI in a browser
#'
#' @inheritParams h2o_context
#'
#' @param sc Object of type \code{spark_connection}.
#' @export
h2o_flow <- function(sc) {
  flow <- sparklyr::invoke(h2o_context(sc), "h2oLocalClient")
  utils::browseURL(paste0("http://", flow))
}

#' Convert a Spark DataFrame to an H2O Frame
#'
#' @param sc Object of type \code{spark_connection}.
#' @param x A \code{spark_dataframe}.
#'
#' @export
as_h2o_frame <- function(sc, x) {
  # sc is not actually required since the sc is monkey-patched into the Spark DataFrame
  # it is kept as an argument for API consistency

  # Ensure we are dealing with a Spark DataFrame (might be e.g. a tbl)
  x <- sparklyr::spark_dataframe(x)

  # Convert the Spark DataFrame to an H2OFrame
  jhf <- sparklyr::invoke(h2o_context(x), "asH2OFrame", x)
  key <- sparklyr::invoke(sparklyr::invoke(jhf, "key"), "toString")
  h2o::h2o.getFrame(key)
}

#' Convert an H2O Frame to a Spark DataFrame
#'
#' @param sc Object of type \code{spark_connection}.
#' @param x An \code{H2OFrame}.
#' @param name The name to assign the data frame in Spark.
#'
#' @export
as_spark_dataframe <- function(sc, x, name = deparse(substitute(x))) {
  # TO DO: ensure we are dealing with a H2OFrame

  # Get SQLContext
  sqlContext <- sparklyr::invoke_static(sc, "org.apache.spark.sql.SQLContext", "getOrCreate", sparklyr::spark_context(sc))
  # Get H2OContext
  hc <- h2o_context(sc)
  # Invoke H2OContext#asDataFrame method on the backend
  spark_df <- sparklyr::invoke(hc, "asDataFrame", h2o::h2o.getId(x), TRUE, sqlContext)
  # Register returned spark_jobj as a table for dplyr
  sparklyr::sdf_register(spark_df, name = name)
}
