
# https://github.com/h2oai/sparkling-water/blob/master/core/src/main/scala/org/apache/spark/h2o/H2OContext.scala

#' Get the H2OContext from another object
#'
#' Get the H2OContext. Will create the context if it hasn't been previously created.
#'
#' @param x Object of type \code{spark_connection} or \code{spark_jobj}
#'
#' @export
h2o_context <- function(x, ...) {
  UseMethod("h2o_context")
}

#' @export
h2o_context.spark_connection <- function(x, ...) {
  hc <- invoke_static(sc, "org.apache.spark.h2o.H2OContext", "getOrCreate", spark_context(x))
  ip <- invoke(hc, "h2oLocalClientIp")
  port <- invoke(hc, "h2oLocalClientPort")
  h2o.init(ip = ip, port = port, strict_version_check = FALSE)  #should update strict_version_check to TRUE
  hc
}

#' @export
h2o_context.spark_jobj <- function(x, ...) {
  h2o_context(spark_connection(x))
}


#' Open the H2O Flow UI in a browser
#'
#' @inheritParams h2o_context
#'
#' @export
h2o_flow <- function(sc) {
  flow <- invoke(h2o_context(sc), "h2oLocalClient")
  utils::browseURL(paste0("http://", flow))
}

#' Convert a Spark DataFrame to an H2O Frame
#'
#' x A \code{spark_dataframe}
#'
#' @keywords internal
#'
#' @export
as_h2o_frame <- function(x) {
  # Ensure we are dealing with a Spark DataFrame (might be e.g. a tbl)
  df <- spark_dataframe(x)

  # Convert the Spark DataFrame to an H2OFrame
  jhf <- invoke(h2o_context(x), "asH2OFrame", x)
  key <- invoke(invoke(jhf, "key"), "toString")
  h2o.getFrame(key)
}

#' Convert an H2O Frame to a Spark DataFrame
#'
#' x An \code{H2OFrame}.
#'
#' @keywords internal
#'
#' @export
as_spark_dataframe <- function(x) {
  # TO DO: ensure we are dealing with a H2OFrame
  
  # Get SQLContext
  sqlContext <- invoke_static(sc, "org.apache.spark.sql.SQLContext", "getOrCreate", spark_context(sc))
  # Get H2OContext
  hc <- h2o_context(sc)
  # Invoke H2OContext#asDataFrame method on the backend
  spark_df <- invoke(hc, "asDataFrame", h2o.getId(x), TRUE, sqlContext)
  # Register returned spark_jobj as a table for dplyr
  sdf_register(spark_df)
}
