
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
  invoke_static(sc, "org.apache.spark.h2o.H2OContext", "getOrCreate", spark_context(x))
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
#' df A \code{spark_dataframe}
#'
#' @keywords internal
#'
#' @export
h2o_frame <- function(df) {
  # ensure we are dealing with a spark data frame (might be e.g. a tbl)
  df <- spark_dataframe(df)

  # convert it to an H2OFrame
  invoke(h2o_context(df), "asH2OFrame", df)
}

